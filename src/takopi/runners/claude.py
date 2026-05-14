from __future__ import annotations

import os
import re
import shutil
import datetime
import json
import subprocess
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any

import msgspec

from ..backends import EngineBackend, EngineConfig
from ..events import EventFactory
from ..logging import get_logger
from ..model import Action, ActionKind, EngineId, ResumeToken, TakopiEvent, StartedEvent
from ..runner import JsonlSubprocessRunner, ResumeTokenMixin, Runner
from .run_options import get_run_options
from ..schemas import claude as claude_schema
from .tool_actions import tool_input_path, tool_kind_and_title

logger = get_logger(__name__)

ENGINE: EngineId = "claude"
DEFAULT_ALLOWED_TOOLS = ["Bash", "Read", "Edit", "Write"]

_RESUME_RE = re.compile(
    r"(?im)^\s*`?claude\s+(?:--resume|-r)\s+(?P<token>[^`\s]+)`?\s*$"
)


ANSI_RE = re.compile(r"\x1b\[[0-?]*[ -/]*[@-~]")


def _tmux_run(args: list[str], *, check: bool = True) -> subprocess.CompletedProcess[str]:
    proc = subprocess.run(
        ["tmux", *args],
        text=True,
        capture_output=True,
    )
    if check and proc.returncode != 0:
        raise RuntimeError(f"tmux {' '.join(args)} failed: {proc.stderr.strip()}")
    return proc


def _tmux_has_session(session: str) -> bool:
    return _tmux_run(["has-session", "-t", session], check=False).returncode == 0


def _tmux_capture(session: str) -> str:
    return _tmux_run(["capture-pane", "-t", session, "-p", "-S", "-"]).stdout


def _tmux_send_text(session: str, text: str) -> None:
    subprocess.run(["tmux", "set-buffer", "-b", "takopi_claude_in", text], check=True)
    subprocess.run(
        ["tmux", "paste-buffer", "-b", "takopi_claude_in", "-t", session],
        check=True,
    )
    subprocess.run(["tmux", "send-keys", "-t", session, "Enter"], check=True)


def _normalize_interactive_text(text: str) -> str:
    text = ANSI_RE.sub("", text).replace("\u00a0", " ")
    lines: list[str] = []
    chrome_markers = [
        "Claude Code v", "Welcome back", "Tips for getting", "Quick safety check",
        "Accessing workspace", "Security guide", "Enter to confirm", "Esc to cancel",
        "Yes, I trust this folder", "No, exit", "Welcome to Opus", "context)",
        "? for shortcuts", "Auto-updating", "/effort to tune", "/root",
    ]
    for raw in text.splitlines():
        line = raw.rstrip()
        if not line.strip():
            continue
        if line.startswith(("─", "╭", "╰", "│")):
            continue
        if any(marker in line for marker in chrome_markers):
            continue
        if re.fullmatch(r"[▐▛▜▌▝▘█ ]+", line.strip()):
            continue
        lines.append(line)
    return "\n".join(lines)


def _extract_interactive_answer(before: str, after: str, prompt: str) -> str:
    if after.startswith(before):
        suffix = after[len(before):]
    else:
        # capture-pane scrollback may trim old content, so anchor on the latest user prompt
        idx = after.rfind(prompt)
        suffix = after[idx + len(prompt):] if idx >= 0 else after
    clean = _normalize_interactive_text(suffix)
    out: list[str] = []
    prompt_text = prompt.strip()
    for line in clean.splitlines():
        s = line.strip()
        if not s:
            continue
        if s.startswith("❯"):
            continue
        if prompt_text and prompt_text in s:
            continue
        s = re.sub(r"^[●⏺✻]\s*", "", s)
        if re.search(
            r"\b(?:Churned|Worked|Brewed|Baked|Sautéed|Sauteed|Stewed|Cooked|"
            r"Simmered|Toasted|Roasted|Stirred) for \d+s",
            s,
        ):
            continue
        out.append(s)
    return "\n".join(out).strip()


def _slash_segment_after_latest_prompt(prompt: str, pane: str) -> str:
    prompt_text = prompt.strip()
    lines = pane.splitlines()
    start_index = -1
    if prompt_text:
        normalized_prompt = prompt_text.lower().replace(" ", "")
        for idx, raw in enumerate(lines):
            stripped = raw.strip().lower().replace(" ", "")
            if stripped.startswith("❯") and normalized_prompt in stripped:
                start_index = idx + 1
    if start_index < 0:
        return ""
    return "\n".join(lines[start_index:])


def _is_interactive_slash_overlay(prompt: str, pane: str) -> bool:
    cmd = prompt.strip().lower().replace(" ", "")
    if cmd not in {"/usage", "/status", "/config", "/stats"}:
        return False
    segment = _slash_segment_after_latest_prompt(prompt, pane)
    if not segment:
        return False
    # Ignore stale scrollback and the dismissal line from the previous overlay.
    if "Settings dialog dismissed" in segment and "Settings  Status   Config   Usage   Stats" not in segment:
        return False
    indicators = [
        "Settings  Status   Config   Usage   Stats",
        "Total cost:",
        "Current session",
        "Current week",
        "Extra usage",
    ]
    return any(item in segment for item in indicators)


def _format_interactive_slash_overlay(prompt: str, pane: str) -> str:
    # Work only with the latest slash-command response, not old scrollback.
    segment = _slash_segment_after_latest_prompt(prompt, pane) or pane
    clean = _normalize_interactive_text(segment)
    all_lines = clean.splitlines()

    lines = []
    capture = False
    for raw in all_lines:
        line = raw.rstrip()
        stripped = line.strip()
        if not stripped:
            if capture and lines and lines[-1] != "":
                lines.append("")
            continue
        if "Settings  Status   Config   Usage   Stats" in line:
            capture = True
        if not capture:
            continue
        if stripped.startswith("❯"):
            break
        if "Esc to cancel" in line or "? for shortcuts" in line:
            continue
        if "dialog dismissed" in line:
            continue
        lines.append(line)
    while lines and lines[-1] == "":
        lines.pop()
    return "\n".join(lines).strip()


def _render_overlay_png(text: str, *, cwd: str, name_hint: str = "claude_usage") -> str | None:
    """Render monospace overlay text to a PNG using system python3/Pillow.

    Takopi's venv may not have Pillow, while the host python usually does.
    Returns a path relative to cwd so Telegram artifact collection can upload it.
    """
    root = Path(cwd).expanduser()
    out_dir = root / "artifacts"
    out_dir.mkdir(parents=True, exist_ok=True)
    stamp = datetime.datetime.utcnow().strftime("%Y%m%d-%H%M%S")
    out = out_dir / f"{name_hint}-{stamp}.png"
    script = r"""
import json, sys
from pathlib import Path
from PIL import Image, ImageDraw, ImageFont
payload=json.load(sys.stdin)
text=payload["text"]
out=Path(payload["out"])
lines=text.splitlines() or [""]
font_path="/usr/share/fonts/truetype/dejavu/DejaVuSansMono.ttf"
try:
    font=ImageFont.truetype(font_path, 24)
except Exception:
    font=ImageFont.load_default()
dummy=Image.new("RGB", (1,1))
d=ImageDraw.Draw(dummy)
line_h=max(32, int(d.textbbox((0,0), "Mg", font=font)[3]*1.35))
width=max(900, max(int(d.textlength(line, font=font)) for line in lines)+64)
height=max(180, line_h*len(lines)+64)
img=Image.new("RGB", (width, height), (12, 14, 18))
d=ImageDraw.Draw(img)
y=32
for line in lines:
    d.text((32,y), line, font=font, fill=(232, 238, 245))
    y += line_h
img.save(out)
"""
    try:
        proc = subprocess.run(
            ["python3", "-c", script],
            input=json.dumps({"text": text, "out": str(out)}),
            text=True,
            capture_output=True,
            timeout=15,
        )
        if proc.returncode != 0:
            logger.warning("interactive.overlay_png_failed", error=proc.stderr.strip())
            return None
        return str(out.relative_to(root))
    except (OSError, subprocess.SubprocessError, ValueError) as exc:
        logger.warning(
            "interactive.overlay_png_failed",
            error=str(exc),
            error_type=type(exc).__name__,
        )
        return None


def _wait_interactive_answer(session: str, before: str, prompt: str, timeout_s: int, *, cwd: str = "/root") -> str:
    last = ""
    stable = 0
    deadline = time.time() + timeout_s
    while time.time() < deadline:
        cur = _tmux_capture(session)
        if _is_interactive_slash_overlay(prompt, cur):
            # Snapshot the modal output, render it as an image, and dismiss it so
            # the tmux session returns to prompt. Telegram artifact collection will
            # upload the generated PNG after the run.
            answer = _format_interactive_slash_overlay(prompt, cur)
            rel_png = _render_overlay_png(answer, cwd=cwd, name_hint="claude_usage") if answer else None
            subprocess.run(["tmux", "send-keys", "-t", session, "Escape"], check=False)
            if rel_png:
                return f"Скриншот: {rel_png}"
            return answer or _extract_interactive_answer(before, cur, prompt)
        answer = _extract_interactive_answer(before, cur, prompt)
        tail = "\n".join(cur.splitlines()[-16:])
        prompt_ready = "❯" in tail
        if answer and answer == last:
            stable += 1
            if (prompt_ready and stable >= 2) or stable >= 4:
                return answer
        else:
            stable = 0
            last = answer
        time.sleep(1)
    return last or _extract_interactive_answer(before, _tmux_capture(session), prompt)


def _ensure_interactive_claude_session(*, session: str, cwd: str, claude_cmd: str) -> None:
    if _tmux_has_session(session):
        return
    subprocess.run(
        ["tmux", "new-session", "-d", "-x", "180", "-y", "50", "-s", session, "-c", cwd, claude_cmd],
        check=True,
    )
    # Detached tmux defaults to 80x24 unless told otherwise; Claude Code's /usage
    # hides the wide limit columns in narrow terminals.
    subprocess.run(["tmux", "resize-window", "-t", session, "-x", "180", "-y", "50"], check=False)
    time.sleep(3)
    out = _tmux_capture(session)
    if "Yes, I trust this folder" in out:
        subprocess.run(["tmux", "send-keys", "-t", session, "Enter"], check=True)
        time.sleep(4)


@dataclass(slots=True)
class ClaudeStreamState:
    factory: EventFactory = field(default_factory=lambda: EventFactory(ENGINE))
    pending_actions: dict[str, Action] = field(default_factory=dict)
    last_assistant_text: str | None = None
    note_seq: int = 0


def _normalize_tool_result(content: Any) -> str:
    if content is None:
        return ""
    if isinstance(content, str):
        return content
    if isinstance(content, list):
        parts: list[str] = []
        for item in content:
            if isinstance(item, dict):
                text = item.get("text")
                if isinstance(text, str) and text:
                    parts.append(text)
            elif isinstance(item, str):
                parts.append(item)
        return "\n".join(part for part in parts if part)
    if isinstance(content, dict):
        text = content.get("text")
        if isinstance(text, str):
            return text
    return str(content)


def _coerce_comma_list(value: Any) -> str | None:
    if value is None:
        return None
    if isinstance(value, (list, tuple, set)):
        parts = [str(item) for item in value if item is not None]
        joined = ",".join(part for part in parts if part)
        return joined or None
    text = str(value)
    return text or None


def _tool_kind_and_title(
    name: str, tool_input: dict[str, Any]
) -> tuple[ActionKind, str]:
    return tool_kind_and_title(name, tool_input, path_keys=("file_path", "path"))


def _tool_action(
    content: claude_schema.StreamToolUseBlock,
    *,
    parent_tool_use_id: str | None,
) -> Action:
    tool_id = content.id
    tool_name = str(content.name or "tool")
    tool_input = content.input

    kind, title = _tool_kind_and_title(tool_name, tool_input)

    detail: dict[str, Any] = {
        "name": tool_name,
        "input": tool_input,
    }
    if parent_tool_use_id:
        detail["parent_tool_use_id"] = parent_tool_use_id

    if kind == "file_change":
        path = tool_input_path(tool_input, path_keys=("file_path", "path"))
        if path:
            detail["changes"] = [{"path": path, "kind": "update"}]

    return Action(id=tool_id, kind=kind, title=title, detail=detail)


def _tool_result_event(
    content: claude_schema.StreamToolResultBlock,
    *,
    action: Action,
    factory: EventFactory,
) -> TakopiEvent:
    is_error = content.is_error is True
    raw_result = content.content
    normalized = _normalize_tool_result(raw_result)
    preview = normalized

    detail = action.detail | {
        "tool_use_id": content.tool_use_id,
        "result_preview": preview,
        "result_len": len(normalized),
        "is_error": is_error,
    }
    return factory.action_completed(
        action_id=action.id,
        kind=action.kind,
        title=action.title,
        ok=not is_error,
        detail=detail,
    )


def _extract_error(event: claude_schema.StreamResultMessage) -> str | None:
    if event.is_error:
        if isinstance(event.result, str) and event.result:
            return event.result
        subtype = event.subtype
        if subtype:
            return f"claude run failed ({subtype})"
        return "claude run failed"
    return None


def _usage_payload(event: claude_schema.StreamResultMessage) -> dict[str, Any]:
    usage: dict[str, Any] = {}
    for key in (
        "total_cost_usd",
        "duration_ms",
        "duration_api_ms",
        "num_turns",
    ):
        value = getattr(event, key, None)
        if value is not None:
            usage[key] = value
    if event.usage is not None:
        usage["usage"] = event.usage
    return usage


def translate_claude_event(
    event: claude_schema.StreamJsonMessage,
    *,
    title: str,
    state: ClaudeStreamState,
    factory: EventFactory,
) -> list[TakopiEvent]:
    match event:
        case claude_schema.StreamSystemMessage(subtype=subtype):
            if subtype != "init":
                return []
            session_id = event.session_id
            if not session_id:
                return []
            meta: dict[str, Any] = {}
            for key in (
                "cwd",
                "tools",
                "permissionMode",
                "output_style",
                "apiKeySource",
                "mcp_servers",
            ):
                value = getattr(event, key, None)
                if value is not None:
                    meta[key] = value
            model = event.model
            token = ResumeToken(engine=ENGINE, value=session_id)
            event_title = str(model) if isinstance(model, str) and model else title
            return [factory.started(token, title=event_title, meta=meta or None)]
        case claude_schema.StreamAssistantMessage(
            message=message, parent_tool_use_id=parent_tool_use_id
        ):
            out: list[TakopiEvent] = []
            for content in message.content:
                match content:
                    case claude_schema.StreamToolUseBlock():
                        action = _tool_action(
                            content,
                            parent_tool_use_id=parent_tool_use_id,
                        )
                        state.pending_actions[action.id] = action
                        out.append(
                            factory.action_started(
                                action_id=action.id,
                                kind=action.kind,
                                title=action.title,
                                detail=action.detail,
                            )
                        )
                    case claude_schema.StreamThinkingBlock(
                        thinking=thinking, signature=signature
                    ):
                        if not thinking:
                            continue
                        state.note_seq += 1
                        action_id = f"claude.thinking.{state.note_seq}"
                        detail: dict[str, Any] = {}
                        if parent_tool_use_id:
                            detail["parent_tool_use_id"] = parent_tool_use_id
                        if signature:
                            detail["signature"] = signature
                        out.append(
                            factory.action_completed(
                                action_id=action_id,
                                kind="note",
                                title=thinking,
                                ok=True,
                                detail=detail,
                            )
                        )
                    case claude_schema.StreamTextBlock(text=text):
                        if text:
                            state.last_assistant_text = text
                    case _:
                        continue
            return out
        case claude_schema.StreamUserMessage(message=message):
            if not isinstance(message.content, list):
                return []
            out: list[TakopiEvent] = []
            for content in message.content:
                if not isinstance(content, claude_schema.StreamToolResultBlock):
                    continue
                tool_use_id = content.tool_use_id
                action = state.pending_actions.pop(tool_use_id, None)
                if action is None:
                    action = Action(
                        id=tool_use_id,
                        kind="tool",
                        title="tool result",
                        detail={},
                    )
                out.append(
                    _tool_result_event(
                        content,
                        action=action,
                        factory=factory,
                    )
                )
            return out
        case claude_schema.StreamResultMessage():
            ok = not event.is_error
            result_text = event.result or ""
            if ok and not result_text and state.last_assistant_text:
                result_text = state.last_assistant_text

            resume = ResumeToken(engine=ENGINE, value=event.session_id)
            error = None if ok else _extract_error(event)
            usage = _usage_payload(event)

            return [
                factory.completed(
                    ok=ok,
                    answer=result_text,
                    resume=resume,
                    error=error,
                    usage=usage or None,
                )
            ]
        case _:
            return []


@dataclass(slots=True)
class ClaudeRunner(ResumeTokenMixin, JsonlSubprocessRunner):
    engine: EngineId = ENGINE
    resume_re: re.Pattern[str] = _RESUME_RE

    claude_cmd: str = "claude"
    model: str | None = None
    allowed_tools: list[str] | None = None
    dangerously_skip_permissions: bool = False
    use_api_billing: bool = False
    interactive: bool = False
    interactive_session: str = "takopi_claude"
    interactive_cwd: str = "/root"
    interactive_timeout: int = 300
    session_title: str = "claude"
    logger = logger

    def format_resume(self, token: ResumeToken) -> str:
        if token.engine != ENGINE:
            raise RuntimeError(f"resume token is for engine {token.engine!r}")
        return f"`claude --resume {token.value}`"

    def is_resume_line(self, line: str) -> bool:
        if self.interactive:
            return False
        return super().is_resume_line(line)

    def extract_resume(self, text: str | None) -> ResumeToken | None:
        if self.interactive:
            return None
        return super().extract_resume(text)

    async def run_impl(self, prompt: str, resume: ResumeToken | None):
        if not self.interactive:
            async for evt in super().run_impl(prompt, resume):
                yield evt
            return
        import anyio
        token = resume or ResumeToken(engine=ENGINE, value=self.interactive_session)
        yield StartedEvent(engine=ENGINE, resume=token, title=self.session_title, meta={"mode": "interactive"})
        await anyio.to_thread.run_sync(
            lambda: _ensure_interactive_claude_session(
                session=self.interactive_session,
                cwd=self.interactive_cwd,
                claude_cmd=self.claude_cmd,
            )
        )
        before = await anyio.to_thread.run_sync(_tmux_capture, self.interactive_session)
        await anyio.to_thread.run_sync(_tmux_send_text, self.interactive_session, prompt)
        answer = await anyio.to_thread.run_sync(
            lambda: _wait_interactive_answer(
                self.interactive_session,
                before,
                prompt,
                self.interactive_timeout,
                cwd=self.interactive_cwd,
            )
        )
        yield EventFactory(ENGINE).completed_ok(answer=answer, resume=token)

    def _build_args(self, prompt: str, resume: ResumeToken | None) -> list[str]:
        run_options = get_run_options()
        args: list[str] = ["-p", "--output-format", "stream-json", "--verbose"]
        if resume is not None:
            args.extend(["--resume", resume.value])
        model = self.model
        if run_options is not None and run_options.model:
            model = run_options.model
        if model is not None:
            args.extend(["--model", str(model)])
        allowed_tools = _coerce_comma_list(self.allowed_tools)
        if allowed_tools is not None:
            args.extend(["--allowedTools", allowed_tools])
        if self.dangerously_skip_permissions is True:
            args.append("--dangerously-skip-permissions")
        args.append("--")
        args.append(prompt)
        return args

    def command(self) -> str:
        return self.claude_cmd

    def build_args(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: Any,
    ) -> list[str]:
        return self._build_args(prompt, resume)

    def stdin_payload(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: Any,
    ) -> bytes | None:
        return None

    def env(self, *, state: Any) -> dict[str, str] | None:
        if self.use_api_billing is not True:
            env = dict(os.environ)
            env.pop("ANTHROPIC_API_KEY", None)
            return env
        return None

    def new_state(self, prompt: str, resume: ResumeToken | None) -> ClaudeStreamState:
        return ClaudeStreamState()

    def start_run(
        self,
        prompt: str,
        resume: ResumeToken | None,
        *,
        state: ClaudeStreamState,
    ) -> None:
        pass

    def decode_jsonl(
        self,
        *,
        line: bytes,
    ) -> claude_schema.StreamJsonMessage:
        return claude_schema.decode_stream_json_line(line)

    def decode_error_events(
        self,
        *,
        raw: str,
        line: str,
        error: Exception,
        state: ClaudeStreamState,
    ) -> list[TakopiEvent]:
        if isinstance(error, msgspec.DecodeError):
            self.get_logger().warning(
                "jsonl.msgspec.invalid",
                tag=self.tag(),
                error=str(error),
                error_type=error.__class__.__name__,
            )
            return []
        return super().decode_error_events(
            raw=raw,
            line=line,
            error=error,
            state=state,
        )

    def invalid_json_events(
        self,
        *,
        raw: str,
        line: str,
        state: ClaudeStreamState,
    ) -> list[TakopiEvent]:
        return []

    def translate(
        self,
        data: claude_schema.StreamJsonMessage,
        *,
        state: ClaudeStreamState,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
    ) -> list[TakopiEvent]:
        return translate_claude_event(
            data,
            title=self.session_title,
            state=state,
            factory=state.factory,
        )

    def process_error_events(
        self,
        rc: int,
        *,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
        state: ClaudeStreamState,
    ) -> list[TakopiEvent]:
        message = f"claude failed (rc={rc})."
        resume_for_completed = found_session or resume
        return [
            self.note_event(message, state=state, ok=False),
            state.factory.completed_error(
                error=message,
                resume=resume_for_completed,
            ),
        ]

    def stream_end_events(
        self,
        *,
        resume: ResumeToken | None,
        found_session: ResumeToken | None,
        state: ClaudeStreamState,
    ) -> list[TakopiEvent]:
        if not found_session:
            message = "claude finished but no session_id was captured"
            resume_for_completed = resume
            return [
                state.factory.completed_error(
                    error=message,
                    resume=resume_for_completed,
                )
            ]

        message = "claude finished without a result event"
        return [
            state.factory.completed_error(
                error=message,
                answer=state.last_assistant_text or "",
                resume=found_session,
            )
        ]


def build_runner(config: EngineConfig, _config_path: Path) -> Runner:
    claude_cmd = shutil.which("claude") or "claude"

    model = config.get("model")
    if "allowed_tools" in config:
        allowed_tools = config.get("allowed_tools")
    else:
        allowed_tools = DEFAULT_ALLOWED_TOOLS
    dangerously_skip_permissions = config.get("dangerously_skip_permissions") is True
    use_api_billing = config.get("use_api_billing") is True
    mode = str(config.get("mode") or "print").strip().lower()
    interactive = mode in {"interactive", "tmux", "pty"}
    interactive_session = str(config.get("interactive_session") or "takopi_claude")
    interactive_cwd = str(config.get("interactive_cwd") or "/root")
    try:
        interactive_timeout = int(config.get("interactive_timeout") or 300)
    except (TypeError, ValueError):
        interactive_timeout = 300
    title = str(model) if model is not None else "claude"

    return ClaudeRunner(
        claude_cmd=claude_cmd,
        model=model,
        allowed_tools=allowed_tools,
        dangerously_skip_permissions=dangerously_skip_permissions,
        use_api_billing=use_api_billing,
        interactive=interactive,
        interactive_session=interactive_session,
        interactive_cwd=interactive_cwd,
        interactive_timeout=interactive_timeout,
        session_title=title,
    )


BACKEND = EngineBackend(
    id="claude",
    build_runner=build_runner,
    install_cmd="npm install -g @anthropic-ai/claude-code",
)
