from __future__ import annotations

import asyncio
import json
import re
import subprocess
import textwrap
import time
from contextlib import suppress
from collections.abc import Awaitable, Callable, Sequence
from dataclasses import asdict, dataclass, field
from typing import Any

import httpx

from ..context import RunContext
from ..logging import get_logger
from ..markdown import MarkdownParts
from ..runners.claude import (
    _format_interactive_slash_overlay,
    _is_interactive_slash_overlay,
    _normalize_interactive_text,
    _slash_segment_after_latest_prompt,
)
from ..transport import MessageRef, RenderedMessage, SendOptions
from .bridge import TelegramBridgeConfig, send_plain
from .render import MAX_BODY_CHARS, prepare_telegram, prepare_telegram_multi

logger = get_logger(__name__)

_MAX_LIVE_PROGRESS_CHARS = min(1600, MAX_BODY_CHARS)
_CHANNEL_SLASH_COMMANDS = frozenset({"/model", "/stats", "/status", "/usage"})
_CHANNEL_SLASH_TIMEOUT_S = 12.0
_TELEGRAM_BULLET = "·"


@dataclass(slots=True)
class LiveProgressRun:
    progress_ref: MessageRef
    started_at: float
    chat_id: int = 0
    user_msg_id: int = 0
    thread_id: int | None = None
    verbose: bool = False
    done: asyncio.Event = field(default_factory=asyncio.Event)
    last_text: str = ""
    verbose_seen: set[str] = field(default_factory=set)
    verbose_sent: int = 0


_LIVE_PROGRESS_RUNS: dict[tuple[int, int], LiveProgressRun] = {}
_LIVE_PROGRESS_BY_PROGRESS: dict[tuple[int, int], tuple[int, int]] = {}
_LIVE_PROGRESS_LOCK = asyncio.Lock()
_MAX_VERBOSE_ACTION_MESSAGES = 8


def _render_channel_reply(text: str) -> RenderedMessage:
    payloads = prepare_telegram_multi(
        MarkdownParts(header="", body=text, footer=""),
        max_body_chars=MAX_BODY_CHARS,
    )
    first_text, first_entities = payloads[0]
    extra: dict[str, Any] = {"entities": first_entities}
    if len(payloads) > 1:
        extra["followups"] = [
            RenderedMessage(text=followup_text, extra={"entities": followup_entities})
            for followup_text, followup_entities in payloads[1:]
        ]
    return RenderedMessage(text=first_text, extra=extra)


def _render_live_progress(
    *,
    text: str,
    elapsed_s: float,
    status: str,
    engine: str = "claude",
) -> RenderedMessage:
    body = text.strip() or "↻ Working…"
    if len(body) > _MAX_LIVE_PROGRESS_CHARS:
        body = body[: _MAX_LIVE_PROGRESS_CHARS - 1] + "…"
    rendered_text, entities = prepare_telegram(
        MarkdownParts(
            header=f"{status} · {engine} · {max(0, int(elapsed_s))}s",
            body=body,
            footer=None,
        )
    )
    return RenderedMessage(text=rendered_text, extra={"entities": entities})


def _tmux_capture(session: str) -> str:
    proc = subprocess.run(
        ["tmux", "capture-pane", "-pt", session, "-S", "-200"],
        text=True,
        capture_output=True,
        check=False,
    )
    if proc.returncode != 0:
        return ""
    return proc.stdout


def _tmux_send_choice(session: str, choice: str) -> bool:
    proc = subprocess.run(
        ["tmux", "send-keys", "-t", session, choice, "Enter"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc.returncode == 0


def _tmux_send_slash_command(session: str, command: str) -> bool:
    subprocess.run(
        ["tmux", "resize-window", "-t", session, "-x", "140", "-y", "50"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    proc = subprocess.run(
        ["tmux", "send-keys", "-t", session, "C-u", command, "Enter"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc.returncode == 0


def _tmux_send_keys(session: str, *keys: str) -> bool:
    proc = subprocess.run(
        ["tmux", "send-keys", "-t", session, *keys],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )
    return proc.returncode == 0


def _tmux_send_keys_slow(session: str, *keys: str, delay_s: float = 0.35) -> bool:
    for key in keys:
        if not _tmux_send_keys(session, key):
            return False
        time.sleep(delay_s)
    return True


def _tmux_send_escape(session: str) -> None:
    subprocess.run(
        ["tmux", "send-keys", "-t", session, "Escape"],
        check=False,
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL,
    )


def _permission_choice(text: str | None) -> str | None:
    if text is None:
        return None
    stripped = text.strip()
    return stripped if stripped in {"1", "2", "3"} else None


def _looks_like_claude_permission_prompt(text: str) -> bool:
    normalized = text.lower()
    return (
        "do you want to proceed" in normalized
        or "allow reading from" in normalized
        or "yes, allow" in normalized
    )


def _latest_takopi_segment(pane: str) -> str:
    marker = "← takopi:"
    idx = pane.rfind(marker)
    return pane[idx:] if idx >= 0 else pane


def _extract_live_progress_text(pane: str) -> str:
    clean = _normalize_interactive_text(_latest_takopi_segment(pane))
    lines: list[str] = []
    for raw in clean.splitlines():
        s = raw.strip()
        if not s:
            continue
        if s.startswith("❯"):
            break
        if s.startswith("← takopi:"):
            continue
        if "gh auth login" in s:
            continue
        lowered = s.lower()
        if "how is claude doing this session" in lowered:
            break
        if re.search(r"\b1:\s*bad\b", lowered) and re.search(r"\b3:\s*good\b", lowered):
            break
        if "calling takopi" in lowered or "called takopi" in lowered:
            continue
        if "composing…" in lowered or "composing..." in lowered:
            continue
        if s.startswith(("●", "⏺")):
            body = re.sub(r"^[●⏺]\s*", "", s)
            lines.append(body)
            continue
        if s.startswith("✻"):
            body = re.sub(r"^✻\s*", "", s)
            lines.append(f"↻ {body}")
            continue
        lines.append(s)
    if not lines:
        return "↻ Working…"
    return "\n".join(lines[-12:])


def channel_bridge_status_text(cfg: TelegramBridgeConfig) -> str:
    bridge = cfg.channel_bridge
    lines = [
        "Takopi channel bridge:",
        f"enabled: {'yes' if bridge.enabled else 'no'}",
        f"inbound: {bridge.inbound_url}",
        f"reply: {bridge.reply_host}:{bridge.reply_port}",
        f"live progress: {'yes' if bridge.live_progress else 'no'}",
        f"tmux: {bridge.tmux_session or '-'}",
    ]
    if bridge.tmux_session:
        pane = _tmux_capture(bridge.tmux_session)
        if pane:
            progress_text = _extract_live_progress_text(pane)
            prompt_visible = _looks_like_claude_permission_prompt(progress_text)
            lines.append(f"permission prompt visible: {'yes' if prompt_visible else 'no'}")
            if progress_text:
                preview = progress_text.strip().replace("\n", " / ")
                if len(preview) > 240:
                    preview = f"{preview[:239]}…"
                lines.append(f"visible state: {preview}")
        else:
            lines.append("tmux capture: unavailable")
    return "\n".join(lines)


def _format_channel_slash_result(command: str, text: str) -> str:
    body = text.strip()
    if not body:
        body = "Claude Code returned an empty response."
    elif command == "/usage":
        body = _format_usage_overlay_for_telegram(body)
    elif command == "/status":
        body = _format_status_overlay_for_telegram(body)
    elif command == "/stats":
        body = _format_stats_overlay_for_telegram(body)
    return f"Claude Code {command}:\n\n{body}"


def _current_model_from_options(options: Sequence[str]) -> str | None:
    for option in options:
        if "current" not in option.lower():
            continue
        marker = " · current · "
        if marker in option:
            value = option.split(marker, 1)[1]
        else:
            value = re.sub(r"^\d+\.\s+", "", option)
        value = value.split(" · ", 1)[0].strip()
        return value or None
    return None


def _model_options_from_overlay(text: str) -> tuple[list[str], str]:
    clean = _normalize_interactive_text(text)
    options: list[str] = []
    effort = ""
    current: str | None = None
    for raw in clean.splitlines():
        line = re.sub(r"\s+", " ", raw.strip())
        if not line:
            continue
        if line in {"Select model"}:
            continue
        if line.startswith("Switch between Claude models"):
            continue
        if line.startswith("sessions. For other/previous"):
            continue
        if "Enter to confirm" in line or "Enter to set as default" in line:
            continue
        if "Esc to cancel" in line or "s to use this session only" in line:
            continue
        option = re.match(r"^(?:❯ )?([1-9])\.\s+(.+)$", line)
        if option is not None:
            if current is not None:
                options.append(current)
            current = f"{option.group(1)}. {option.group(2).replace(' ✔ ', ' · current · ')}"
            continue
        if "effort" in line.lower():
            effort = line.replace("◉", "").strip()
            continue
        if current is not None:
            current = f"{current} {line}"
    if current is not None:
        options.append(current)
    return options, effort


def _format_model_overlay_for_telegram(text: str) -> str:
    options, effort = _model_options_from_overlay(text)
    current_model = _current_model_from_options(options)
    lines = ["Claude Code model:"]
    if current_model:
        lines.append(f"Current: {current_model}")
        lines.append("")
    lines.append("Available models:")
    lines.extend(f"{_TELEGRAM_BULLET} {option}" for option in options)
    if effort:
        lines.append("")
        lines.append(f"Effort: {effort}")
    lines.append("")
    lines.append("Use `/model 1`, `/model 2`, `/model 3`, or `/model 4`.")
    return "  \n".join(lines).strip()


def _current_model_from_overlay(text: str) -> str | None:
    options, _effort = _model_options_from_overlay(text)
    return _current_model_from_options(options)


def _focused_model_index_from_overlay(text: str) -> int | None:
    clean = _normalize_interactive_text(text)
    for raw in clean.splitlines():
        line = re.sub(r"\s+", " ", raw.strip())
        option = re.match(r"^❯\s+([1-9])\.\s+", line)
        if option is not None:
            return int(option.group(1))
    return None


def _format_usage_overlay_for_telegram(text: str) -> str:
    section_names = {
        "Session",
        "Usage by model:",
        "Current session",
        "Current week (all models)",
        "Current week (Sonnet only)",
        "Extra usage",
    }
    lines: list[str] = []
    current_section = ""
    sections_with_usage_bar: set[str] = set()
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw.strip())
        if not line:
            continue
        if "Settings Status Config Usage Stats" in line:
            continue
        if line == "Refreshing…":
            continue
        if line.startswith("Refresh"):
            continue
        if re.fullmatch(r"\d{1,2}:\d{2}(?:am|pm) \(UTC\)", line):
            continue
        if line.startswith("Rese") and not line.startswith("Resets "):
            continue
        if re.fullmatch(r"[█▌▐▛▜▝▘ ]+\d*", line):
            continue
        if line in section_names:
            current_section = line.rstrip(":")
            if lines:
                lines.append("")
            lines.append(current_section)
            continue
        if re.match(r"^(?:[█▌▐▛▜▝▘ ]+ )?\d+% used$", line):
            if current_section in sections_with_usage_bar:
                continue
            sections_with_usage_bar.add(current_section)
            lines.append(f"{_TELEGRAM_BULLET} {line}")
            continue
        if line.startswith("Resets ") or "not enabled" in line:
            lines.append(f"{_TELEGRAM_BULLET} {line}")
            continue
        if current_section == "Usage by model" and lines and lines[-1].startswith(f"{_TELEGRAM_BULLET} "):
            lines[-1] = f"{lines[-1]} {line}"
            continue
        if ":" in line:
            lines.append(f"{_TELEGRAM_BULLET} {line}")
            continue
        lines.append(line)
    return "  \n".join(lines).strip() or text.strip()


def _format_status_overlay_for_telegram(text: str) -> str:
    lines: list[str] = []
    for raw in text.splitlines():
        line = re.sub(r"\s+", " ", raw.strip())
        if not line:
            continue
        if "Settings Status Config Usage Stats" in line:
            continue
        if "dialog dismissed" in line or "Esc to cancel" in line:
            continue
        if ":" in line:
            lines.append(f"{_TELEGRAM_BULLET} {line}")
            continue
        if lines and lines[-1].startswith(f"{_TELEGRAM_BULLET} "):
            lines[-1] = f"{lines[-1]} {line}"
            continue
        lines.append(line)
    return "  \n".join(lines).strip() or text.strip()


def _format_stats_overlay_for_telegram(text: str) -> str:
    graph_lines: list[str] = []
    detail_lines: list[str] = []
    for raw in text.splitlines():
        stripped = raw.strip()
        if not stripped:
            continue
        normalized = re.sub(r"\s+", " ", stripped)
        if "Settings Status Config Usage Stats" in normalized:
            continue
        if "dialog dismissed" in normalized or "Esc to cancel" in normalized:
            continue
        if normalized.startswith("↓ stats") or "ctrl+s to copy" in normalized:
            continue
        parts = [part.strip() for part in re.split(r"\s{2,}", stripped) if part.strip()]
        if len(parts) > 1 and all(":" in part for part in parts):
            detail_lines.extend(f"{_TELEGRAM_BULLET} {part}" for part in parts)
            continue
        if ":" in normalized and not any(char in normalized for char in "░▒▓█"):
            detail_lines.append(f"{_TELEGRAM_BULLET} {normalized}")
            continue
        if any(char in normalized for char in "░▒▓█") or "··" in normalized:
            graph_lines.append(raw.rstrip())
            continue
        if normalized.startswith(("You've used ", "Your ")):
            detail_lines.append(normalized)
            continue
        graph_lines.append(normalized)
    lines: list[str] = []
    if graph_lines:
        graph = textwrap.dedent("\n".join(graph_lines)).strip("\n")
        lines.append(f"```\n{graph}\n```")
    if graph_lines and detail_lines:
        details = "  \n".join(detail_lines)
        return f"{lines[0]}\n\n{details}".strip()
    lines.extend(detail_lines)
    return "  \n".join(lines).strip() or text.strip()


def _capture_channel_slash_command(session: str, command: str) -> str:
    sent = _tmux_send_slash_command(session, command)
    if not sent:
        return f"failed to send {command} to Claude tmux session."

    deadline = time.time() + _CHANNEL_SLASH_TIMEOUT_S
    last_text = ""
    while time.time() < deadline:
        pane = _tmux_capture(session)
        if _is_interactive_slash_overlay(command, pane):
            text = _format_interactive_slash_overlay(command, pane)
            _tmux_send_escape(session)
            return _format_channel_slash_result(command, text)
        live_text = _extract_live_progress_text(pane)
        if live_text and live_text != "↻ Working…":
            last_text = live_text
        time.sleep(0.5)
    if last_text:
        return _format_channel_slash_result(command, last_text)
    return f"timed out waiting for Claude Code {command} output."


def _wait_for_model_overlay(session: str) -> str:
    deadline = time.time() + _CHANNEL_SLASH_TIMEOUT_S
    last_segment = ""
    while time.time() < deadline:
        pane = _tmux_capture(session)
        segment = _slash_segment_after_latest_prompt("/model", pane) or pane
        if "Select model" in segment:
            return segment
        last_segment = segment
        time.sleep(0.5)
    return last_segment


def _capture_current_model(session: str) -> str | None:
    if not _tmux_send_slash_command(session, "/model"):
        return None
    segment = _wait_for_model_overlay(session)
    try:
        if "Select model" not in segment:
            return None
        return _current_model_from_overlay(segment)
    finally:
        _tmux_send_escape(session)


def _capture_channel_model_command(session: str, selection: str | None) -> str:
    sent = _tmux_send_slash_command(session, "/model")
    if not sent:
        return "failed to send /model to Claude tmux session."

    segment = _wait_for_model_overlay(session)
    if "Select model" not in segment:
        _tmux_send_escape(session)
        return "timed out waiting for Claude Code /model output."

    if selection is None:
        _tmux_send_escape(session)
        return f"Claude Code /model:\n\n{_format_model_overlay_for_telegram(segment)}"

    if selection not in {"1", "2", "3", "4"}:
        _tmux_send_escape(session)
        return "usage: `/model`, `/model 1`, `/model 2`, `/model 3`, or `/model 4`"

    focused_index = _focused_model_index_from_overlay(segment) or 1
    target_index = int(selection)
    offset = target_index - focused_index
    moves = ["Down"] * offset if offset > 0 else ["Up"] * abs(offset)
    # Use "s" so Telegram changes the live session without overwriting the
    # user's default Claude Code model for future sessions.
    time.sleep(0.2)
    _tmux_send_keys_slow(session, *moves, "s")
    status_line = ""
    deadline = time.time() + 8.0
    while time.time() < deadline:
        pane = _tmux_capture(session)
        segment = _slash_segment_after_latest_prompt("/model", pane) or pane
        clean = _normalize_interactive_text(segment)
        for raw in reversed(clean.splitlines()):
            line = raw.strip()
            lowered = line.lower()
            if "model as" in lowered or "switched model" in lowered or "set model" in lowered:
                status_line = line.lstrip("⎿ ").strip()
                break
        if status_line:
            break
        time.sleep(0.5)
    if status_line:
        return f"Claude Code /model:\n\n{status_line}"
    if selection == "1":
        current_model = _capture_current_model(session)
        if current_model is not None:
            return f"Claude Code /model:\n\nCurrent: {current_model}"
    return (
        "Claude Code /model:\n\n"
        f"Could not confirm that option {selection} changed the active model."
    )



async def channel_bridge_slash_command_text(
    cfg: TelegramBridgeConfig,
    command: str,
) -> str:
    normalized = command.strip().lower()
    if normalized not in _CHANNEL_SLASH_COMMANDS:
        return f"Claude Code slash command is not allowlisted: {command}"
    bridge = cfg.channel_bridge
    if not bridge.enabled:
        return "channel bridge is disabled."
    if not bridge.tmux_session:
        return "Claude Code tmux session is not configured."
    if normalized == "/model":
        return await asyncio.to_thread(
            _capture_channel_model_command,
            bridge.tmux_session,
            None,
        )
    return await asyncio.to_thread(
        _capture_channel_slash_command,
        bridge.tmux_session,
        normalized,
    )


async def channel_bridge_model_command_text(
    cfg: TelegramBridgeConfig,
    args_text: str,
) -> str:
    bridge = cfg.channel_bridge
    if not bridge.enabled:
        return "channel bridge is disabled."
    if not bridge.tmux_session:
        return "Claude Code tmux session is not configured."
    selection = args_text.strip() or None
    return await asyncio.to_thread(
        _capture_channel_model_command,
        bridge.tmux_session,
        selection,
    )


async def _register_live_progress(chat_id: int, user_msg_id: int, run: LiveProgressRun) -> None:
    async with _LIVE_PROGRESS_LOCK:
        source_key = (chat_id, user_msg_id)
        progress_key = (chat_id, run.progress_ref.message_id)
        _LIVE_PROGRESS_RUNS[source_key] = run
        _LIVE_PROGRESS_BY_PROGRESS[progress_key] = source_key


async def _pop_live_progress(chat_id: int, user_msg_id: int) -> LiveProgressRun | None:
    async with _LIVE_PROGRESS_LOCK:
        source_key = (chat_id, user_msg_id)
        run = _LIVE_PROGRESS_RUNS.pop(source_key, None)
        if run is not None:
            _LIVE_PROGRESS_BY_PROGRESS.pop((chat_id, run.progress_ref.message_id), None)
        return run


async def handle_live_progress_choice(
    cfg: TelegramBridgeConfig,
    *,
    chat_id: int,
    reply_to_message_id: int | None,
    text: str | None,
    reply: Callable[..., Awaitable[None]],
) -> bool:
    choice = _permission_choice(text)
    if choice is None or reply_to_message_id is None:
        return False

    async with _LIVE_PROGRESS_LOCK:
        source_key = _LIVE_PROGRESS_BY_PROGRESS.get((chat_id, reply_to_message_id))
        run = _LIVE_PROGRESS_RUNS.get(source_key) if source_key is not None else None

    if run is None:
        return False

    session = cfg.channel_bridge.tmux_session
    if not session:
        await reply(text="live progress tmux session is not configured.")
        return True

    pane = await asyncio.to_thread(_tmux_capture, session)
    progress_text = _extract_live_progress_text(pane)
    if not _looks_like_claude_permission_prompt(progress_text):
        await reply(text="no visible Claude permission prompt to answer.")
        return True

    sent = await asyncio.to_thread(_tmux_send_choice, session, choice)
    if not sent:
        await reply(text="failed to send permission choice to Claude tmux session.")
        return True

    run.last_text = progress_text
    await reply(text=f"sent permission choice `{choice}` to Claude.")
    return True


async def _run_live_progress(
    cfg: TelegramBridgeConfig,
    *,
    run: LiveProgressRun,
    tmux_session: str,
    engine: str,
) -> None:
    poll_s = cfg.channel_bridge.poll_interval_s
    try:
        while not run.done.is_set():
            pane = await asyncio.to_thread(_tmux_capture, tmux_session)
            text = _extract_live_progress_text(pane)
            if text != run.last_text:
                run.last_text = text
                await _maybe_send_verbose_actions(cfg, run=run, text=text)
                await cfg.exec_cfg.transport.edit(
                    ref=run.progress_ref,
                    message=_render_live_progress(
                        text=text,
                        elapsed_s=time.time() - run.started_at,
                        status="working",
                        engine=engine,
                    ),
                )
            try:
                await asyncio.wait_for(run.done.wait(), timeout=poll_s)
            except TimeoutError:
                continue
        pane = await asyncio.to_thread(_tmux_capture, tmux_session)
        text = _extract_live_progress_text(pane)
        await _maybe_send_verbose_actions(cfg, run=run, text=text)
        await cfg.exec_cfg.transport.edit(
            ref=run.progress_ref,
            message=_render_live_progress(
                text=text,
                elapsed_s=time.time() - run.started_at,
                status="done",
                engine=engine,
            ),
        )
    except (OSError, RuntimeError, ValueError, subprocess.SubprocessError) as exc:
        logger.warning("telegram.channel_bridge.live_progress_failed", error=str(exc))


def _verbose_action_lines(text: str) -> list[str]:
    out: list[str] = []
    for raw in text.splitlines():
        line = raw.strip()
        if not line or line == "↻ Working…":
            continue
        lowered = line.lower()
        if "how is claude doing this session" in lowered:
            continue
        if re.search(r"\b1:\s*bad\b", lowered) and re.search(r"\b3:\s*good\b", lowered):
            continue
        if "calling takopi" in lowered or "called takopi" in lowered:
            continue
        if re.search(r"\b(?:Worked|Brewed|Baked|Cooked|Crunched|Cogitated|Sautéed|Churned) for \d", line):
            continue
        if len(line) > 220:
            line = f"{line[:219]}…"
        out.append(line)
    return out[-6:]


async def _maybe_send_verbose_actions(
    cfg: TelegramBridgeConfig,
    *,
    run: LiveProgressRun,
    text: str,
) -> None:
    if not run.verbose or run.verbose_sent >= _MAX_VERBOSE_ACTION_MESSAGES:
        return
    for line in _verbose_action_lines(text):
        if line in run.verbose_seen:
            continue
        run.verbose_seen.add(line)
        run.verbose_sent += 1
        await cfg.exec_cfg.transport.send(
            channel_id=run.chat_id,
            message=_render_channel_reply(f"Claude action:\n{_TELEGRAM_BULLET} {line}"),
            options=SendOptions(
                reply_to=MessageRef(channel_id=run.chat_id, message_id=run.user_msg_id),
                thread_id=run.thread_id,
                notify=False,
            ),
        )
        if run.verbose_sent >= _MAX_VERBOSE_ACTION_MESSAGES:
            return


async def maybe_start_live_progress(
    cfg: TelegramBridgeConfig,
    *,
    chat_id: int,
    user_msg_id: int,
    thread_id: int | None,
    engine: str | None,
    verbose: bool = False,
) -> None:
    bridge = cfg.channel_bridge
    if not bridge.live_progress or not bridge.tmux_session:
        return
    sent = await cfg.exec_cfg.transport.send(
        channel_id=chat_id,
        message=_render_live_progress(
            text="↻ Working…",
            elapsed_s=0,
            status="working",
            engine=engine or "claude",
        ),
        options=SendOptions(
            reply_to=MessageRef(channel_id=chat_id, message_id=user_msg_id),
            thread_id=thread_id,
            notify=False,
        ),
    )
    if sent is None:
        return
    run = LiveProgressRun(
        progress_ref=sent,
        started_at=time.time(),
        chat_id=chat_id,
        user_msg_id=user_msg_id,
        thread_id=thread_id,
        verbose=verbose,
    )
    await _register_live_progress(chat_id, user_msg_id, run)
    asyncio.create_task(
        _run_live_progress(
            cfg,
            run=run,
            tmux_session=bridge.tmux_session,
            engine=engine or "claude",
        )
    )


def _context_payload(context: RunContext | None) -> dict[str, Any] | None:
    if context is None:
        return None
    try:
        return asdict(context)
    except TypeError:
        return {
            "project": getattr(context, "project", None),
            "branch": getattr(context, "branch", None),
        }


async def forward_to_channel(
    cfg: TelegramBridgeConfig,
    *,
    chat_id: int,
    user_msg_id: int,
    text: str,
    context: RunContext | None,
    thread_id: int | None,
    engine: str | None,
    verbose: bool = False,
) -> bool:
    bridge = cfg.channel_bridge
    headers: dict[str, str] = {}
    if bridge.shared_secret:
        headers["authorization"] = f"Bearer {bridge.shared_secret}"
    payload = {
        "chat_id": str(chat_id),
        "message_id": str(user_msg_id),
        "thread_id": str(thread_id) if thread_id is not None else "",
        "text": text,
        "context": _context_payload(context),
        "project": getattr(context, "project", "") if context is not None else "",
        "branch": getattr(context, "branch", "") if context is not None else "",
        "engine": engine or "claude",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(bridge.inbound_url, json=payload, headers=headers)
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("telegram.channel_bridge.forward_failed", error=str(exc))
        await send_plain(
            cfg.exec_cfg.transport,
            chat_id=chat_id,
            user_msg_id=user_msg_id,
            text=f"channel bridge unavailable: {exc}",
            thread_id=thread_id,
        )
        return False
    await maybe_start_live_progress(
        cfg,
        chat_id=chat_id,
        user_msg_id=user_msg_id,
        thread_id=thread_id,
        engine=engine,
        verbose=verbose,
    )
    if bridge.send_progress:
        await send_plain(
            cfg.exec_cfg.transport,
            chat_id=chat_id,
            user_msg_id=user_msg_id,
            text="forwarded to Claude Code channel",
            notify=False,
            thread_id=thread_id,
        )
    return True


async def run_reply_server(cfg: TelegramBridgeConfig) -> None:
    bridge = cfg.channel_bridge
    if not bridge.enabled:
        return

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            try:
                method, path, _version = request_line.decode("latin1").strip().split(" ", 2)
            except ValueError:
                await _write_response(writer, 400, b"bad request")
                return
            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin1").partition(":")
                headers[name.strip().lower()] = value.strip()
            length = int(headers.get("content-length", "0") or "0")
            body = await reader.readexactly(length) if length else b"{}"
            if method != "POST" or path != "/reply":
                await _write_response(writer, 404, b"not found")
                return
            if bridge.shared_secret:
                expected = f"Bearer {bridge.shared_secret}"
                if headers.get("authorization") != expected:
                    await _write_response(writer, 401, b"unauthorized")
                    return
            data = json.loads(body.decode("utf-8"))
            chat_id = int(data["chat_id"])
            text = str(data["text"])
            reply_to_raw = data.get("reply_to_message_id") or data.get("message_id")
            reply_to_message_id = int(reply_to_raw) if reply_to_raw else None
            thread_raw = data.get("thread_id")
            thread_id = int(thread_raw) if thread_raw not in (None, "") else None
            reply_to = (
                MessageRef(channel_id=chat_id, message_id=reply_to_message_id)
                if reply_to_message_id is not None
                else None
            )
            await cfg.exec_cfg.transport.send(
                channel_id=chat_id,
                message=_render_channel_reply(text),
                options=SendOptions(reply_to=reply_to, thread_id=thread_id),
            )
            if reply_to_message_id is not None:
                run = await _pop_live_progress(chat_id, reply_to_message_id)
                if run is not None:
                    run.done.set()
            await _write_response(writer, 200, b"ok")
        except (OSError, ValueError, json.JSONDecodeError, KeyError) as exc:
            logger.exception("telegram.channel_bridge.reply_failed", error=str(exc))
            with suppress(OSError):
                await _write_response(writer, 500, str(exc).encode())
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, bridge.reply_host, bridge.reply_port)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    logger.info("telegram.channel_bridge.reply_server_started", bind=addrs)
    async with server:
        await server.serve_forever()


async def _write_response(writer: asyncio.StreamWriter, status: int, body: bytes) -> None:
    reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 500: "Internal Server Error"}.get(status, "OK")
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\ncontent-length: {len(body)}\r\nconnection: close\r\n\r\n".encode("latin1") + body
    )
    await writer.drain()
