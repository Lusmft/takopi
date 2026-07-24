from __future__ import annotations

import json
import os
from pathlib import Path
import re
import shlex
import shutil
import subprocess
import time
from typing import Any
import zlib

import anyio

from .config import HOME_CONFIG_PATH
from .settings import load_settings
from .telegram.client import TelegramClient


JOBS_ROOT = Path.home() / ".takopi" / "jobs"
MIN_RELEASE_DEPLOY_TIMEOUT_S = 90 * 60
_JOB_ID_RE = re.compile(r"^[a-z0-9][a-z0-9_.-]{0,63}$")
_DETACHED_COMMAND_RE = re.compile(
    r"(^|[\s;|()])(nohup|setsid|disown)(?=\s|$)",
    re.IGNORECASE,
)
_GH_RUN_WATCH_RE = re.compile(r"(^|[\s;&|])gh\s+run\s+watch(?=\s|$)", re.IGNORECASE)


class JobError(RuntimeError):
    pass


def _is_release_deploy_job(*, job_id: str, title: str | None, script: str) -> bool:
    metadata = f"{job_id}\n{title or ''}".lower()
    script_lower = script.lower()
    return (
        any(marker in metadata for marker in ("release", "deploy"))
        or "make release" in script_lower
        or "gh run " in script_lower
    )


def validate_job_script(
    *,
    job_id: str,
    title: str | None,
    script: str,
    timeout_s: int,
) -> None:
    """Reject fragile durable-job patterns before starting external work."""
    if _GH_RUN_WATCH_RE.search(script):
        raise JobError(
            "durable jobs must not use interactive `gh run watch`; poll "
            "`gh run view <id> --json status,conclusion` quietly and print only "
            "state changes plus the final conclusion"
        )
    if (
        _is_release_deploy_job(job_id=job_id, title=title, script=script)
        and timeout_s < MIN_RELEASE_DEPLOY_TIMEOUT_S
    ):
        raise JobError(
            "release/deploy durable jobs require --timeout >= "
            f"{MIN_RELEASE_DEPLOY_TIMEOUT_S} (90 minutes)"
        )


def background_guard_reason(tool_input: dict[str, Any]) -> str | None:
    """Return why a Bash tool call must use a durable job, if applicable."""
    if tool_input.get("run_in_background") is True:
        return "Bash run_in_background is not durable after this Claude run exits"

    command = tool_input.get("command")
    if not isinstance(command, str) or not command.strip():
        return None
    if _DETACHED_COMMAND_RE.search(command):
        return "detached shell commands are not durable after this Claude run exits"

    try:
        lexer = shlex.shlex(command, posix=True, punctuation_chars=";&|<>")
        lexer.whitespace_split = True
        tokens = list(lexer)
    except ValueError:
        tokens = command.split()
    if "&" in tokens:
        return (
            "shell background operator '&' is not durable after this Claude run exits"
        )

    executable_names = {Path(token).name for token in tokens}
    if "systemd-run" in executable_names and "--wait" not in tokens:
        return "direct detached systemd-run has no Takopi completion callback"
    return None


def background_guard_decision(payload: dict[str, Any]) -> dict[str, Any] | None:
    """Build a Claude Code PreToolUse denial for unsafe background Bash calls."""
    if payload.get("tool_name") != "Bash":
        return None
    tool_input = payload.get("tool_input")
    if not isinstance(tool_input, dict):
        return None
    reason = background_guard_reason(tool_input)
    if reason is None:
        return None
    guidance = (
        f"{reason}. Do not retry with nohup, setsid, '&', or detached systemd-run. "
        "Write the complete wait/action/verification flow to a shell script and run "
        "`takopi jobs start <unique-id> --script <path> --chat-id <chat-id> "
        "--timeout <seconds> --title <title>`. The durable job sends the final "
        "output to Telegram."
    )
    return {
        "hookSpecificOutput": {
            "hookEventName": "PreToolUse",
            "permissionDecision": "deny",
            "permissionDecisionReason": guidance,
        }
    }


def validate_job_id(value: str) -> str:
    job_id = value.strip().lower()
    if not _JOB_ID_RE.fullmatch(job_id):
        raise JobError(
            "job id must be 1-64 lowercase letters, digits, dots, dashes, "
            "or underscores"
        )
    return job_id


def job_dir(job_id: str, *, root: Path = JOBS_ROOT) -> Path:
    return root / validate_job_id(job_id)


def _atomic_json(path: Path, payload: dict[str, Any]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    tmp = path.with_suffix(f"{path.suffix}.{os.getpid()}.tmp")
    tmp.write_text(json.dumps(payload, indent=2, sort_keys=True), encoding="utf-8")
    os.replace(tmp, path)


def read_json(path: Path) -> dict[str, Any] | None:
    try:
        value = json.loads(path.read_text(encoding="utf-8"))
    except (FileNotFoundError, json.JSONDecodeError, OSError):
        return None
    return value if isinstance(value, dict) else None


def unit_name(job_id: str) -> str:
    normalized = validate_job_id(job_id)
    safe = normalized.replace(".", "-").replace("_", "-")
    checksum = zlib.crc32(normalized.encode()).to_bytes(4).hex()
    return f"takopi-job-{safe[:48]}-{checksum}"


def create_job(
    *,
    job_id: str,
    script_path: Path,
    chat_id: int,
    timeout_s: int,
    title: str | None,
    root: Path = JOBS_ROOT,
) -> Path:
    if timeout_s < 1 or timeout_s > 7 * 24 * 60 * 60:
        raise JobError("timeout must be between 1 second and 7 days")
    try:
        script = script_path.read_text(encoding="utf-8")
    except OSError as exc:
        raise JobError(f"cannot read script {script_path}: {exc}") from exc
    if not script.strip():
        raise JobError("job script is empty")
    validate_job_script(
        job_id=job_id,
        title=title,
        script=script,
        timeout_s=timeout_s,
    )

    directory = job_dir(job_id, root=root)
    if directory.exists():
        raise JobError(f"job {job_id!r} already exists")
    directory.mkdir(parents=True, mode=0o700)
    stored_script = directory / "script.sh"
    stored_script.write_text(script, encoding="utf-8")
    stored_script.chmod(0o700)
    _atomic_json(
        directory / "spec.json",
        {
            "id": validate_job_id(job_id),
            "chat_id": chat_id,
            "config_path": str(HOME_CONFIG_PATH),
            "created_at": time.time(),
            "timeout_s": timeout_s,
            "title": title or job_id,
        },
    )
    _atomic_json(
        directory / "state.json",
        {"id": validate_job_id(job_id), "status": "queued", "updated_at": time.time()},
    )
    return directory


def launch_job(job_id: str, *, takopi_executable: str | None = None) -> None:
    executable = takopi_executable or shutil.which("takopi")
    if executable is None:
        raise JobError("cannot find takopi executable on PATH")
    command = [
        "systemd-run",
        "--quiet",
        "--collect",
        f"--unit={unit_name(job_id)}",
        "--property=Type=exec",
        "--property=TimeoutStopSec=15",
        "--property=MemoryMax=1G",
        "--property=TasksMax=128",
        executable,
        "jobs",
        "worker",
        validate_job_id(job_id),
    ]
    try:
        subprocess.run(command, check=True, capture_output=True, text=True)
    except FileNotFoundError as exc:
        raise JobError("systemd-run is not installed") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "systemd-run failed").strip()
        raise JobError(detail) from exc


def start_job(
    *,
    job_id: str,
    script_path: Path,
    chat_id: int,
    timeout_s: int,
    title: str | None,
    root: Path = JOBS_ROOT,
    takopi_executable: str | None = None,
) -> Path:
    directory = create_job(
        job_id=job_id,
        script_path=script_path,
        chat_id=chat_id,
        timeout_s=timeout_s,
        title=title,
        root=root,
    )
    try:
        launch_job(job_id, takopi_executable=takopi_executable)
    except Exception:
        _atomic_json(
            directory / "state.json",
            {"id": job_id, "status": "launch_failed", "updated_at": time.time()},
        )
        raise
    return directory


def _tail(path: Path, *, max_chars: int = 3000) -> str:
    try:
        text = path.read_text(encoding="utf-8", errors="replace")
    except OSError:
        return ""
    if len(text) <= max_chars:
        return text.strip()
    return f"…{text[-max_chars:]}".strip()


async def _notify(chat_id: int, text: str) -> None:
    settings, _ = load_settings()
    telegram = settings.transports.telegram
    if telegram is None:
        raise JobError("telegram transport is not configured")
    bot = TelegramClient(telegram.bot_token)
    try:
        await bot.send_message(chat_id, text, disable_notification=False)
    finally:
        await bot.close()


def notify(chat_id: int, text: str) -> None:
    anyio.run(_notify, chat_id, text)


def run_worker(job_id: str, *, root: Path = JOBS_ROOT) -> int:
    directory = job_dir(job_id, root=root)
    spec = read_json(directory / "spec.json")
    if spec is None:
        raise JobError(f"missing job spec for {job_id!r}")
    script_path = directory / "script.sh"
    output_path = directory / "output.log"
    state_path = directory / "state.json"
    started_at = time.time()
    _atomic_json(
        state_path,
        {"id": job_id, "status": "running", "updated_at": started_at},
    )

    status = "failed"
    return_code = 1
    try:
        with output_path.open("w", encoding="utf-8") as output:
            try:
                completed = subprocess.run(
                    ["/bin/bash", str(script_path)],
                    cwd=directory,
                    stdout=output,
                    stderr=subprocess.STDOUT,
                    timeout=int(spec["timeout_s"]),
                    check=False,
                    text=True,
                    start_new_session=True,
                )
                return_code = completed.returncode
                status = "succeeded" if return_code == 0 else "failed"
            except subprocess.TimeoutExpired:
                return_code = 124
                status = "timed_out"
    except OSError as exc:
        output_path.write_text(f"{type(exc).__name__}: {exc}\n", encoding="utf-8")

    finished_at = time.time()
    result = {
        "id": job_id,
        "status": status,
        "return_code": return_code,
        "started_at": started_at,
        "finished_at": finished_at,
        "duration_s": round(finished_at - started_at, 3),
        "updated_at": finished_at,
    }
    _atomic_json(directory / "result.json", result)
    _atomic_json(state_path, result)

    icon = "✅" if status == "succeeded" else "❌"
    output = _tail(output_path)
    title = str(spec.get("title") or job_id)
    message = (
        f"{icon} Durable job `{title}`: {status}\n"
        f"exit={return_code} · {result['duration_s']}s"
    )
    if output:
        message = f"{message}\n\n{output}"
    try:
        notify(int(spec["chat_id"]), message[:4000])
    except Exception as exc:  # noqa: BLE001
        (directory / "notify_error.log").write_text(
            f"{type(exc).__name__}: {exc}\n", encoding="utf-8"
        )
    return return_code


def cancel_job(job_id: str) -> None:
    try:
        subprocess.run(
            ["systemctl", "stop", f"{unit_name(job_id)}.service"],
            check=True,
            capture_output=True,
            text=True,
        )
    except FileNotFoundError as exc:
        raise JobError("systemctl is not installed") from exc
    except subprocess.CalledProcessError as exc:
        detail = (exc.stderr or exc.stdout or "systemctl stop failed").strip()
        raise JobError(detail) from exc


def job_status(job_id: str, *, root: Path = JOBS_ROOT) -> dict[str, Any]:
    directory = job_dir(job_id, root=root)
    state = read_json(directory / "state.json")
    if state is None:
        raise JobError(f"unknown job {job_id!r}")
    state["unit"] = f"{unit_name(job_id)}.service"
    state["output"] = _tail(directory / "output.log")
    return state


def list_jobs(*, root: Path = JOBS_ROOT) -> list[dict[str, Any]]:
    if not root.exists():
        return []
    jobs: list[dict[str, Any]] = []
    for directory in root.iterdir():
        if not directory.is_dir():
            continue
        state = read_json(directory / "state.json")
        if state is not None:
            jobs.append(state)
    return sorted(jobs, key=lambda item: float(item.get("updated_at", 0)), reverse=True)
