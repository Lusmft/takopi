from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from typing import Any

from ..progress import ProgressTracker
from ..runner_bridge import IncomingMessage
from .bridge import TelegramBridgeConfig

_IMAGE_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"})
_MAX_ARTIFACT_BYTES = 20 * 1024 * 1024


@dataclass(frozen=True, slots=True)
class TelegramArtifact:
    path: Path
    rel_path: str


def _iter_file_change_paths(tracker: ProgressTracker) -> list[str]:
    paths: list[str] = []
    seen: set[str] = set()
    state = tracker.snapshot()
    for action_state in state.actions:
        action = action_state.action
        if action.kind != "file_change" or action_state.ok is False:
            continue
        detail = action.detail
        changes = detail.get("changes")
        if isinstance(changes, list):
            for change in changes:
                if not isinstance(change, dict):
                    continue
                path = change.get("path")
                if isinstance(path, str) and path and path not in seen:
                    paths.append(path)
                    seen.add(path)
        title = action.title
        if title and title not in seen:
            paths.append(title)
            seen.add(title)
    return paths


def _resolve_artifact_path(raw_path: str, *, cwd: Path) -> Path | None:
    candidate = Path(raw_path).expanduser()
    if not candidate.is_absolute():
        candidate = cwd / candidate
    try:
        resolved = candidate.resolve(strict=False)
        resolved.relative_to(cwd.resolve(strict=False))
    except (OSError, ValueError):
        return None
    return resolved


def collect_image_artifacts(
    *,
    tracker: ProgressTracker,
    cwd: Path | None,
    max_count: int = 4,
) -> list[TelegramArtifact]:
    if cwd is None:
        return []
    artifacts: list[TelegramArtifact] = []
    seen: set[Path] = set()
    for raw_path in _iter_file_change_paths(tracker):
        path = _resolve_artifact_path(raw_path, cwd=cwd)
        if path is None or path in seen:
            continue
        seen.add(path)
        if path.suffix.lower() not in _IMAGE_SUFFIXES:
            continue
        try:
            stat = path.stat()
        except OSError:
            continue
        if not path.is_file() or stat.st_size > _MAX_ARTIFACT_BYTES:
            continue
        try:
            rel_path = str(path.relative_to(cwd))
        except ValueError:
            rel_path = path.name
        artifacts.append(TelegramArtifact(path=path, rel_path=rel_path))
        if len(artifacts) >= max_count:
            break
    return artifacts


async def send_image_artifacts(
    cfg: TelegramBridgeConfig,
    *,
    incoming: IncomingMessage,
    tracker: ProgressTracker,
    cwd: Path | None,
) -> int:
    artifacts = collect_image_artifacts(tracker=tracker, cwd=cwd)
    sent_count = 0
    for artifact in artifacts:
        try:
            content = artifact.path.read_bytes()
        except OSError:
            continue
        sent = await cfg.bot.send_document(
            chat_id=int(incoming.channel_id),
            filename=artifact.path.name,
            content=content,
            reply_to_message_id=int(incoming.message_id),
            message_thread_id=int(incoming.thread_id) if incoming.thread_id is not None else None,
            caption=f"artifact: {artifact.rel_path}",
        )
        if sent is not None:
            sent_count += 1
    return sent_count
