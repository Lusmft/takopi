from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
from ..logging import get_logger
from ..progress import ProgressTracker
from ..runner_bridge import IncomingMessage
from .bridge import TelegramBridgeConfig

logger = get_logger(__name__)

_IMAGE_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".webp", ".gif", ".svg"})
_PHOTO_SUFFIXES = frozenset({".png", ".jpg", ".jpeg", ".webp"})
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


def _iter_recent_image_files(cwd: Path, *, since: float | None) -> list[Path]:
    roots = [cwd, cwd / "artifacts"]
    paths: list[Path] = []
    seen: set[Path] = set()
    for root in roots:
        if not root.exists() or not root.is_dir():
            continue
        pattern = "*" if root == cwd else "**/*"
        for path in root.glob(pattern):
            if path in seen or path.suffix.lower() not in _IMAGE_SUFFIXES:
                continue
            seen.add(path)
            try:
                stat = path.stat()
            except OSError:
                continue
            if since is None or stat.st_mtime >= since:
                paths.append(path)
    def mtime(path: Path) -> float:
        try:
            return path.stat().st_mtime
        except OSError:
            return 0.0

    return sorted(paths, key=mtime, reverse=True)


def collect_image_artifacts(
    *,
    tracker: ProgressTracker,
    cwd: Path | None,
    max_count: int = 4,
    since: float | None = None,
) -> list[TelegramArtifact]:
    if cwd is None:
        return []
    artifacts: list[TelegramArtifact] = []
    seen: set[Path] = set()
    candidates: list[Path] = []
    # Prefer filesystem scan first. It is cheap, independent from runner-specific
    # action parsing, and covers the common path where Claude creates
    # ./artifacts/*.png then only mentions the path in the final answer.
    candidates.extend(_iter_recent_image_files(cwd, since=since))
    file_change_paths: list[str] = []
    # For explicit image/screenshot requests the caller passes since=None.
    # In that hot path, avoid tracker/action parsing entirely: it is runner-specific
    # and can block delivery even when the artifact already exists on disk.
    if since is not None:
        try:
            file_change_paths = _iter_file_change_paths(tracker)
        except Exception as exc:
            logger.warning(
                "telegram.artifacts.tracker_scan_failed",
                error=str(exc),
                error_type=type(exc).__name__,
            )
    for raw_path in file_change_paths:
        path = _resolve_artifact_path(raw_path, cwd=cwd)
        if path is not None:
            candidates.append(path)

    for path in candidates:
        if path in seen:
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
    since: float | None = None,
) -> int:
    logger.info(
        "telegram.artifacts.collect.start",
        cwd=str(cwd) if cwd is not None else None,
        since=since,
    )
    artifacts = collect_image_artifacts(tracker=tracker, cwd=cwd, since=since)
    logger.info(
        "telegram.artifacts.collect",
        cwd=str(cwd) if cwd is not None else None,
        count=len(artifacts),
        paths=[artifact.rel_path for artifact in artifacts],
    )
    photo_items: list[tuple[TelegramArtifact, bytes]] = []
    document_items: list[tuple[TelegramArtifact, bytes]] = []
    for artifact in artifacts:
        try:
            content = artifact.path.read_bytes()
        except OSError:
            continue
        if artifact.path.suffix.lower() in _PHOTO_SUFFIXES and hasattr(cfg.bot, "send_photo"):
            photo_items.append((artifact, content))
        else:
            document_items.append((artifact, content))

    sent_count = 0
    if len(photo_items) >= 2 and hasattr(cfg.bot, "send_media_group"):
        media: list[dict[str, str]] = []
        files: dict[str, tuple[str, bytes]] = {}
        for index, (artifact, content) in enumerate(photo_items):
            attachment_name = f"photo{index}"
            item: dict[str, str] = {"type": "photo", "media": f"attach://{attachment_name}"}
            if index == 0:
                item["caption"] = f"{len(photo_items)} images"
            media.append(item)
            files[attachment_name] = (artifact.path.name, content)
        await cfg.bot.send_media_group(
            chat_id=int(incoming.channel_id),
            media=media,
            files=files,
            reply_to_message_id=int(incoming.message_id),
            message_thread_id=int(incoming.thread_id) if incoming.thread_id is not None else None,
            wait=False,
        )
        sent_count += len(photo_items)
        logger.info(
            "telegram.artifacts.enqueued",
            paths=[artifact.rel_path for artifact, _ in photo_items],
            method="media_group",
        )
    else:
        for artifact, content in photo_items:
            await cfg.bot.send_photo(
                chat_id=int(incoming.channel_id),
                filename=artifact.path.name,
                content=content,
                reply_to_message_id=int(incoming.message_id),
                message_thread_id=int(incoming.thread_id) if incoming.thread_id is not None else None,
                caption=artifact.rel_path,
                wait=False,
            )
            sent_count += 1
            logger.info("telegram.artifacts.enqueued", path=artifact.rel_path, method="photo")

    for artifact, content in document_items:
        await cfg.bot.send_document(
            chat_id=int(incoming.channel_id),
            filename=artifact.path.name,
            content=content,
            reply_to_message_id=int(incoming.message_id),
            message_thread_id=int(incoming.thread_id) if incoming.thread_id is not None else None,
            caption=artifact.rel_path,
            wait=False,
        )
        sent_count += 1
        logger.info("telegram.artifacts.enqueued", path=artifact.rel_path, method="document")
    return sent_count
