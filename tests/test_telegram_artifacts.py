from __future__ import annotations

from pathlib import Path

import pytest

from takopi.markdown import MarkdownPresenter
from takopi.model import Action, ActionEvent
from takopi.progress import ProgressTracker
from takopi.runner_bridge import ExecBridgeConfig, IncomingMessage
from takopi.runners.mock import Return, ScriptRunner
from takopi.telegram.artifacts import collect_image_artifacts, send_image_artifacts
from takopi.telegram.bridge import TelegramBridgeConfig
from takopi.transport_runtime import TransportRuntime
from tests.telegram_fakes import FakeBot, FakeTransport, _empty_projects, _make_router


def _tracker_with_file_change(path: str) -> ProgressTracker:
    tracker = ProgressTracker(engine="claude")
    tracker.note_event(
        ActionEvent(
            engine="claude",
            action=Action(
                id="write-1",
                kind="file_change",
                title=path,
                detail={"changes": [{"path": path, "kind": "update"}]},
            ),
            phase="completed",
            ok=True,
        )
    )
    return tracker


def test_collect_image_artifacts_from_file_changes(tmp_path: Path) -> None:
    image = tmp_path / "out" / "cat.png"
    image.parent.mkdir()
    image.write_bytes(b"png")
    tracker = _tracker_with_file_change("out/cat.png")

    artifacts = collect_image_artifacts(tracker=tracker, cwd=tmp_path)

    assert len(artifacts) == 1
    assert artifacts[0].path == image
    assert artifacts[0].rel_path == "out/cat.png"


def test_collect_image_artifacts_ignores_non_images(tmp_path: Path) -> None:
    note = tmp_path / "notes.txt"
    note.write_text("hello")
    tracker = _tracker_with_file_change("notes.txt")

    assert collect_image_artifacts(tracker=tracker, cwd=tmp_path) == []


def test_collect_image_artifacts_from_recent_artifacts_dir(tmp_path: Path) -> None:
    started_at = 1000.0
    image = tmp_path / "artifacts" / "dashboard.png"
    image.parent.mkdir()
    image.write_bytes(b"png")
    old = tmp_path / "artifacts" / "old.png"
    old.write_bytes(b"old")
    import os

    os.utime(image, (started_at + 1, started_at + 1))
    os.utime(old, (started_at - 10, started_at - 10))
    tracker = ProgressTracker(engine="claude")

    artifacts = collect_image_artifacts(tracker=tracker, cwd=tmp_path, since=started_at)

    assert [artifact.rel_path for artifact in artifacts] == ["artifacts/dashboard.png"]


@pytest.mark.anyio
async def test_send_image_artifacts_sends_photo_for_png(tmp_path: Path) -> None:
    image = tmp_path / "cat.png"
    image.write_bytes(b"png")
    tracker = _tracker_with_file_change("cat.png")
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine="claude")
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=TransportRuntime(router=_make_router(runner), projects=_empty_projects()),
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=FakeTransport(),
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
    )
    incoming = IncomingMessage(channel_id=123, message_id=10, text="make image")

    count = await send_image_artifacts(cfg, incoming=incoming, tracker=tracker, cwd=tmp_path)

    assert count == 1
    assert bot.photo_calls[0]["filename"] == "cat.png"
    assert bot.photo_calls[0]["content"] == b"png"
    assert bot.photo_calls[0]["reply_to_message_id"] == 10
    assert bot.photo_calls[0]["caption"] == "cat.png"
    assert bot.photo_calls[0]["wait"] is False
    assert bot.document_calls == []


@pytest.mark.anyio
async def test_send_image_artifacts_sends_photo_album_for_multiple_pngs(tmp_path: Path) -> None:
    artifacts_dir = tmp_path / "artifacts"
    artifacts_dir.mkdir()
    first = artifacts_dir / "one.png"
    second = artifacts_dir / "two.jpg"
    first.write_bytes(b"one")
    second.write_bytes(b"two")
    tracker = ProgressTracker(engine="claude")
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine="claude")
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=TransportRuntime(router=_make_router(runner), projects=_empty_projects()),
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=FakeTransport(),
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
    )
    incoming = IncomingMessage(channel_id=123, message_id=10, text="make images")

    count = await send_image_artifacts(cfg, incoming=incoming, tracker=tracker, cwd=tmp_path, since=None)

    assert count == 2
    assert bot.media_group_calls[0]["reply_to_message_id"] == 10
    assert bot.media_group_calls[0]["wait"] is False
    assert [item["type"] for item in bot.media_group_calls[0]["media"]] == ["photo", "photo"]
    assert set(bot.media_group_calls[0]["files"]) == {"photo0", "photo1"}
    assert bot.photo_calls == []
    assert bot.document_calls == []
