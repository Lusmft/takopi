from __future__ import annotations

import pytest

from takopi.markdown import MarkdownPresenter
from takopi.runner_bridge import ExecBridgeConfig
from takopi.runners.mock import Return, ScriptRunner
from takopi.settings import TelegramImagesSettings
from takopi.telegram.bridge import TelegramBridgeConfig
from takopi.telegram.image_backend import ImageResult
from takopi.telegram.image_flow import handle_image_request
from takopi.telegram.image_intent import build_basic_image_prompt, is_image_intent
from takopi.telegram.types import TelegramIncomingMessage
from takopi.transport_runtime import TransportRuntime
from tests.telegram_fakes import FakeBot, FakeTransport, _empty_projects, _make_router


class _FakeImageGenerator:
    async def generate(self, *, prompt: str, settings: TelegramImagesSettings) -> ImageResult:
        return ImageResult(
            filename="custom.png",
            content=f"PNG:{prompt}:{settings.size}".encode(),
            mime_type="image/png",
            caption="done",
        )


class _ReplyRecorder:
    def __init__(self) -> None:
        self.calls: list[dict] = []

    async def __call__(self, **kwargs) -> None:
        self.calls.append(kwargs)


def _make_cfg(bot: FakeBot, *, images: TelegramImagesSettings | None = None) -> TelegramBridgeConfig:
    runner = ScriptRunner([Return(answer="ok")], engine="codex")
    return TelegramBridgeConfig(
        bot=bot,
        runtime=TransportRuntime(router=_make_router(runner), projects=_empty_projects()),
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=FakeTransport(),
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        images=images or TelegramImagesSettings(provider="stub"),
    )


def test_is_image_intent_detects_russian_and_english() -> None:
    assert is_image_intent("нарисуй космического кота")
    assert is_image_intent("please generate image of a dashboard")
    assert not is_image_intent("объясни этот traceback")


def test_build_basic_image_prompt_strips_prefix() -> None:
    built = build_basic_image_prompt("нарисуй: неоновый баннер")

    assert built.prompt == "неоновый баннер"
    assert built.size == "1024x1024"


@pytest.mark.anyio
async def test_handle_image_request_sends_document() -> None:
    bot = FakeBot()
    cfg = _make_cfg(bot)
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="нарисуй неоновый баннер",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=99,
    )
    reply = _ReplyRecorder()

    handled = await handle_image_request(
        cfg,
        msg,
        msg.text,
        reply,
        generator=_FakeImageGenerator(),
    )

    assert handled is True
    assert reply.calls == []
    assert len(bot.document_calls) == 1
    call = bot.document_calls[0]
    assert call["chat_id"] == 123
    assert call["reply_to_message_id"] == 10
    assert call["filename"] == "custom.png"
    assert call["content"] == "PNG:неоновый баннер:1024x1024".encode()
    assert call["caption"] == "done"


@pytest.mark.anyio
async def test_handle_image_request_ignores_normal_text() -> None:
    bot = FakeBot()
    cfg = _make_cfg(bot)
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="объясни traceback",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=99,
    )
    reply = _ReplyRecorder()

    handled = await handle_image_request(cfg, msg, msg.text, reply)

    assert handled is False
    assert bot.document_calls == []
    assert reply.calls == []


@pytest.mark.anyio
async def test_handle_image_request_ignores_when_disabled() -> None:
    bot = FakeBot()
    cfg = _make_cfg(bot, images=TelegramImagesSettings(enabled=False))
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="нарисуй кота",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=99,
    )
    reply = _ReplyRecorder()

    handled = await handle_image_request(cfg, msg, msg.text, reply)

    assert handled is False
    assert bot.document_calls == []
    assert reply.calls == []
