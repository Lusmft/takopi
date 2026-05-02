from __future__ import annotations

from collections.abc import Awaitable, Callable
from typing import Any

from .image_backend import ImageGenerationError, ImageGenerator, generate_image
from .image_intent import build_basic_image_prompt, is_image_intent
from .types import TelegramIncomingMessage


async def handle_image_request(
    cfg: Any,
    msg: TelegramIncomingMessage,
    text: str,
    reply: Callable[..., Awaitable[None]],
    *,
    generator: ImageGenerator | None = None,
) -> bool:
    if not is_image_intent(text):
        return False

    settings = cfg.images
    if not settings.enabled:
        return False

    try:
        built = build_basic_image_prompt(text)
        settings = settings.model_copy(update={"size": built.size})
        result = await generate_image(built.prompt, settings, generator=generator)
    except ImageGenerationError as exc:
        await reply(text=f"не смог сгенерировать картинку: {exc}")
        return True
    except Exception as exc:  # noqa: BLE001
        await reply(text=f"не смог сгенерировать картинку: {exc}")
        return True

    sent = await cfg.bot.send_document(
        chat_id=msg.chat_id,
        filename=result.filename,
        content=result.content,
        reply_to_message_id=msg.message_id,
        message_thread_id=msg.thread_id,
        caption=result.caption,
    )
    if sent is None:
        await reply(text="не смог отправить картинку.")
    return True
