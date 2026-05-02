from __future__ import annotations

import base64
import html
import os
from dataclasses import dataclass
from typing import Literal, Protocol

from openai import AsyncOpenAI, OpenAIError

from ..logging import get_logger
from ..settings import TelegramImagesSettings

logger = get_logger(__name__)

ImageProvider = Literal["stub", "openai"]


@dataclass(frozen=True, slots=True)
class ImageResult:
    filename: str
    content: bytes
    mime_type: str | None = None
    caption: str | None = None


class ImageGenerationError(RuntimeError):
    pass


class ImageGenerator(Protocol):
    async def generate(self, *, prompt: str, settings: TelegramImagesSettings) -> ImageResult: ...


def _svg_placeholder(prompt: str, size: str) -> bytes:
    title = html.escape(prompt[:160] or "image request")
    subtitle = html.escape(f"Takopi image backend stub · {size}")
    svg = f"""<svg xmlns=\"http://www.w3.org/2000/svg\" width=\"1024\" height=\"1024\" viewBox=\"0 0 1024 1024\">
  <defs>
    <linearGradient id=\"g\" x1=\"0\" y1=\"0\" x2=\"1\" y2=\"1\">
      <stop offset=\"0%\" stop-color=\"#17142f\"/>
      <stop offset=\"50%\" stop-color=\"#2748b5\"/>
      <stop offset=\"100%\" stop-color=\"#10d6b2\"/>
    </linearGradient>
  </defs>
  <rect width=\"1024\" height=\"1024\" fill=\"url(#g)\"/>
  <circle cx=\"790\" cy=\"190\" r=\"150\" fill=\"#ffffff\" opacity=\"0.12\"/>
  <circle cx=\"220\" cy=\"800\" r=\"210\" fill=\"#000000\" opacity=\"0.18\"/>
  <rect x=\"96\" y=\"328\" width=\"832\" height=\"368\" rx=\"42\" fill=\"#050714\" opacity=\"0.72\"/>
  <text x=\"512\" y=\"455\" text-anchor=\"middle\" font-family=\"Arial, sans-serif\" font-size=\"38\" font-weight=\"700\" fill=\"#ffffff\">Image request captured</text>
  <foreignObject x=\"164\" y=\"500\" width=\"696\" height=\"110\">
    <div xmlns=\"http://www.w3.org/1999/xhtml\" style=\"font-family: Arial, sans-serif; font-size: 24px; color: #dfe8ff; text-align: center; line-height: 1.35;\">{title}</div>
  </foreignObject>
  <text x=\"512\" y=\"635\" text-anchor=\"middle\" font-family=\"Arial, sans-serif\" font-size=\"20\" fill=\"#94fff0\">{subtitle}</text>
</svg>
"""
    return svg.encode("utf-8")


class StubImageGenerator:
    async def generate(self, *, prompt: str, settings: TelegramImagesSettings) -> ImageResult:
        return ImageResult(
            filename="takopi-image-request.svg",
            content=_svg_placeholder(prompt, settings.size),
            mime_type="image/svg+xml",
            caption="Готово: image backend stub. Подключи provider=openai для реальной генерации.",
        )


class OpenAIImageGenerator:
    async def generate(self, *, prompt: str, settings: TelegramImagesSettings) -> ImageResult:
        api_key = settings.api_key or os.environ.get("OPENAI_API_KEY")
        if not api_key:
            raise ImageGenerationError(
                "OpenAI image backend needs images.api_key or OPENAI_API_KEY."
            )
        try:
            async with AsyncOpenAI(
                base_url=settings.base_url,
                api_key=api_key,
                timeout=180,
            ) as client:
                response = await client.images.generate(
                    model=settings.model,
                    prompt=prompt,
                    size=settings.size,
                    quality=settings.quality,
                    n=1,
                    output_format=settings.output_format,
                )
        except TypeError:
            # Older OpenAI SDKs may not know output_format yet.
            async with AsyncOpenAI(
                base_url=settings.base_url,
                api_key=api_key,
                timeout=180,
            ) as client:
                response = await client.images.generate(
                    model=settings.model,
                    prompt=prompt,
                    size=settings.size,
                    quality=settings.quality,
                    n=1,
                )
        except OpenAIError as exc:
            logger.error(
                "openai.image.generate.error",
                error=str(exc),
                error_type=exc.__class__.__name__,
            )
            raise ImageGenerationError(str(exc).strip() or "OpenAI image generation failed") from exc

        if not response.data:
            raise ImageGenerationError("OpenAI image generation returned no image.")
        b64_json = response.data[0].b64_json
        if not b64_json:
            raise ImageGenerationError("OpenAI image generation returned no image bytes.")
        try:
            content = base64.b64decode(b64_json)
        except ValueError as exc:
            raise ImageGenerationError("OpenAI image generation returned invalid image bytes.") from exc

        ext = settings.output_format
        return ImageResult(
            filename=f"takopi-image.{ext}",
            content=content,
            mime_type=f"image/{'jpeg' if ext == 'jpeg' else ext}",
            caption="Готово.",
        )


def _generator_for(provider: ImageProvider) -> ImageGenerator:
    if provider == "openai":
        return OpenAIImageGenerator()
    return StubImageGenerator()


async def generate_image(
    prompt: str,
    settings: TelegramImagesSettings,
    *,
    generator: ImageGenerator | None = None,
) -> ImageResult:
    """Generate an image for Telegram delivery."""
    if not settings.enabled:
        raise ImageGenerationError("image generation is disabled in Telegram config.")
    if generator is None:
        generator = _generator_for(settings.provider)
    return await generator.generate(prompt=prompt, settings=settings)
