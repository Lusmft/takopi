from __future__ import annotations

import html
from dataclasses import dataclass


@dataclass(frozen=True, slots=True)
class ImageResult:
    filename: str
    content: bytes
    mime_type: str | None = None
    caption: str | None = None


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


async def generate_image(prompt: str, size: str = "1024x1024") -> ImageResult:
    """Generate an image for Telegram delivery.

    MVP backend: returns a deterministic SVG placeholder so the Telegram media
    pipeline can be exercised before a real provider is configured.
    """
    return ImageResult(
        filename="takopi-image-request.svg",
        content=_svg_placeholder(prompt, size),
        mime_type="image/svg+xml",
        caption="Готово: image backend stub. Подключи реальный provider следующим шагом.",
    )
