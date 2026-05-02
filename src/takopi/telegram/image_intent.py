from __future__ import annotations

from dataclasses import dataclass

_IMAGE_TRIGGERS = (
    "нарисуй",
    "сгенерируй картинку",
    "сделай картинку",
    "сделай баннер",
    "создай обложку",
    "generate image",
    "draw",
    "make a banner",
    "create cover",
)

_IMAGE_PREFIXES = (
    "нарисуй",
    "сгенерируй картинку",
    "сделай картинку",
    "сделай баннер",
    "создай обложку",
    "generate image",
    "draw",
    "make a banner",
    "create cover",
)


@dataclass(frozen=True, slots=True)
class ImagePrompt:
    prompt: str
    size: str = "1024x1024"


def is_image_intent(text: str) -> bool:
    normalized = (text or "").strip().lower()
    if not normalized:
        return False
    return any(trigger in normalized for trigger in _IMAGE_TRIGGERS)


def build_basic_image_prompt(text: str) -> ImagePrompt:
    cleaned = (text or "").strip()
    lowered = cleaned.lower()
    for prefix in _IMAGE_PREFIXES:
        if lowered.startswith(prefix):
            cleaned = cleaned[len(prefix) :].strip(" :,-—")
            break
    if not cleaned:
        cleaned = "minimalist abstract illustration"
    return ImagePrompt(prompt=cleaned)
