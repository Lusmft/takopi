from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
import re
from typing import Literal

AttachmentKind = Literal["image", "document", "video", "audio"]
AttachmentSource = Literal["telegram"]


@dataclass(frozen=True, slots=True)
class PromptAttachment:
    kind: AttachmentKind
    path: Path
    mime_type: str | None
    source: AttachmentSource = "telegram"
    caption: str | None = None


def _image_instruction(user_prompt: str, image_count: int) -> str:
    text = user_prompt.strip().lower()
    if image_count > 1 and re.search(r"\b(compare|difference|diff|versus|vs)\b", text):
        return "Compare the attached images and use them as primary context for the request."
    if re.search(r"\b(read|ocr|text|screenshot|screen|error|ui|interface)\b", text):
        return "Inspect the attached images carefully, including any visible text or UI details, and use them as primary context for the request."
    if re.search(r"\b(describe|what|analyze|analyse|look|see)\b", text):
        return "Analyze the attached images and use them as primary context for the request."
    return "Use the attached files as primary context for the request when relevant."


def format_attachment_block(
    attachments: list[PromptAttachment] | tuple[PromptAttachment, ...],
    *,
    user_prompt: str = "",
) -> str:
    if not attachments:
        return ""
    image_paths = [item.path.as_posix() for item in attachments if item.kind == "image"]
    other_paths = [item.path.as_posix() for item in attachments if item.kind != "image"]
    sections: list[str] = []
    if image_paths:
        sections.append(
            "Attached images:\n" + "\n".join(f"- {path}" for path in image_paths)
        )
    if other_paths:
        sections.append(
            "Attached files:\n" + "\n".join(f"- {path}" for path in other_paths)
        )
    sections.append(_image_instruction(user_prompt, len(image_paths)))
    return "\n\n".join(section for section in sections if section)
