from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path
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


def format_attachment_block(
    attachments: list[PromptAttachment] | tuple[PromptAttachment, ...],
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
    sections.append("Use the attached files as primary context for the request when relevant.")
    return "\n\n".join(section for section in sections if section)
