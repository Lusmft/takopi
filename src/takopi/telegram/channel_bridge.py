from __future__ import annotations

import asyncio
import json
from contextlib import suppress
from dataclasses import asdict
from typing import Any

import httpx

from ..context import RunContext
from ..logging import get_logger
from ..transport import MessageRef, RenderedMessage, SendOptions
from .bridge import TelegramBridgeConfig, send_plain

logger = get_logger(__name__)


def _context_payload(context: RunContext | None) -> dict[str, Any] | None:
    if context is None:
        return None
    try:
        return asdict(context)
    except TypeError:
        return {
            "project": getattr(context, "project", None),
            "branch": getattr(context, "branch", None),
        }


async def forward_to_channel(
    cfg: TelegramBridgeConfig,
    *,
    chat_id: int,
    user_msg_id: int,
    text: str,
    context: RunContext | None,
    thread_id: int | None,
    engine: str | None,
) -> bool:
    bridge = cfg.channel_bridge
    headers: dict[str, str] = {}
    if bridge.shared_secret:
        headers["authorization"] = f"Bearer {bridge.shared_secret}"
    payload = {
        "chat_id": str(chat_id),
        "message_id": str(user_msg_id),
        "thread_id": str(thread_id) if thread_id is not None else "",
        "text": text,
        "context": _context_payload(context),
        "project": getattr(context, "project", "") if context is not None else "",
        "branch": getattr(context, "branch", "") if context is not None else "",
        "engine": engine or "claude",
    }
    try:
        async with httpx.AsyncClient(timeout=10.0) as client:
            resp = await client.post(bridge.inbound_url, json=payload, headers=headers)
            resp.raise_for_status()
    except httpx.HTTPError as exc:
        logger.warning("telegram.channel_bridge.forward_failed", error=str(exc))
        await send_plain(
            cfg.exec_cfg.transport,
            chat_id=chat_id,
            user_msg_id=user_msg_id,
            text=f"channel bridge unavailable: {exc}",
            thread_id=thread_id,
        )
        return False
    if bridge.send_progress:
        await send_plain(
            cfg.exec_cfg.transport,
            chat_id=chat_id,
            user_msg_id=user_msg_id,
            text="forwarded to Claude Code channel",
            notify=False,
            thread_id=thread_id,
        )
    return True


async def run_reply_server(cfg: TelegramBridgeConfig) -> None:
    bridge = cfg.channel_bridge
    if not bridge.enabled:
        return

    async def handle(reader: asyncio.StreamReader, writer: asyncio.StreamWriter) -> None:
        try:
            request_line = await reader.readline()
            if not request_line:
                return
            try:
                method, path, _version = request_line.decode("latin1").strip().split(" ", 2)
            except ValueError:
                await _write_response(writer, 400, b"bad request")
                return
            headers: dict[str, str] = {}
            while True:
                line = await reader.readline()
                if line in (b"\r\n", b"\n", b""):
                    break
                name, _, value = line.decode("latin1").partition(":")
                headers[name.strip().lower()] = value.strip()
            length = int(headers.get("content-length", "0") or "0")
            body = await reader.readexactly(length) if length else b"{}"
            if method != "POST" or path != "/reply":
                await _write_response(writer, 404, b"not found")
                return
            if bridge.shared_secret:
                expected = f"Bearer {bridge.shared_secret}"
                if headers.get("authorization") != expected:
                    await _write_response(writer, 401, b"unauthorized")
                    return
            data = json.loads(body.decode("utf-8"))
            chat_id = int(data["chat_id"])
            text = str(data["text"])
            reply_to_raw = data.get("reply_to_message_id") or data.get("message_id")
            reply_to_message_id = int(reply_to_raw) if reply_to_raw else None
            thread_raw = data.get("thread_id")
            thread_id = int(thread_raw) if thread_raw not in (None, "") else None
            reply_to = (
                MessageRef(channel_id=chat_id, message_id=reply_to_message_id)
                if reply_to_message_id is not None
                else None
            )
            await cfg.exec_cfg.transport.send(
                channel_id=chat_id,
                message=RenderedMessage(text=text),
                options=SendOptions(reply_to=reply_to, thread_id=thread_id),
            )
            await _write_response(writer, 200, b"ok")
        except (OSError, ValueError, json.JSONDecodeError, KeyError) as exc:
            logger.exception("telegram.channel_bridge.reply_failed", error=str(exc))
            with suppress(OSError):
                await _write_response(writer, 500, str(exc).encode())
        finally:
            writer.close()
            await writer.wait_closed()

    server = await asyncio.start_server(handle, bridge.reply_host, bridge.reply_port)
    addrs = ", ".join(str(sock.getsockname()) for sock in server.sockets or [])
    logger.info("telegram.channel_bridge.reply_server_started", bind=addrs)
    async with server:
        await server.serve_forever()


async def _write_response(writer: asyncio.StreamWriter, status: int, body: bytes) -> None:
    reason = {200: "OK", 400: "Bad Request", 401: "Unauthorized", 404: "Not Found", 500: "Internal Server Error"}.get(status, "OK")
    writer.write(
        f"HTTP/1.1 {status} {reason}\r\ncontent-length: {len(body)}\r\nconnection: close\r\n\r\n".encode("latin1") + body
    )
    await writer.drain()
