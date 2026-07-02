from __future__ import annotations

from pathlib import Path

import msgspec

from ..logging import get_logger
from ..context import RunContext
from ..model import ResumeToken
from .state_store import JsonStateStore

logger = get_logger(__name__)

STATE_VERSION = 1
STATE_FILENAME = "telegram_chat_sessions_state.json"


class _SessionState(msgspec.Struct, forbid_unknown_fields=False):
    resume: str
    project: str | None = None
    branch: str | None = None


class _ChatState(msgspec.Struct, forbid_unknown_fields=False):
    sessions: dict[str, _SessionState] = msgspec.field(default_factory=dict)


class _ChatSessionsState(msgspec.Struct, forbid_unknown_fields=False):
    version: int
    cwd: str | None = None
    chats: dict[str, _ChatState] = msgspec.field(default_factory=dict)


def resolve_sessions_path(config_path: Path) -> Path:
    return config_path.with_name(STATE_FILENAME)


def _chat_key(chat_id: int, owner_id: int | None) -> str:
    owner = "chat" if owner_id is None else str(owner_id)
    return f"{chat_id}:{owner}"


def _new_state() -> _ChatSessionsState:
    return _ChatSessionsState(version=STATE_VERSION, chats={})


class ChatSessionStore(JsonStateStore[_ChatSessionsState]):
    def __init__(self, path: Path) -> None:
        super().__init__(
            path,
            version=STATE_VERSION,
            state_type=_ChatSessionsState,
            state_factory=_new_state,
            log_prefix="telegram.chat_sessions",
            logger=logger,
        )

    async def get_session_resume(
        self,
        chat_id: int,
        owner_id: int | None,
        engine: str,
        context: RunContext | None = None,
    ) -> ResumeToken | None:
        async with self._lock:
            self._reload_locked_if_needed()
            chat = self._get_chat_locked(chat_id, owner_id)
            if chat is None:
                return None
            entry = chat.sessions.get(engine)
            if entry is None or not entry.resume:
                return None
            if not _session_context_matches(entry, context):
                return None
            return ResumeToken(engine=engine, value=entry.resume)

    async def sync_startup_cwd(self, cwd: Path) -> bool:
        normalized = str(cwd.expanduser().resolve())
        async with self._lock:
            self._reload_locked_if_needed()
            previous = self._state.cwd
            cleared = False
            if previous is not None and previous != normalized:
                self._state.chats = {}
                cleared = True
            if previous != normalized:
                self._state.cwd = normalized
                self._save_locked()
            return cleared

    async def set_session_resume(
        self,
        chat_id: int,
        owner_id: int | None,
        token: ResumeToken,
        context: RunContext | None = None,
    ) -> None:
        async with self._lock:
            self._reload_locked_if_needed()
            if self._state.cwd is None:
                self._state.cwd = str(Path.cwd().expanduser().resolve())
            chat = self._ensure_chat_locked(chat_id, owner_id)
            chat.sessions[token.engine] = _SessionState(
                resume=token.value,
                project=context.project if context is not None else None,
                branch=context.branch if context is not None else None,
            )
            self._save_locked()

    async def clear_sessions(self, chat_id: int, owner_id: int | None) -> None:
        async with self._lock:
            self._reload_locked_if_needed()
            chat = self._get_chat_locked(chat_id, owner_id)
            if chat is None:
                return
            chat.sessions = {}
            self._save_locked()

    def _get_chat_locked(self, chat_id: int, owner_id: int | None) -> _ChatState | None:
        return self._state.chats.get(_chat_key(chat_id, owner_id))

    def _ensure_chat_locked(self, chat_id: int, owner_id: int | None) -> _ChatState:
        key = _chat_key(chat_id, owner_id)
        entry = self._state.chats.get(key)
        if entry is not None:
            return entry
        entry = _ChatState()
        self._state.chats[key] = entry
        return entry


def _session_context_matches(
    entry: _SessionState, context: RunContext | None
) -> bool:
    if context is None or context.project is None:
        return True
    if entry.project != context.project:
        return False
    return context.branch is None or entry.branch == context.branch
