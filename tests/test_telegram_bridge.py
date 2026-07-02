import asyncio
import textwrap
from dataclasses import replace
from pathlib import Path
from types import SimpleNamespace
from typing import Any, cast

import anyio
import pytest

from takopi import commands, plugins
from takopi.telegram.commands.executor import _CaptureTransport, _run_engine
from takopi.telegram.commands.file_transfer import _handle_file_get, _handle_file_put
from takopi.telegram.commands.model import _handle_model_command
from takopi.telegram.commands.reasoning import _handle_reasoning_command
from takopi.telegram.commands.topics import _handle_topic_command
import takopi.telegram.loop as telegram_loop
import takopi.telegram.topics as telegram_topics
from takopi.directives import parse_directives
from takopi.telegram.api_models import Chat, File, ForumTopic, Message, Update, User
from takopi.settings import (
    TelegramChannelBridgeProjectSettings,
    TelegramChannelBridgeSettings,
    TelegramFilesSettings,
    TelegramTopicsSettings,
)
from takopi.telegram.bridge import (
    TelegramBridgeConfig,
    TelegramPresenter,
    TelegramTransport,
    build_bot_commands,
    handle_callback_cancel,
    handle_cancel,
    is_cancel_command,
    run_main_loop,
    send_with_resume,
)
from takopi.telegram.client import BotClient
from takopi.telegram.render import MAX_BODY_CHARS, prepare_telegram
import takopi.telegram.channel_bridge as telegram_channel_bridge
from takopi.telegram.topic_state import TopicStateStore, resolve_state_path
from takopi.telegram.chat_sessions import ChatSessionStore, resolve_sessions_path
from takopi.telegram.chat_prefs import ChatPrefsStore, resolve_prefs_path
from takopi.telegram.engine_overrides import EngineOverrides
from takopi.context import RunContext
from takopi.config import ProjectConfig, ProjectsConfig
from takopi.runner_bridge import ExecBridgeConfig, RunningTask
from takopi.runner import RunnerTurnControl
from takopi.markdown import MarkdownParts, MarkdownPresenter
from takopi.model import ResumeToken
from takopi.progress import ProgressTracker
from takopi.router import AutoRouter, RunnerEntry
from takopi.scheduler import ThreadScheduler
from takopi.transport_runtime import TransportRuntime
from takopi.runners.mock import Return, ScriptRunner, Sleep, Wait
from takopi.telegram.types import (
    TelegramCallbackQuery,
    TelegramDocument,
    TelegramIncomingMessage,
    TelegramVoice,
)
from takopi.transport import MessageRef, RenderedMessage, SendOptions
from tests.plugin_fixtures import FakeEntryPoint, install_entrypoints
from tests.telegram_fakes import (
    FakeBot,
    FakeTransport,
    _empty_projects,
    make_cfg,
    _make_router,
)

CODEX_ENGINE = "codex"
FAST_FORWARD_COALESCE_S = 0.0
FAST_MEDIA_GROUP_DEBOUNCE_S = 0.0
BATCH_MEDIA_GROUP_DEBOUNCE_S = 0.05
DEBOUNCE_FORWARD_COALESCE_S = 0.05


async def _noop_reply_server(_cfg: TelegramBridgeConfig) -> None:
    return None


class _NoopTaskGroup:
    def start_soon(self, func, *args: Any) -> None:
        _ = func, args
        return None


@pytest.mark.anyio
async def test_scheduler_keeps_busy_queued_job_addressable_by_progress() -> None:
    resume = ResumeToken(engine=CODEX_ENGINE, value="sid")
    active_done = anyio.Event()
    ran = anyio.Event()
    progress_ref = MessageRef(channel_id=123, message_id=55)

    async def _run_job(_) -> None:
        ran.set()

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        await scheduler.note_thread_known(resume, active_done)
        await scheduler.enqueue_resume(
            chat_id=123,
            user_msg_id=10,
            text="queued prompt",
            resume_token=resume,
            progress_ref=progress_ref,
        )

        await anyio.sleep(0)

        queued = await scheduler.get_queued(123, progress_ref.message_id)
        assert queued is not None
        assert queued.text == "queued prompt"
        assert ran.is_set() is False

        active_done.set()
        with anyio.fail_after(1):
            await ran.wait()
        assert await scheduler.get_queued(123, progress_ref.message_id) is None

        tg.cancel_scope.cancel()


def test_parse_directives_inline_engine() -> None:
    directives = parse_directives(
        "/claude do it",
        engine_ids=("codex", "claude"),
        projects=_empty_projects(),
    )
    assert directives.engine == "claude"
    assert directives.prompt == "do it"


def test_parse_directives_newline() -> None:
    directives = parse_directives(
        "/codex\nhello",
        engine_ids=("codex", "claude"),
        projects=_empty_projects(),
    )
    assert directives.engine == "codex"
    assert directives.prompt == "hello"


def test_parse_directives_ignores_unknown() -> None:
    directives = parse_directives(
        "/unknown hi",
        engine_ids=("codex", "claude"),
        projects=_empty_projects(),
    )
    assert directives.engine is None
    assert directives.prompt == "/unknown hi"


def test_parse_directives_bot_suffix() -> None:
    directives = parse_directives(
        "/claude@bunny_agent_bot hi",
        engine_ids=("claude",),
        projects=_empty_projects(),
    )
    assert directives.engine == "claude"
    assert directives.prompt == "hi"


def test_parse_directives_only_first_non_empty_line() -> None:
    directives = parse_directives(
        "hello\n/claude hi",
        engine_ids=("codex", "claude"),
        projects=_empty_projects(),
    )
    assert directives.engine is None
    assert directives.prompt == "hello\n/claude hi"


def test_build_bot_commands_includes_cancel_and_engine() -> None:
    runner = ScriptRunner(
        [Return(answer="ok")], engine=CODEX_ENGINE, resume_value="sid"
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )
    commands = build_bot_commands(runtime)

    assert {"command": "cancel", "description": "cancel run"} in commands
    assert {"command": "file", "description": "upload or fetch files"} in commands
    assert {"command": "new", "description": "start a new thread"} in commands
    assert {"command": "compact", "description": "reset the current thread"} in commands
    assert {"command": "ctx", "description": "show or update context"} in commands
    assert {"command": "usage", "description": "show usage info"} in commands
    assert {"command": "status", "description": "show Claude Code status"} in commands
    assert {"command": "stats", "description": "show Claude Code stats"} in commands
    assert {"command": "bridge_status", "description": "show Takopi channel status"} in commands
    assert {"command": "verbose", "description": "toggle action updates"} in commands
    assert {"command": "agent", "description": "set default engine"} in commands
    assert {"command": "model", "description": "choose Claude Code model"} in commands
    assert any(cmd["command"] == "codex" for cmd in commands)


def test_build_bot_commands_includes_projects() -> None:
    runner = ScriptRunner(
        [Return(answer="ok")], engine=CODEX_ENGINE, resume_value="sid"
    )
    router = _make_router(runner)
    projects = ProjectsConfig(
        projects={
            "good": ProjectConfig(
                alias="good",
                path=Path("."),
                worktrees_dir=Path(".worktrees"),
            ),
            "bad-name": ProjectConfig(
                alias="bad-name",
                path=Path("."),
                worktrees_dir=Path(".worktrees"),
            ),
        },
        default_project=None,
    )

    runtime = TransportRuntime(router=router, projects=projects)
    commands = build_bot_commands(runtime)

    assert any(cmd["command"] == "good" for cmd in commands)
    assert not any(cmd["command"] == "bad-name" for cmd in commands)


def test_build_bot_commands_includes_topics_when_enabled() -> None:
    runner = ScriptRunner(
        [Return(answer="ok")], engine=CODEX_ENGINE, resume_value="sid"
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )

    commands = build_bot_commands(runtime, include_topics=True)

    assert {"command": "topic", "description": "create or bind a topic"} in commands
    assert {"command": "ctx", "description": "show or update context"} in commands


def test_build_bot_commands_includes_command_plugins(monkeypatch) -> None:
    class _Command:
        id = "pingcmd"
        description = "ping command"

        async def handle(self, ctx):
            _ = ctx
            return None

    entrypoints = [
        FakeEntryPoint(
            "pingcmd",
            "takopi.commands.ping:BACKEND",
            plugins.COMMAND_GROUP,
            loader=_Command,
        )
    ]
    install_entrypoints(monkeypatch, entrypoints)
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )

    commands_list = build_bot_commands(runtime)

    assert {"command": "pingcmd", "description": "ping command"} in commands_list


def test_build_bot_commands_caps_total() -> None:
    runner = ScriptRunner(
        [Return(answer="ok")], engine=CODEX_ENGINE, resume_value="sid"
    )
    router = _make_router(runner)
    projects = ProjectsConfig(
        projects={
            f"proj{i}": ProjectConfig(
                alias=f"proj{i}",
                path=Path("."),
                worktrees_dir=Path(".worktrees"),
            )
            for i in range(150)
        },
        default_project=None,
    )

    runtime = TransportRuntime(router=router, projects=projects)
    commands = build_bot_commands(runtime)

    assert len(commands) == 100
    assert any(cmd["command"] == "codex" for cmd in commands)
    assert any(cmd["command"] == "cancel" for cmd in commands)


def test_telegram_presenter_progress_shows_cancel_button() -> None:
    presenter = TelegramPresenter()
    state = ProgressTracker(engine="codex").snapshot()

    rendered = presenter.render_progress(state, elapsed_s=0.0)

    reply_markup = rendered.extra["reply_markup"]
    assert reply_markup["inline_keyboard"][0][0]["text"] == "cancel"
    assert reply_markup["inline_keyboard"][0][0]["callback_data"] == "takopi:cancel"


def test_telegram_presenter_clears_button_on_cancelled() -> None:
    presenter = TelegramPresenter()
    state = ProgressTracker(engine="codex").snapshot()

    rendered = presenter.render_progress(state, elapsed_s=0.0, label="cancelled")

    assert rendered.extra["reply_markup"]["inline_keyboard"] == []


def test_telegram_presenter_clears_button_on_steered() -> None:
    presenter = TelegramPresenter()
    state = ProgressTracker(engine="codex").snapshot()

    rendered = presenter.render_progress(state, elapsed_s=0.0, label="steered")

    assert rendered.extra["reply_markup"]["inline_keyboard"] == []


def test_telegram_presenter_progress_shows_steer_button_for_queued() -> None:
    presenter = TelegramPresenter()
    state = ProgressTracker(engine="codex").snapshot()

    rendered = presenter.render_progress(state, elapsed_s=0.0, label="queued")

    row = rendered.extra["reply_markup"]["inline_keyboard"][0]
    assert row == [
        {"text": "steer", "callback_data": "takopi:steer"},
        {"text": "cancel", "callback_data": "takopi:cancel"},
    ]


@pytest.mark.anyio
async def test_send_queued_progress_omits_steer_when_not_steerable() -> None:
    transport = FakeTransport()
    cfg = replace(
        make_cfg(transport),
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=TelegramPresenter(),
            final_notify=True,
        ),
    )

    await telegram_loop._send_queued_progress(
        cfg,
        chat_id=123,
        user_msg_id=10,
        thread_id=None,
        resume_token=ResumeToken(engine=CODEX_ENGINE, value="sid"),
        context=None,
        steerable=False,
    )

    assert transport.send_calls
    message = transport.send_calls[0]["message"]
    assert message.text.lower().startswith("starting")
    assert message.extra["reply_markup"]["inline_keyboard"] == [
        [{"text": "cancel", "callback_data": "takopi:cancel"}]
    ]


def test_telegram_presenter_final_clears_button() -> None:
    presenter = TelegramPresenter()
    state = ProgressTracker(engine="codex").snapshot()

    rendered = presenter.render_final(state, elapsed_s=0.0, status="done", answer="ok")

    assert rendered.extra["reply_markup"]["inline_keyboard"] == []


def test_channel_bridge_reply_render_splits_followups() -> None:
    rendered = telegram_channel_bridge._render_channel_reply("x" * (MAX_BODY_CHARS + 50))

    assert rendered.text
    followups = rendered.extra.get("followups")
    assert followups
    assert all(isinstance(item, RenderedMessage) for item in followups)


def test_latest_takopi_segment_uses_last_marker() -> None:
    pane = "old\n← takopi: first\nfoo\n← takopi: second\nbar\n❯"

    assert telegram_channel_bridge._latest_takopi_segment(pane).startswith("← takopi: second")


def test_extract_live_progress_text_filters_prompt_chrome() -> None:
    pane = """← takopi: привет

● Bash(pwd)
  ⎿ /root/usegateway

✻ Worked for 3s

❯ prompt
gh auth login
""" 

    text = telegram_channel_bridge._extract_live_progress_text(pane)

    assert "· Bash: `pwd`" in text
    assert "Worked for 3s" in text
    assert "gh auth login" not in text


def test_extract_live_progress_text_formats_tool_calls() -> None:
    pane = '''← takopi: Сделай гит пулл

● Bash(echo "=== git pull ===" && git pull 2>&1 | head -40 && echo "=== status ===" && git status -s)
  ⎿  Waiting…

● Bash(echo "=== recent commit dates ===" && git log -10 --format="%h %ci %s")
  ⎿  === recent commit dates ===

✻ Newspapering… (10s · ↑ 399 tokens)

❯
'''

    text = telegram_channel_bridge._extract_live_progress_text(pane)

    assert "· Bash: git pull" in text
    assert "↳ waiting for permission" in text
    assert "· Bash: recent commit dates" in text
    assert "↳ === recent commit dates ===" in text
    assert "↻ Newspapering" in text


def test_extract_live_progress_text_hides_permission_overlay() -> None:
    pane = '''← takopi: в каком состоянии сейчас проект?

● Bash(echo "=== HEAD vs latest QA tag RC-v1.23.13-11 ===" && git rev-list --left-right --count RC-v1.23.13-11...HEAD 2>&1 && echo "(left=in
      QA tag not in HEAD, right=…)
  ⎿  Waiting…

────────────────────────────────────────────────────────────────────────────────
 Bash command

   echo "=== HEAD vs latest QA tag RC-v1.23.13-11 ===" && git rev-list --left-right --count RC-v1.23.13-11...HEAD 2>&1
   Compare HEAD with QA release tag

 Do you want to proceed?
 ❯ 1. Yes
   2. Yes, and don’t ask again for: git rev-list *
   3. No

 Esc to cancel · Tab to amend · ctrl+e to explain
'''

    text = telegram_channel_bridge._extract_live_progress_text(pane)

    assert "· Bash: HEAD vs latest QA tag RC-v1.23.13-11" in text
    assert "↳ waiting for permission" in text
    assert "Bash command" not in text
    assert "Do you want to proceed" not in text
    assert "Compare HEAD with QA release tag" not in text
    assert "QA tag not in HEAD" not in text


def test_extract_live_progress_text_formats_mcp_permission_overlay() -> None:
    pane = '''← takopi: привет, что это за проект?

  Listed 1 directory (ctrl+o to expand)

● Calling takopi… (ctrl+o to expand)

────────────────────────────────────────────────────────────────────────────────
 Tool use

   takopi - reply(chat_id: "8081295168", reply_to_message_id: "1708", text:
   "Привет! Это **FastAPI Admin Pro** — админ-панель.\\n\\n**Стек:**\\n-
   FastAPI + Tortoise ORM\\n- UI на Tabler\\n\\nЧто хочешь с ним сделать?") (MCP)
   Send a text reply back to the Telegram chat through Takopi.

 Do you want to proceed?
 ❯ 1. Yes
   2. Yes, and don't ask again for takopi - reply commands in
      /root/projects/fastapi-admin-pro
   3. No

 Esc to cancel · Tab to amend
'''

    text = telegram_channel_bridge._extract_live_progress_text(pane)

    assert "· MCP: takopi.reply" in text
    assert "↳ waiting for permission" in text
    assert "FastAPI Admin Pro" not in text
    assert "Do you want to proceed" not in text
    assert "1. Yes" not in text


def test_extract_live_progress_text_filters_claude_feedback_prompt() -> None:
    pane = """← takopi: Сделай гит пулл

● git pull не сработал автоматически — у ветки нет upstream.

  Calling takopi… (ctrl+o to expand)

· Composing… (25s · ↑ 863 tokens)

● How is Claude doing this session? (optional)
  1: Bad    2: Fine   3: Good   0: Dismiss

────────────────────────────────────────────────────────────────────────────────
❯
"""

    text = telegram_channel_bridge._extract_live_progress_text(pane)

    assert "git pull не сработал" in text
    assert "Calling takopi" not in text
    assert "Composing" not in text
    assert "How is Claude doing" not in text
    assert "1: Bad" not in text


def test_extract_live_progress_text_filters_tips_and_ctrl_o_chrome() -> None:
    pane = """← takopi: статус

● Bash(git status -sb)
  ⎿  ## release/1_23_13
     ?? artifacts/
     … +3 lines (ctrl+o to expand)

✻ Coalescing… (9s · ↑ 469 tokens)
  ⎿  Tip: Use ctrl+v to paste images from your clipboard

❯
"""

    text = telegram_channel_bridge._extract_live_progress_text(pane)

    assert "… +3 lines" in text
    assert "ctrl+o to expand" not in text
    assert "Tip:" not in text
    assert "ctrl+v" not in text


def test_channel_bridge_status_text_mentions_tmux(monkeypatch) -> None:
    cfg = SimpleNamespace(
        channel_bridge=SimpleNamespace(
            enabled=True,
            inbound_url="http://127.0.0.1:8788/push",
            reply_host="127.0.0.1",
            reply_port=8789,
            live_progress=True,
            tmux_session="takopi_channel_usegateway",
        )
    )
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_capture",
        lambda session: "← takopi: request\n● ok",
    )

    text = telegram_channel_bridge.channel_bridge_status_text(cfg)

    assert "tmux: takopi_channel_usegateway" in text
    assert "visible state: ok" in text


def test_capture_channel_slash_command_returns_overlay(monkeypatch) -> None:
    sent: list[tuple[str, str]] = []
    escaped: list[str] = []
    pane = textwrap.dedent(
        """
        ❯ /usage
        Settings  Status   Config   Usage   Stats
        Current session
        Total cost: $0.42
        """
    )

    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_send_slash_command",
        lambda session, command: sent.append((session, command)) or True,
    )
    monkeypatch.setattr(telegram_channel_bridge, "_tmux_capture", lambda session: pane)
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_send_escape",
        lambda session: escaped.append(session),
    )

    text = telegram_channel_bridge._capture_channel_slash_command("sess", "/usage")

    assert sent == [("sess", "/usage")]
    assert escaped == ["sess"]
    assert "Claude Code /usage:" in text
    assert "· Total cost: $0.42" in text
    assert "Settings  Status" not in text


def test_format_usage_overlay_for_telegram_wraps_sections() -> None:
    raw = textwrap.dedent(
        """
        Settings  Status   Config   Usage   Stats
        Session
        Total cost:            $2.99
        Total duration (API):  2m 20s
        Usage by model:
             claude-opus-4-7:  6.7k input, 3.7k output, 2.6m cache read, 247.6k
        cache write ($2.99)
        Current session
        █████                                              10% used
        █ 6% used
        Resets 2pm (UTC)
        Extra usage
        Extra usage not enabled · /extra-usage to enable
        Rese s Jun 16, 9pm (UTC)
        Refresh1:59pm (UTC)
        8:59pm (UTC)
        """
    )

    text = telegram_channel_bridge._format_usage_overlay_for_telegram(raw)

    assert "Settings  Status" not in text
    assert "Session  \n· Total cost: $2.99" in text
    assert "· Total duration (API): 2m 20s" in text
    assert "Usage by model  \n· claude-opus-4-7:" in text
    assert "cache write ($2.99)" in text
    assert "Current session  \n· █████ 10% used  \n· Resets 2pm (UTC)" in text
    assert "Extra usage  \n· Extra usage not enabled" in text
    assert "█ 6% used" not in text
    assert "Rese s" not in text
    assert "Refresh1" not in text
    assert "8:59pm" not in text


def test_format_usage_overlay_keeps_bullets_after_telegram_render() -> None:
    body = telegram_channel_bridge._format_usage_overlay_for_telegram(
        "Session\nTotal cost: $2.99"
    )

    rendered, _entities = prepare_telegram(MarkdownParts(header=body))

    assert "· Total cost: $2.99" in rendered
    assert "- Total cost: $2.99" not in rendered


def test_format_status_overlay_for_telegram_wraps_fields() -> None:
    raw = textwrap.dedent(
        """
        Settings  Status   Config   Usage   Stats
        Version:          2.1.173
        Session name:     /rename to add a name
        Model:            Default (Opus 4.8 with 1M context · Best for everyday,
                          complex tasks)
        MCP servers:      1 connected, 1 failed · /mcp
        Esc to cancel
        """
    )

    text = telegram_channel_bridge._format_status_overlay_for_telegram(raw)

    assert "Settings  Status" not in text
    assert "· Version: 2.1.173" in text
    assert "· Session name: /rename to add a name" in text
    assert "· Model: Default (Opus 4.8 with 1M context" in text
    assert "complex tasks)" in text
    assert "· MCP servers: 1 connected, 1 failed · /mcp" in text


def test_format_stats_overlay_for_telegram_splits_metrics() -> None:
    raw = textwrap.dedent(
        """
        Settings  Status   Config   Usage   Stats
           Overview   Models

          Mon ·······▒▒▓█
              Less ░ ▒ ▓ █ More
          All time · Last 7 days · Last 30 days

          Favorite model: Opus 4.8        Total tokens: 4.1m
          Sessions: 376                   Longest session: 28d 2h 31m
          Active days: 31/31              Longest streak: 31 days
          Most active day: May 14         Current streak: 31 days
          You've used ~14x more tokens than Crime and Punishment
            ↓ stats · r to cycle dates · ctrl+s to copy
        """
    )

    text = telegram_channel_bridge._format_stats_overlay_for_telegram(raw)

    assert "Settings  Status" not in text
    assert text.startswith("```\n")
    assert "Overview Models" in text
    assert "Mon ·······▒▒▓█" in text
    assert "Less ░ ▒ ▓ █ More" in text
    assert "· Favorite model: Opus 4.8" in text
    assert "· Total tokens: 4.1m" in text
    assert "· Sessions: 376" in text
    assert "· Longest session: 28d 2h 31m" in text
    assert "· Current streak: 31 days" in text
    assert "ctrl+s" not in text

    rendered, entities = prepare_telegram(MarkdownParts(header=text))
    assert "Mon ·······▒▒▓█" in rendered
    assert "Last 7 days · Last 30 days\n\n· Favorite model" in rendered
    assert any(entity.get("type") == "pre" for entity in entities)


def test_format_model_overlay_for_telegram_lists_options() -> None:
    raw = textwrap.dedent(
        """
        Select model
        Switch between Claude models. Applies to this session and future Claude Code
        sessions. For other/previous model names, specify with --model.

        ❯ 1. Default (recommended) ✔  Opus 4.7 with 1M context · Most capable for
                                      complex work
          2. Sonnet                   Sonnet 4.6 · Best for everyday tasks
          3. Haiku                    Haiku 4.5 · Fastest for quick answers
          4. Fable                    Fable 5 · Most capable

        ◉ xHigh effort (default) ←/→ to adjust
        Enter to confirm · Esc to cancel
        """
    )

    text = telegram_channel_bridge._format_model_overlay_for_telegram(raw)

    assert "Claude Code model:" in text
    assert "Current: Opus 4.7 with 1M context" in text
    assert "Available models:" in text
    assert "· 1. Default (recommended)" in text
    assert "current" in text
    assert "· 2. Sonnet" in text
    assert "Effort: xHigh effort" in text
    assert "Use `/model 1`, `/model 2`, `/model 3`, `/model 4`." in text


def test_capture_channel_model_command_confirms_current_model(monkeypatch) -> None:
    raw_before = textwrap.dedent(
        """
        Select model
        ❯ 1. Default (recommended) ✔  Opus 4.8 with 1M context
          2. Fable                    Fable 5
          3. Sonnet                   Sonnet 4.6
          4. Haiku                    Haiku 4.5
        """
    )
    sent_keys: list[tuple[str, ...]] = []

    monkeypatch.setattr(telegram_channel_bridge, "_tmux_send_slash_command", lambda *_: True)
    monkeypatch.setattr(telegram_channel_bridge, "_wait_for_model_overlay", lambda *_: raw_before)
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_send_keys_slow",
        lambda _session, *keys: sent_keys.append(keys) or True,
    )
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_capture",
        lambda _session: "❯ /model\n  ⎿  Set model to Fable 5 for this session",
    )
    monkeypatch.setattr(telegram_channel_bridge, "_tmux_send_escape", lambda *_: None)

    text = telegram_channel_bridge._capture_channel_model_command("sess", "2")

    assert sent_keys == [("Down", "s")]
    assert "Set model to Fable 5 for this session" in text
    assert "selected option 2" not in text


def test_capture_channel_model_command_confirms_switch_dialog(monkeypatch) -> None:
    raw_before = textwrap.dedent(
        """
        Select model
        ❯ 1. Default (recommended) ✔  Opus 4.8 with 1M context
          2. Opus                     Opus 4.8 with 1M context
          3. Fable                    Fable 5
          4. Sonnet                   Sonnet 4.6
          5. Haiku                    Haiku 4.5
        """
    )
    captures = iter(
        [
            textwrap.dedent(
                """
                ❯ /model
                Switch model?
                ❯ 1. Yes, switch to Fable 5
                  2. No, go back
                """
            ),
            "❯ /model\n  ⎿  Set model to Fable 5 for this session",
        ]
    )
    sent_keys: list[tuple[str, ...]] = []

    monkeypatch.setattr(telegram_channel_bridge, "_tmux_send_slash_command", lambda *_: True)
    monkeypatch.setattr(telegram_channel_bridge, "_wait_for_model_overlay", lambda *_: raw_before)
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_send_keys_slow",
        lambda _session, *keys, **_kwargs: sent_keys.append(keys) or True,
    )
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_capture",
        lambda _session: next(captures),
    )
    monkeypatch.setattr(telegram_channel_bridge, "_tmux_send_escape", lambda *_: None)

    text = telegram_channel_bridge._capture_channel_model_command("sess", "3")

    assert sent_keys == [("Down", "Down", "s"), ("Enter",)]
    assert "Set model to Fable 5 for this session" in text


def test_capture_channel_model_command_moves_from_current_cursor(monkeypatch) -> None:
    raw_before = textwrap.dedent(
        """
        Select model
          1. Default (recommended)    Opus 4.8 with 1M context
          2. Fable                    Fable 5
        ❯ 3. Sonnet                ✔  Sonnet 4.6
          4. Haiku                    Haiku 4.5
        """
    )
    sent_keys: list[tuple[str, ...]] = []

    monkeypatch.setattr(telegram_channel_bridge, "_tmux_send_slash_command", lambda *_: True)
    monkeypatch.setattr(telegram_channel_bridge, "_wait_for_model_overlay", lambda *_: raw_before)
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_send_keys_slow",
        lambda _session, *keys: sent_keys.append(keys) or True,
    )
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_capture",
        lambda _session: "❯ /model\n  ⎿  Set model to Opus 4.8 for this session",
    )
    monkeypatch.setattr(telegram_channel_bridge, "_tmux_send_escape", lambda *_: None)

    text = telegram_channel_bridge._capture_channel_model_command("sess", "1")

    assert sent_keys == [("Up", "Up", "s")]
    assert "Set model to Opus 4.8 for this session" in text


@pytest.mark.anyio
async def test_verbose_actions_send_new_lines_only() -> None:
    transport = FakeTransport()
    cfg = SimpleNamespace(
        exec_cfg=SimpleNamespace(transport=transport),
    )
    run = telegram_channel_bridge.LiveProgressRun(
        progress_ref=MessageRef(channel_id=123, message_id=10),
        started_at=0,
        chat_id=123,
        user_msg_id=1,
        thread_id=None,
        verbose=True,
    )

    await telegram_channel_bridge._maybe_send_verbose_actions(
        cfg,
        run=run,
        text="Read src/app.py\nRead src/app.py\nBash(pytest)",
    )
    await telegram_channel_bridge._maybe_send_verbose_actions(
        cfg,
        run=run,
        text="Read src/app.py\nEdit src/app.py",
    )

    messages = [call["message"].text for call in transport.send_calls]
    assert len(messages) == 3
    assert any("Read src/app.py" in message for message in messages)
    assert any("Bash(pytest)" in message for message in messages)
    assert any("Edit src/app.py" in message for message in messages)


@pytest.mark.anyio
async def test_channel_bridge_slash_command_requires_tmux_session() -> None:
    cfg = SimpleNamespace(
        channel_bridge=SimpleNamespace(
            enabled=True,
            tmux_session=None,
        )
    )

    text = await telegram_channel_bridge.channel_bridge_slash_command_text(cfg, "/usage")

    assert text == "Claude Code tmux session is not configured."


def test_channel_route_uses_project_specific_endpoint() -> None:
    cfg = SimpleNamespace(
        channel_bridge=TelegramChannelBridgeSettings(
            inbound_url="http://127.0.0.1:8788/push",
            tmux_session="takopi_channel_usegateway",
            projects={
                "project": TelegramChannelBridgeProjectSettings(
                    inbound_url="http://127.0.0.1:8791/push",
                    tmux_session="takopi_channel_project",
                )
            },
        )
    )

    route = telegram_channel_bridge._channel_route(
        cfg,
        RunContext(project="project"),
    )

    assert route.project == "project"
    assert route.inbound_url == "http://127.0.0.1:8791/push"
    assert route.tmux_session == "takopi_channel_project"


def test_channel_route_falls_back_to_default_endpoint() -> None:
    cfg = SimpleNamespace(
        channel_bridge=TelegramChannelBridgeSettings(
            inbound_url="http://127.0.0.1:8788/push",
            tmux_session="takopi_channel_usegateway",
            projects={
                "project": TelegramChannelBridgeProjectSettings(
                    inbound_url="http://127.0.0.1:8791/push",
                    tmux_session="takopi_channel_project",
                )
            },
        )
    )

    route = telegram_channel_bridge._channel_route(
        cfg,
        RunContext(project="usegateway"),
    )

    assert route.project == "usegateway"
    assert route.inbound_url == "http://127.0.0.1:8788/push"
    assert route.tmux_session == "takopi_channel_usegateway"


def test_live_progress_choice_requires_permission_prompt(monkeypatch):
    telegram_channel_bridge._LIVE_PROGRESS_RUNS.clear()
    telegram_channel_bridge._LIVE_PROGRESS_BY_PROGRESS.clear()
    run = telegram_channel_bridge.LiveProgressRun(
        progress_ref=telegram_channel_bridge.MessageRef(channel_id=123, message_id=10),
        started_at=0,
    )
    telegram_channel_bridge._LIVE_PROGRESS_RUNS[(123, 1)] = run
    telegram_channel_bridge._LIVE_PROGRESS_BY_PROGRESS[(123, 10)] = (123, 1)
    cfg = SimpleNamespace(channel_bridge=SimpleNamespace(tmux_session="sess"))
    sent: list[tuple[str, str]] = []
    replies: list[str] = []

    monkeypatch.setattr(telegram_channel_bridge, "_tmux_capture", lambda session: "← takopi: working")
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_send_choice",
        lambda session, choice: sent.append((session, choice)) or True,
    )

    async def reply(**kwargs):
        replies.append(kwargs["text"])

    handled = asyncio.run(
        telegram_channel_bridge.handle_live_progress_choice(
            cfg,
            chat_id=123,
            reply_to_message_id=10,
            text="2",
            reply=reply,
        )
    )

    assert handled is True
    assert sent == []
    assert replies == ["no visible Claude permission prompt to answer."]


def test_live_progress_choice_sends_tmux_choice(monkeypatch):
    telegram_channel_bridge._LIVE_PROGRESS_RUNS.clear()
    telegram_channel_bridge._LIVE_PROGRESS_BY_PROGRESS.clear()
    run = telegram_channel_bridge.LiveProgressRun(
        progress_ref=telegram_channel_bridge.MessageRef(channel_id=123, message_id=10),
        started_at=0,
    )
    telegram_channel_bridge._LIVE_PROGRESS_RUNS[(123, 1)] = run
    telegram_channel_bridge._LIVE_PROGRESS_BY_PROGRESS[(123, 10)] = (123, 1)
    cfg = SimpleNamespace(channel_bridge=SimpleNamespace(tmux_session="sess"))
    replies: list[str] = []
    sent: list[tuple[str, str]] = []
    pane = textwrap.dedent(
        """
        ← takopi: prompt
        Read file
        Do you want to proceed?
        1. Yes
        2. Yes, allow reading from uploads/ during this session
        3. No
        """
    )

    monkeypatch.setattr(telegram_channel_bridge, "_tmux_capture", lambda session: pane)
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_send_choice",
        lambda session, choice: sent.append((session, choice)) or True,
    )

    async def reply(**kwargs):
        replies.append(kwargs["text"])

    handled = asyncio.run(
        telegram_channel_bridge.handle_live_progress_choice(
            cfg,
            chat_id=123,
            reply_to_message_id=10,
            text="2",
            reply=reply,
        )
    )

    assert handled is True
    assert sent == [("sess", "2")]
    assert replies == ["sent permission choice `2` to Claude."]


def test_render_live_progress_hides_permission_buttons_by_default() -> None:
    rendered = telegram_channel_bridge._render_live_progress(
        text="· Bash: Read file\n  ↳ waiting for permission",
        elapsed_s=3,
        status="working",
        engine="claude",
    )

    assert rendered.extra["reply_markup"]["inline_keyboard"] == []


def test_render_live_progress_adds_permission_buttons_when_ready() -> None:
    rendered = telegram_channel_bridge._render_live_progress(
        text="· Bash: Read file\n  ↳ waiting for permission",
        elapsed_s=3,
        status="working",
        engine="claude",
        permission_controls=True,
    )

    keyboard = rendered.extra["reply_markup"]["inline_keyboard"]
    assert [button["text"] for button in keyboard[0]] == ["Yes", "Always", "No"]
    assert [button["callback_data"] for button in keyboard[0]] == [
        "takopi:perm:1",
        "takopi:perm:2",
        "takopi:perm:3",
    ]


def test_render_live_progress_preserves_action_line_breaks() -> None:
    rendered = telegram_channel_bridge._render_live_progress(
        text=(
            "· Bash: git status\n"
            '  ↳ On branch release/1_23_13 Untracked files: (use "git add <file>..." to include)\n'
            '· Bash: echo "---vs main---"; git log --oneline main..HEAD\n'
            "  ↳ ---vs main--- 218 commits ahead of main\n"
            "↻ Metamorphosing…"
        ),
        elapsed_s=14,
        status="working",
        engine="claude",
    )

    assert "· Bash: git status\n↳ On branch release/1_23_13" in rendered.text
    assert 'include)\n· Bash: echo "---vs main---"; git log' in rendered.text
    assert "main..HEAD\n↳ ---vs main--- 218 commits ahead of main" in rendered.text
    assert "main\n↻ Metamorphosing" in rendered.text


def test_live_progress_callback_sends_tmux_choice(monkeypatch):
    telegram_channel_bridge._LIVE_PROGRESS_RUNS.clear()
    telegram_channel_bridge._LIVE_PROGRESS_BY_PROGRESS.clear()
    run = telegram_channel_bridge.LiveProgressRun(
        progress_ref=telegram_channel_bridge.MessageRef(channel_id=123, message_id=10),
        started_at=0,
    )
    telegram_channel_bridge._LIVE_PROGRESS_RUNS[(123, 1)] = run
    telegram_channel_bridge._LIVE_PROGRESS_BY_PROGRESS[(123, 10)] = (123, 1)
    sent: list[tuple[str, str]] = []
    answers: list[tuple[str, str | None]] = []
    pane = textwrap.dedent(
        """
        ← takopi: prompt
        Do you want to proceed?
        1. Yes
        2. Yes, allow reading from uploads/ during this session
        3. No
        """
    )

    async def answer_callback_query(callback_query_id, text=None, **_kwargs):
        answers.append((callback_query_id, text))
        return True

    cfg = SimpleNamespace(
        channel_bridge=SimpleNamespace(tmux_session="sess"),
        bot=SimpleNamespace(answer_callback_query=answer_callback_query),
    )
    query = SimpleNamespace(
        data="takopi:perm:2",
        chat_id=123,
        message_id=10,
        callback_query_id="cb1",
    )

    monkeypatch.setattr(telegram_channel_bridge, "_tmux_capture", lambda session: pane)
    monkeypatch.setattr(
        telegram_channel_bridge,
        "_tmux_send_choice",
        lambda session, choice: sent.append((session, choice)) or True,
    )

    handled = asyncio.run(telegram_channel_bridge.handle_live_progress_callback_choice(cfg, query))

    assert handled is True
    assert sent == [("sess", "2")]
    assert answers == [("cb1", "sent permission choice 2 to Claude.")]


def test_permission_controls_ready_debounces_transient_prompt() -> None:
    run = telegram_channel_bridge.LiveProgressRun(
        progress_ref=telegram_channel_bridge.MessageRef(channel_id=123, message_id=10),
        started_at=0,
    )
    pane = "Do you want to proceed?\n1. Yes\n2. Yes, allow reading\n3. No"
    text = "· Bash: Read file\n  ↳ waiting for permission"

    assert not telegram_channel_bridge._permission_controls_ready(run, pane=pane, text=text, now=10.0)
    assert not telegram_channel_bridge._permission_controls_ready(run, pane=pane, text=text, now=11.0)
    assert telegram_channel_bridge._permission_controls_ready(run, pane=pane, text=text, now=12.1)
    assert not telegram_channel_bridge._permission_controls_ready(run, pane="done", text="done", now=13.0)
    assert run.permission_seen_at is None


def test_register_live_progress_supersedes_previous_chat_run() -> None:
    telegram_channel_bridge._LIVE_PROGRESS_RUNS.clear()
    telegram_channel_bridge._LIVE_PROGRESS_BY_PROGRESS.clear()
    old_run = telegram_channel_bridge.LiveProgressRun(
        progress_ref=telegram_channel_bridge.MessageRef(channel_id=123, message_id=10),
        started_at=0,
    )
    new_run = telegram_channel_bridge.LiveProgressRun(
        progress_ref=telegram_channel_bridge.MessageRef(channel_id=123, message_id=11),
        started_at=1,
    )

    asyncio.run(telegram_channel_bridge._register_live_progress(123, 1, old_run))
    asyncio.run(telegram_channel_bridge._register_live_progress(123, 2, new_run))

    expected_runs = {(123, 2): new_run}
    expected_progress = {(123, 11): (123, 2)}

    assert old_run.superseded is True
    assert old_run.done.is_set()
    assert expected_runs == telegram_channel_bridge._LIVE_PROGRESS_RUNS
    assert expected_progress == telegram_channel_bridge._LIVE_PROGRESS_BY_PROGRESS


def test_telegram_presenter_split_overflow_adds_followups() -> None:
    presenter = TelegramPresenter(message_overflow="split")
    state = ProgressTracker(engine="codex").snapshot()

    rendered = presenter.render_final(
        state,
        elapsed_s=0.0,
        status="done",
        answer="x" * (MAX_BODY_CHARS + 10),
    )

    followups = rendered.extra.get("followups")
    assert followups
    assert all(isinstance(item, RenderedMessage) for item in followups)
    assert rendered.extra["reply_markup"]["inline_keyboard"] == []
    assert all(
        item.extra["reply_markup"]["inline_keyboard"] == [] for item in followups
    )


@pytest.mark.anyio
async def test_telegram_transport_passes_replace_and_wait() -> None:
    bot = FakeBot()
    transport = TelegramTransport(bot)
    reply = MessageRef(channel_id=123, message_id=10)
    replace = MessageRef(channel_id=123, message_id=11)

    await transport.send(
        channel_id=123,
        message=RenderedMessage(text="hello"),
        options=SendOptions(reply_to=reply, notify=True, replace=replace),
    )
    assert bot.send_calls
    assert bot.send_calls[0]["replace_message_id"] == 11

    await transport.edit(
        ref=replace,
        message=RenderedMessage(text="edit"),
        wait=False,
    )
    assert bot.edit_calls
    assert bot.edit_calls[0]["wait"] is False


@pytest.mark.anyio
async def test_telegram_transport_passes_reply_markup() -> None:
    bot = FakeBot()
    transport = TelegramTransport(bot)
    markup = {"inline_keyboard": []}

    await transport.send(
        channel_id=123,
        message=RenderedMessage(text="hello", extra={"reply_markup": markup}),
    )
    assert bot.send_calls
    assert bot.send_calls[0]["reply_markup"] == markup

    ref = MessageRef(channel_id=123, message_id=1)
    await transport.edit(
        ref=ref,
        message=RenderedMessage(text="edit", extra={"reply_markup": markup}),
    )
    assert bot.edit_calls
    assert bot.edit_calls[0]["reply_markup"] == markup


@pytest.mark.anyio
async def test_telegram_transport_sends_followups() -> None:
    bot = FakeBot()
    transport = TelegramTransport(bot)
    reply = MessageRef(channel_id=123, message_id=10)
    followup = RenderedMessage(text="part 2")

    await transport.send(
        channel_id=123,
        message=RenderedMessage(text="part 1", extra={"followups": [followup]}),
        options=SendOptions(reply_to=reply, notify=False, thread_id=7),
    )

    assert len(bot.send_calls) == 2
    assert bot.send_calls[1]["text"] == "part 2"
    assert bot.send_calls[1]["reply_to_message_id"] == 10
    assert bot.send_calls[1]["message_thread_id"] == 7
    assert bot.send_calls[1]["replace_message_id"] is None
    assert bot.send_calls[1]["disable_notification"] is True


@pytest.mark.anyio
async def test_telegram_transport_edits_and_sends_followups() -> None:
    bot = FakeBot()
    transport = TelegramTransport(bot)
    followup = RenderedMessage(text="part 2")

    await transport.edit(
        ref=MessageRef(channel_id=123, message_id=42),
        message=RenderedMessage(
            text="part 1",
            extra={
                "followups": [followup],
                "followup_reply_to_message_id": 10,
                "followup_thread_id": 7,
                "followup_notify": False,
            },
        ),
    )

    assert len(bot.edit_calls) == 1
    assert len(bot.send_calls) == 1
    assert bot.send_calls[0]["text"] == "part 2"
    assert bot.send_calls[0]["reply_to_message_id"] == 10
    assert bot.send_calls[0]["message_thread_id"] == 7
    assert bot.send_calls[0]["disable_notification"] is True


@pytest.mark.anyio
async def test_telegram_transport_edit_wait_false_returns_ref() -> None:
    class _OutboxBot(BotClient):
        def __init__(self) -> None:
            self.edit_calls: list[dict[str, Any]] = []

        async def get_updates(
            self,
            offset: int | None,
            timeout_s: int = 50,
            allowed_updates: list[str] | None = None,
        ) -> list[Update] | None:
            return None

        async def get_file(self, file_id: str) -> File | None:
            _ = file_id
            return None

        async def download_file(self, file_path: str) -> bytes | None:
            _ = file_path
            return None

        async def send_message(
            self,
            chat_id: int,
            text: str,
            reply_to_message_id: int | None = None,
            disable_notification: bool | None = False,
            message_thread_id: int | None = None,
            entities: list[dict[str, Any]] | None = None,
            parse_mode: str | None = None,
            reply_markup: dict | None = None,
            *,
            replace_message_id: int | None = None,
        ) -> Message | None:
            _ = reply_markup
            return None

        async def send_document(
            self,
            chat_id: int,
            filename: str,
            content: bytes,
            reply_to_message_id: int | None = None,
            message_thread_id: int | None = None,
            disable_notification: bool | None = False,
            caption: str | None = None,
        ) -> Message | None:
            _ = (
                chat_id,
                filename,
                content,
                reply_to_message_id,
                message_thread_id,
                disable_notification,
                caption,
            )
            return None

        async def edit_message_text(
            self,
            chat_id: int,
            message_id: int,
            text: str,
            entities: list[dict[str, Any]] | None = None,
            parse_mode: str | None = None,
            reply_markup: dict | None = None,
            *,
            wait: bool = True,
        ) -> Message | None:
            self.edit_calls.append(
                {
                    "chat_id": chat_id,
                    "message_id": message_id,
                    "text": text,
                    "entities": entities,
                    "parse_mode": parse_mode,
                    "reply_markup": reply_markup,
                    "wait": wait,
                }
            )
            if not wait:
                return None
            return Message(message_id=message_id, chat=Chat(id=chat_id, type="private"))

        async def delete_message(
            self,
            chat_id: int,
            message_id: int,
        ) -> bool:
            return False

        async def set_my_commands(
            self,
            commands: list[dict[str, Any]],
            *,
            scope: dict[str, Any] | None = None,
            language_code: str | None = None,
        ) -> bool:
            return False

        async def get_me(self) -> User | None:
            return None

        async def close(self) -> None:
            return None

        async def answer_callback_query(
            self,
            callback_query_id: str,
            text: str | None = None,
            show_alert: bool | None = None,
        ) -> bool:
            _ = callback_query_id, text, show_alert
            return True

    bot = _OutboxBot()
    transport = TelegramTransport(bot)
    ref = MessageRef(channel_id=123, message_id=1)

    result = await transport.edit(
        ref=ref,
        message=RenderedMessage(text="edit"),
        wait=False,
    )

    assert result == ref
    assert bot.edit_calls
    assert bot.edit_calls[0]["wait"] is False


@pytest.mark.anyio
async def test_handle_cancel_without_reply_prompts_user() -> None:
    transport = FakeTransport()
    cfg = replace(
        make_cfg(transport),
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=TelegramPresenter(),
            final_notify=True,
        ),
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/cancel",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=123,
    )
    running_tasks: dict = {}

    await handle_cancel(cfg, msg, running_tasks)

    assert len(transport.send_calls) == 1
    assert "reply to the progress message" in transport.send_calls[0]["message"].text


@pytest.mark.anyio
async def test_handle_cancel_with_no_progress_message_says_nothing_running() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/cancel",
        reply_to_message_id=None,
        reply_to_text="no message id",
        sender_id=123,
    )
    running_tasks: dict = {}

    await handle_cancel(cfg, msg, running_tasks)

    assert len(transport.send_calls) == 1
    assert "nothing is currently running" in transport.send_calls[0]["message"].text


@pytest.mark.anyio
async def test_handle_cancel_with_finished_task_says_nothing_running() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    progress_id = 99
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/cancel",
        reply_to_message_id=progress_id,
        reply_to_text=None,
        sender_id=123,
    )
    running_tasks: dict = {}

    await handle_cancel(cfg, msg, running_tasks)

    assert len(transport.send_calls) == 1
    assert "nothing is currently running" in transport.send_calls[0]["message"].text


@pytest.mark.anyio
async def test_handle_cancel_cancels_running_task() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    progress_id = 42
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/cancel",
        reply_to_message_id=progress_id,
        reply_to_text=None,
        sender_id=123,
    )

    running_task = RunningTask()
    running_tasks = {MessageRef(channel_id=123, message_id=progress_id): running_task}
    await handle_cancel(cfg, msg, running_tasks)

    assert running_task.cancel_requested.is_set() is True
    assert len(transport.send_calls) == 0  # No error message sent


@pytest.mark.anyio
async def test_handle_cancel_only_cancels_matching_progress_message() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    task_first = RunningTask()
    task_second = RunningTask()
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/cancel",
        reply_to_message_id=1,
        reply_to_text=None,
        sender_id=123,
    )
    running_tasks = {
        MessageRef(channel_id=123, message_id=1): task_first,
        MessageRef(channel_id=123, message_id=2): task_second,
    }

    await handle_cancel(cfg, msg, running_tasks)

    assert task_first.cancel_requested.is_set() is True
    assert task_second.cancel_requested.is_set() is False
    assert len(transport.send_calls) == 0


@pytest.mark.anyio
async def test_handle_cancel_cancels_queued_job() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)

    async def _noop_run_job(_) -> None:
        return None

    scheduler = ThreadScheduler(task_group=_NoopTaskGroup(), run_job=_noop_run_job)
    progress_id = 55
    progress_ref = MessageRef(channel_id=123, message_id=progress_id)
    resume = ResumeToken(engine=CODEX_ENGINE, value="sid")
    await scheduler.enqueue_resume(
        chat_id=123,
        user_msg_id=10,
        text="queued",
        resume_token=resume,
        progress_ref=progress_ref,
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/cancel",
        reply_to_message_id=progress_id,
        reply_to_text=None,
        sender_id=123,
    )

    await handle_cancel(cfg, msg, {}, scheduler)

    assert transport.edit_calls
    cancelled_text = transport.edit_calls[0]["message"].text.lower()
    assert "cancelled" in cancelled_text
    assert "codex resume sid" in cancelled_text
    assert await scheduler.cancel_queued(123, progress_ref.message_id) is None


@pytest.mark.anyio
async def test_handle_file_put_writes_file(tmp_path: Path) -> None:
    payload = b"hello"

    class _FileBot(FakeBot):
        async def get_file(self, file_id: str) -> File | None:
            _ = file_id
            return File(file_path="files/hello.txt")

        async def download_file(self, file_path: str) -> bytes | None:
            _ = file_path
            return payload

    transport = FakeTransport()
    bot = _FileBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
            )
        },
        default_project=None,
    )
    runtime = TransportRuntime(router=_make_router(runner), projects=projects)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        files=TelegramFilesSettings(enabled=True),
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=321,
        chat_type="private",
        document=TelegramDocument(
            file_id="doc-id",
            file_name="hello.txt",
            mime_type="text/plain",
            file_size=len(payload),
            raw={"file_id": "doc-id"},
        ),
    )

    await _handle_file_put(cfg, msg, "/proj uploads/hello.txt", None, None)

    target = tmp_path / "uploads" / "hello.txt"
    assert target.read_bytes() == payload
    assert transport.send_calls
    text = transport.send_calls[-1]["message"].text
    assert "saved uploads/hello.txt" in text
    assert "(5 b)" in text


@pytest.mark.anyio
async def test_handle_file_get_sends_document_for_allowed_user(
    tmp_path: Path,
) -> None:
    payload = b"fetch"
    target = tmp_path / "hello.txt"
    target.write_bytes(payload)

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
            )
        },
        default_project=None,
    )
    runtime = TransportRuntime(router=_make_router(runner), projects=projects)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        files=TelegramFilesSettings(
            enabled=True,
            allowed_user_ids=[42],
        ),
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=-100,
        message_id=10,
        text="",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=42,
        chat_type="supergroup",
    )

    await _handle_file_get(cfg, msg, "/proj hello.txt", None, None)

    assert bot.document_calls
    assert bot.document_calls[0]["filename"] == "hello.txt"
    assert bot.document_calls[0]["content"] == payload


@pytest.mark.anyio
async def test_handle_callback_cancel_cancels_running_task() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    progress_id = 42
    running_task = RunningTask()
    running_tasks = {MessageRef(channel_id=123, message_id=progress_id): running_task}
    query = TelegramCallbackQuery(
        transport="telegram",
        chat_id=123,
        message_id=progress_id,
        callback_query_id="cbq-1",
        data="takopi:cancel",
        sender_id=123,
    )

    await handle_callback_cancel(cfg, query, running_tasks)

    assert running_task.cancel_requested.is_set() is True
    assert len(transport.send_calls) == 0
    bot = cast(FakeBot, cfg.bot)
    assert bot.callback_calls
    assert bot.callback_calls[-1]["text"] == "cancelling..."


@pytest.mark.anyio
async def test_handle_callback_cancel_cancels_queued_job() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)

    async def _noop_run_job(_) -> None:
        return None

    scheduler = ThreadScheduler(task_group=_NoopTaskGroup(), run_job=_noop_run_job)
    progress_id = 77
    progress_ref = MessageRef(channel_id=123, message_id=progress_id)
    resume = ResumeToken(engine=CODEX_ENGINE, value="sid")
    await scheduler.enqueue_resume(
        chat_id=123,
        user_msg_id=10,
        text="queued",
        resume_token=resume,
        progress_ref=progress_ref,
    )
    query = TelegramCallbackQuery(
        transport="telegram",
        chat_id=123,
        message_id=progress_id,
        callback_query_id="cbq-queued",
        data="takopi:cancel",
        sender_id=123,
    )

    await handle_callback_cancel(cfg, query, {}, scheduler)

    assert transport.edit_calls
    cancelled_text = transport.edit_calls[0]["message"].text.lower()
    assert "cancelled" in cancelled_text
    assert "codex resume sid" in cancelled_text
    bot = cast(FakeBot, cfg.bot)
    assert bot.callback_calls
    assert bot.callback_calls[-1]["text"] == "dropped from queue."


@pytest.mark.anyio
async def test_handle_callback_steer_sends_queued_text_to_active_turn() -> None:
    transport = FakeTransport()
    cfg = replace(
        make_cfg(transport),
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=TelegramPresenter(),
            final_notify=True,
        ),
    )

    async def _noop_run_job(_) -> None:
        return None

    class _Control(RunnerTurnControl):
        def __init__(self) -> None:
            self.steered: list[str] = []

        async def steer(self, text: str) -> None:
            self.steered.append(text)

        async def interrupt(self) -> bool:
            return True

    scheduler = ThreadScheduler(task_group=_NoopTaskGroup(), run_job=_noop_run_job)
    progress_id = 88
    progress_ref = MessageRef(channel_id=123, message_id=progress_id)
    resume = ResumeToken(engine=CODEX_ENGINE, value="sid")
    await scheduler.enqueue_resume(
        chat_id=123,
        user_msg_id=10,
        text="queued prompt",
        resume_token=resume,
        progress_ref=progress_ref,
    )
    control = _Control()
    running_task = RunningTask(resume=resume, control=control)
    running_tasks = {MessageRef(channel_id=123, message_id=7): running_task}
    query = TelegramCallbackQuery(
        transport="telegram",
        chat_id=123,
        message_id=progress_id,
        callback_query_id="cbq-steer",
        data="takopi:steer",
        sender_id=123,
    )

    await telegram_loop.handle_callback_steer(cfg, query, running_tasks, scheduler)

    assert control.steered == ["queued prompt"]
    assert transport.edit_calls
    steered_text = transport.edit_calls[0]["message"].text.lower()
    assert "steered" in steered_text
    assert (
        transport.edit_calls[0]["message"].extra["reply_markup"]["inline_keyboard"]
        == []
    )
    assert await scheduler.cancel_queued(123, progress_ref.message_id) is None
    bot = cast(FakeBot, cfg.bot)
    assert bot.callback_calls
    assert bot.callback_calls[-1]["text"] == "steered active turn."


@pytest.mark.anyio
async def test_handle_callback_steer_claims_job_before_awaiting_steer() -> None:
    transport = FakeTransport()
    cfg = replace(
        make_cfg(transport),
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=TelegramPresenter(),
            final_notify=True,
        ),
    )
    active_done = anyio.Event()
    ran_jobs: list[str] = []

    async def _run_job(job) -> None:
        ran_jobs.append(job.text)

    class _Control(RunnerTurnControl):
        def __init__(self) -> None:
            self.steered: list[str] = []

        async def steer(self, text: str) -> None:
            self.steered.append(text)
            active_done.set()
            await anyio.sleep(0)

        async def interrupt(self) -> bool:
            return True

    async with anyio.create_task_group() as tg:
        scheduler = ThreadScheduler(task_group=tg, run_job=_run_job)
        progress_id = 89
        progress_ref = MessageRef(channel_id=123, message_id=progress_id)
        resume = ResumeToken(engine=CODEX_ENGINE, value="sid")
        await scheduler.note_thread_known(resume, active_done)
        await scheduler.enqueue_resume(
            chat_id=123,
            user_msg_id=10,
            text="queued prompt",
            resume_token=resume,
            progress_ref=progress_ref,
        )
        await anyio.sleep(0)

        control = _Control()
        running_task = RunningTask(resume=resume, control=control)
        running_tasks = {MessageRef(channel_id=123, message_id=7): running_task}
        query = TelegramCallbackQuery(
            transport="telegram",
            chat_id=123,
            message_id=progress_id,
            callback_query_id="cbq-steer-race",
            data="takopi:steer",
            sender_id=123,
        )

        await telegram_loop.handle_callback_steer(cfg, query, running_tasks, scheduler)
        await anyio.sleep(0)

        assert control.steered == ["queued prompt"]
        assert ran_jobs == []
        assert await scheduler.get_queued(123, progress_ref.message_id) is None
        tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_handle_callback_steer_requeues_when_steer_fails() -> None:
    transport = FakeTransport()
    cfg = replace(
        make_cfg(transport),
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=TelegramPresenter(),
            final_notify=True,
        ),
    )

    async def _noop_run_job(_) -> None:
        return None

    class _Control(RunnerTurnControl):
        async def steer(self, text: str) -> None:
            _ = text
            raise RuntimeError("nope")

        async def interrupt(self) -> bool:
            return True

    scheduler = ThreadScheduler(task_group=_NoopTaskGroup(), run_job=_noop_run_job)
    progress_id = 90
    progress_ref = MessageRef(channel_id=123, message_id=progress_id)
    resume = ResumeToken(engine=CODEX_ENGINE, value="sid")
    await scheduler.enqueue_resume(
        chat_id=123,
        user_msg_id=10,
        text="queued prompt",
        resume_token=resume,
        progress_ref=progress_ref,
    )
    running_task = RunningTask(resume=resume, control=_Control())
    query = TelegramCallbackQuery(
        transport="telegram",
        chat_id=123,
        message_id=progress_id,
        callback_query_id="cbq-steer-fails",
        data="takopi:steer",
        sender_id=123,
    )

    await telegram_loop.handle_callback_steer(
        cfg,
        query,
        {MessageRef(channel_id=123, message_id=7): running_task},
        scheduler,
    )

    queued = await scheduler.get_queued(123, progress_ref.message_id)
    assert queued is not None
    assert queued.text == "queued prompt"
    bot = cast(FakeBot, cfg.bot)
    assert bot.callback_calls[-1]["text"] == "could not steer; still queued."


@pytest.mark.anyio
async def test_handle_callback_cancel_without_task_acknowledges() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    query = TelegramCallbackQuery(
        transport="telegram",
        chat_id=123,
        message_id=99,
        callback_query_id="cbq-2",
        data="takopi:cancel",
        sender_id=123,
    )

    await handle_callback_cancel(cfg, query, {})

    assert len(transport.send_calls) == 0
    bot = cast(FakeBot, cfg.bot)
    assert bot.callback_calls
    assert "nothing is currently running" in bot.callback_calls[-1]["text"].lower()


def test_allowed_chat_ids_include_allowed_user_ids() -> None:
    cfg = replace(make_cfg(FakeTransport()), allowed_user_ids=(42,))
    allowed = telegram_loop._allowed_chat_ids(cfg)
    assert cfg.chat_id in allowed
    assert 42 in allowed


@pytest.mark.anyio
async def test_run_main_loop_ignores_disallowed_sender() -> None:
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    cfg = replace(make_cfg(FakeTransport(), runner), allowed_user_ids=(999,))

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert runner.calls == []


@pytest.mark.anyio
async def test_run_main_loop_ignores_disallowed_callback() -> None:
    cfg = replace(make_cfg(FakeTransport()), allowed_user_ids=(999,))
    bot = cast(FakeBot, cfg.bot)

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramCallbackQuery(
            transport="telegram",
            chat_id=123,
            message_id=42,
            callback_query_id="cbq-ignored",
            data="takopi:cancel",
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert bot.callback_calls == []


@pytest.mark.anyio
async def test_run_main_loop_allows_allowed_sender() -> None:
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    cfg = replace(make_cfg(FakeTransport(), runner), allowed_user_ids=(123,))

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert runner.calls
    assert runner.calls[0][0] == "hello"


def test_cancel_command_accepts_extra_text() -> None:
    assert is_cancel_command("/cancel now") is True
    assert is_cancel_command("/cancel@takopi please") is True
    assert is_cancel_command("/cancelled") is False


def test_resolve_message_accepts_backticked_ctx_line() -> None:
    runtime = TransportRuntime(
        router=_make_router(ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)),
        projects=ProjectsConfig(
            projects={
                "takopi": ProjectConfig(
                    alias="takopi",
                    path=Path("."),
                    worktrees_dir=Path(".worktrees"),
                )
            },
            default_project=None,
        ),
    )
    resolved = runtime.resolve_message(
        text="do it",
        reply_text="`ctx: takopi @feat/api`",
    )

    assert resolved.prompt == "do it"
    assert resolved.resume_token is None
    assert resolved.engine_override is None
    assert resolved.context == RunContext(project="takopi", branch="feat/api")


def test_is_forwarded_detects_forward_fields() -> None:
    assert telegram_loop._is_forwarded({"forward_origin": {"type": "user"}})
    assert telegram_loop._is_forwarded({"forward_from": {"id": 1}})
    assert telegram_loop._is_forwarded({"forward_from_chat": {"id": 1}})
    assert telegram_loop._is_forwarded({"forward_from_message_id": 2})
    assert telegram_loop._is_forwarded({"forward_sender_name": "anon"})
    assert telegram_loop._is_forwarded({"forward_signature": "sig"})
    assert telegram_loop._is_forwarded({"forward_date": 123})
    assert telegram_loop._is_forwarded({"is_automatic_forward": True})
    assert not telegram_loop._is_forwarded({"text": "hello"})
    assert not telegram_loop._is_forwarded(None)


def test_topic_title_matches_command_syntax() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)

    title = telegram_topics._topic_title(
        runtime=cfg.runtime,
        context=RunContext(project="takopi", branch="master"),
    )

    assert title == "takopi @master"

    title = telegram_topics._topic_title(
        runtime=cfg.runtime,
        context=RunContext(project="takopi", branch=None),
    )

    assert title == "takopi"

    title = telegram_topics._topic_title(
        runtime=cfg.runtime,
        context=RunContext(project=None, branch="main"),
    )

    assert title == "@main"


def test_topic_title_projects_scope_includes_project() -> None:
    transport = FakeTransport()
    cfg = replace(
        make_cfg(transport),
        topics=TelegramTopicsSettings(
            enabled=True,
            scope="projects",
        ),
    )

    title = telegram_topics._topic_title(
        runtime=cfg.runtime,
        context=RunContext(project="takopi", branch="master"),
    )

    assert title == "takopi @master"


@pytest.mark.anyio
async def test_maybe_rename_topic_updates_title(tmp_path: Path) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    store = TopicStateStore(tmp_path / "telegram_topics_state.json")

    await store.set_context(
        123,
        77,
        RunContext(project="takopi", branch="old"),
        topic_title="takopi @old",
    )

    await telegram_topics._maybe_rename_topic(
        cfg,
        store,
        chat_id=123,
        thread_id=77,
        context=RunContext(project="takopi", branch="new"),
    )

    bot = cast(FakeBot, cfg.bot)
    assert bot.edit_topic_calls
    assert bot.edit_topic_calls[-1]["name"] == "takopi @new"
    snapshot = await store.get_thread(123, 77)
    assert snapshot is not None
    assert snapshot.topic_title == "takopi @new"


@pytest.mark.anyio
async def test_maybe_rename_topic_skips_when_title_matches(tmp_path: Path) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    store = TopicStateStore(tmp_path / "telegram_topics_state.json")

    await store.set_context(
        123,
        77,
        RunContext(project="takopi", branch="main"),
        topic_title="takopi @main",
    )
    snapshot = await store.get_thread(123, 77)

    await telegram_topics._maybe_rename_topic(
        cfg,
        store,
        chat_id=123,
        thread_id=77,
        context=RunContext(project="takopi", branch="main"),
        snapshot=snapshot,
    )

    bot = cast(FakeBot, cfg.bot)
    assert bot.edit_topic_calls == []


@pytest.mark.anyio
async def test_topic_command_recreates_stale_topic(tmp_path: Path) -> None:
    class _StaleTopicBot(FakeBot):
        def __init__(self) -> None:
            super().__init__()
            self.create_topic_calls: list[dict[str, Any]] = []

        async def create_forum_topic(
            self, chat_id: int, name: str
        ) -> ForumTopic | None:
            self.create_topic_calls.append({"chat_id": chat_id, "name": name})
            return ForumTopic(message_thread_id=55)

        async def edit_forum_topic(
            self, chat_id: int, message_thread_id: int, name: str
        ) -> bool:
            self.edit_topic_calls.append(
                {
                    "chat_id": chat_id,
                    "message_thread_id": message_thread_id,
                    "name": name,
                }
            )
            return False

    transport = FakeTransport()
    bot = _StaleTopicBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    projects = ProjectsConfig(
        projects={
            "takopi": ProjectConfig(
                alias="takopi",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
            )
        },
        default_project=None,
    )
    runtime = TransportRuntime(router=_make_router(runner), projects=projects)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        topics=TelegramTopicsSettings(enabled=True, scope="main"),
    )
    store = TopicStateStore(tmp_path / "telegram_topics_state.json")
    await store.set_context(
        123,
        77,
        RunContext(project="takopi", branch="master"),
        topic_title="takopi @master",
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/topic takopi @master",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=123,
    )

    await _handle_topic_command(
        cfg,
        msg,
        "takopi @master",
        store,
        resolved_scope="main",
        scope_chat_ids=frozenset({123}),
    )

    assert bot.edit_topic_calls
    assert bot.create_topic_calls
    assert await store.get_thread(123, 77) is None
    snapshot = await store.get_thread(123, 55)
    assert snapshot is not None
    assert snapshot.context == RunContext(project="takopi", branch="master")


@pytest.mark.anyio
async def test_model_command_show_reports_overrides(tmp_path: Path) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    cfg = replace(cfg, topics=TelegramTopicsSettings(enabled=True, scope="main"))
    chat_prefs = ChatPrefsStore(tmp_path / "telegram_chat_prefs_state.json")
    topic_store = TopicStateStore(tmp_path / "telegram_topics_state.json")
    await chat_prefs.set_engine_override(
        123,
        CODEX_ENGINE,
        EngineOverrides(model="gpt-4.1-mini", reasoning=None),
    )
    await topic_store.set_engine_override(
        123,
        77,
        CODEX_ENGINE,
        EngineOverrides(model="gpt-4.1", reasoning=None),
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/model",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=123,
        thread_id=77,
    )

    await _handle_model_command(
        cfg,
        msg,
        "",
        ambient_context=None,
        topic_store=topic_store,
        chat_prefs=chat_prefs,
        resolved_scope="main",
        scope_chat_ids=frozenset({123}),
    )

    text = transport.send_calls[-1]["message"].text
    assert "engine: codex (global default)" in text
    assert "model: gpt-4.1 (topic override)" in text
    assert "defaults: topic: gpt-4.1, chat: gpt-4.1-mini" in text
    assert "available engines: codex" in text


@pytest.mark.anyio
async def test_model_command_set_and_clear_chat_override(tmp_path: Path) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    chat_prefs = ChatPrefsStore(tmp_path / "telegram_chat_prefs_state.json")
    await chat_prefs.set_engine_override(
        123,
        CODEX_ENGINE,
        EngineOverrides(model=None, reasoning="low"),
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/model set gpt-4.1-mini",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=456,
        chat_type="supergroup",
    )

    await _handle_model_command(
        cfg,
        msg,
        "set gpt-4.1-mini",
        ambient_context=None,
        topic_store=None,
        chat_prefs=chat_prefs,
    )

    override = await chat_prefs.get_engine_override(123, CODEX_ENGINE)
    assert override is not None
    assert override.model == "gpt-4.1-mini"
    assert override.reasoning == "low"
    assert (
        "chat model override set to gpt-4.1-mini for codex."
        in transport.send_calls[-1]["message"].text
    )

    msg_clear = replace(
        msg,
        message_id=11,
        text="/model clear codex",
    )
    await _handle_model_command(
        cfg,
        msg_clear,
        "clear codex",
        ambient_context=None,
        topic_store=None,
        chat_prefs=chat_prefs,
    )

    override = await chat_prefs.get_engine_override(123, CODEX_ENGINE)
    assert override is not None
    assert override.model is None
    assert override.reasoning == "low"
    assert "chat model override cleared." in transport.send_calls[-1]["message"].text


@pytest.mark.anyio
async def test_reasoning_command_set_and_clear_topic_override(tmp_path: Path) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    cfg = replace(cfg, topics=TelegramTopicsSettings(enabled=True, scope="main"))
    topic_store = TopicStateStore(tmp_path / "telegram_topics_state.json")
    await topic_store.set_engine_override(
        123,
        77,
        CODEX_ENGINE,
        EngineOverrides(model="gpt-4.1-mini", reasoning=None),
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/reasoning set High",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=456,
        chat_type="supergroup",
        thread_id=77,
    )

    await _handle_reasoning_command(
        cfg,
        msg,
        "set High",
        ambient_context=None,
        topic_store=topic_store,
        chat_prefs=None,
        resolved_scope="main",
        scope_chat_ids=frozenset({123}),
    )

    override = await topic_store.get_engine_override(123, 77, CODEX_ENGINE)
    assert override is not None
    assert override.model == "gpt-4.1-mini"
    assert override.reasoning == "high"
    assert (
        "topic reasoning override set to high for codex."
        in transport.send_calls[-1]["message"].text
    )

    msg_clear = replace(
        msg,
        message_id=11,
        text="/reasoning clear",
    )
    await _handle_reasoning_command(
        cfg,
        msg_clear,
        "clear",
        ambient_context=None,
        topic_store=topic_store,
        chat_prefs=None,
        resolved_scope="main",
        scope_chat_ids=frozenset({123}),
    )

    override = await topic_store.get_engine_override(123, 77, CODEX_ENGINE)
    assert override is not None
    assert override.model == "gpt-4.1-mini"
    assert override.reasoning is None
    assert (
        "topic reasoning override cleared (using chat default)."
        in transport.send_calls[-1]["message"].text
    )


@pytest.mark.anyio
async def test_reasoning_command_show_reports_overrides(tmp_path: Path) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    cfg = replace(cfg, topics=TelegramTopicsSettings(enabled=True, scope="main"))
    chat_prefs = ChatPrefsStore(tmp_path / "telegram_chat_prefs_state.json")
    topic_store = TopicStateStore(tmp_path / "telegram_topics_state.json")
    await chat_prefs.set_engine_override(
        123,
        CODEX_ENGINE,
        EngineOverrides(model=None, reasoning="low"),
    )
    await topic_store.set_engine_override(
        123,
        88,
        CODEX_ENGINE,
        EngineOverrides(model=None, reasoning="high"),
    )
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/reasoning",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=123,
        thread_id=88,
    )

    await _handle_reasoning_command(
        cfg,
        msg,
        "",
        ambient_context=None,
        topic_store=topic_store,
        chat_prefs=chat_prefs,
        resolved_scope="main",
        scope_chat_ids=frozenset({123}),
    )

    text = transport.send_calls[-1]["message"].text
    assert "engine: codex (global default)" in text
    assert "reasoning: high (topic override)" in text
    assert "defaults: topic: high, chat: low" in text
    assert "available levels: minimal, low, medium, high, xhigh" in text


@pytest.mark.anyio
async def test_reasoning_command_show_reports_claude_levels() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport, engine_id="claude")
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/reasoning",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=123,
    )

    await _handle_reasoning_command(
        cfg,
        msg,
        "",
        ambient_context=None,
        topic_store=None,
        chat_prefs=None,
    )

    text = transport.send_calls[-1]["message"].text
    assert "engine: claude (global default)" in text
    assert "available levels: low, medium, high, xhigh, max" in text


@pytest.mark.anyio
async def test_reasoning_command_set_max_for_claude(tmp_path: Path) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport, engine_id="claude")
    chat_prefs = ChatPrefsStore(tmp_path / "telegram_chat_prefs_state.json")
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/reasoning set max",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=123,
        chat_type="private",
    )

    await _handle_reasoning_command(
        cfg,
        msg,
        "set max",
        ambient_context=None,
        topic_store=None,
        chat_prefs=chat_prefs,
    )

    override = await chat_prefs.get_engine_override(123, "claude")
    assert override is not None
    assert override.reasoning == "max"


@pytest.mark.anyio
async def test_reasoning_command_rejects_unsupported_engine(tmp_path: Path) -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport, engine_id="opencode")
    chat_prefs = ChatPrefsStore(tmp_path / "telegram_chat_prefs_state.json")
    msg = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=10,
        text="/reasoning set high",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=123,
        chat_type="private",
    )

    await _handle_reasoning_command(
        cfg,
        msg,
        "set high",
        ambient_context=None,
        topic_store=None,
        chat_prefs=chat_prefs,
    )

    assert "not supported" in transport.send_calls[-1]["message"].text
    assert await chat_prefs.get_engine_override(123, "opencode") is None


@pytest.mark.anyio
async def test_send_with_resume_waits_for_token() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    sent: list[
        tuple[
            int,
            int,
            str,
            ResumeToken,
            RunContext | None,
            int | None,
            tuple[int, int | None] | None,
            MessageRef | None,
        ]
    ] = []

    async def enqueue(
        chat_id: int,
        user_msg_id: int,
        text: str,
        resume: ResumeToken,
        context: RunContext | None,
        thread_id: int | None,
        session_key: tuple[int, int | None] | None,
        progress_ref: MessageRef | None,
    ) -> None:
        sent.append(
            (
                chat_id,
                user_msg_id,
                text,
                resume,
                context,
                thread_id,
                session_key,
                progress_ref,
            )
        )

    running_task = RunningTask()

    async def trigger_resume() -> None:
        await anyio.sleep(0)
        running_task.resume = ResumeToken(engine=CODEX_ENGINE, value="abc123")
        running_task.resume_ready.set()

    async with anyio.create_task_group() as tg:
        tg.start_soon(trigger_resume)
        await send_with_resume(
            cfg,
            enqueue,
            running_task,
            123,
            10,
            None,
            None,
            "hello",
        )

    assert len(sent) == 1
    assert sent[0][:7] == (
        123,
        10,
        "hello",
        ResumeToken(engine=CODEX_ENGINE, value="abc123"),
        None,
        None,
        None,
    )
    assert sent[0][7] == transport.send_calls[0]["ref"]
    assert transport.send_calls
    queued_text = transport.send_calls[0]["message"].text.lower()
    assert "queued" in queued_text
    assert "codex resume abc123" in queued_text


@pytest.mark.anyio
async def test_send_with_resume_reports_when_missing() -> None:
    transport = FakeTransport()
    cfg = make_cfg(transport)
    sent: list[
        tuple[
            int,
            int,
            str,
            ResumeToken,
            RunContext | None,
            int | None,
            tuple[int, int | None] | None,
            MessageRef | None,
        ]
    ] = []

    async def enqueue(
        chat_id: int,
        user_msg_id: int,
        text: str,
        resume: ResumeToken,
        context: RunContext | None,
        thread_id: int | None,
        session_key: tuple[int, int | None] | None,
        progress_ref: MessageRef | None,
    ) -> None:
        sent.append(
            (
                chat_id,
                user_msg_id,
                text,
                resume,
                context,
                thread_id,
                session_key,
                progress_ref,
            )
        )

    running_task = RunningTask()
    running_task.done.set()

    await send_with_resume(
        cfg,
        enqueue,
        running_task,
        123,
        10,
        None,
        None,
        "hello",
    )

    assert sent == []
    assert transport.send_calls
    assert "resume token" in transport.send_calls[-1]["message"].text.lower()


@pytest.mark.anyio
async def test_run_engine_hides_resume_line_in_topics() -> None:
    transport = _CaptureTransport()
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value="resume-123",
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )

    await _run_engine(
        exec_cfg=exec_cfg,
        runtime=runtime,
        running_tasks={},
        chat_id=123,
        user_msg_id=1,
        text="hello",
        resume_token=None,
        context=None,
        reply_ref=None,
        on_thread_known=None,
        engine_override=None,
        thread_id=77,
        show_resume_line=False,
    )

    assert transport.last_message is not None
    assert "resume-123" not in transport.last_message.text


@pytest.mark.anyio
async def test_run_main_loop_routes_reply_to_running_resume() -> None:
    progress_ready = anyio.Event()
    stop_polling = anyio.Event()
    reply_ready = anyio.Event()
    hold = anyio.Event()

    transport = FakeTransport(progress_ready=progress_ready)
    bot = FakeBot()
    resume_value = "abc123"
    runner = ScriptRunner(
        [Wait(hold), Sleep(0.05), Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value=resume_value,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="first",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )
        await progress_ready.wait()
        assert transport.progress_ref is not None
        assert isinstance(transport.progress_ref.message_id, int)
        reply_id = transport.progress_ref.message_id
        reply_ready.set()
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="followup",
            reply_to_message_id=reply_id,
            reply_to_text=None,
            sender_id=123,
        )
        await stop_polling.wait()

    async with anyio.create_task_group() as tg:
        tg.start_soon(run_main_loop, cfg, poller)
        try:
            with anyio.fail_after(2):
                await reply_ready.wait()
            await anyio.sleep(0)
            hold.set()
            with anyio.fail_after(2):
                while len(runner.calls) < 2:
                    await anyio.sleep(0)
            assert runner.calls[1][1] == ResumeToken(
                engine=CODEX_ENGINE, value=resume_value
            )
        finally:
            hold.set()
            stop_polling.set()
            tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_run_main_loop_ignores_duplicate_message_id_for_replies() -> None:
    transport = FakeTransport()
    bot = FakeBot()
    codex_runner = ScriptRunner([Return(answer="codex")], engine=CODEX_ENGINE)
    claude_runner = ScriptRunner([Return(answer="claude")], engine="claude")
    router = AutoRouter(
        entries=[
            RunnerEntry(engine=codex_runner.engine, runner=codex_runner),
            RunnerEntry(engine=claude_runner.engine, runner=claude_runner),
        ],
        default_engine=claude_runner.engine,
    )
    runtime = TransportRuntime(router=router, projects=_empty_projects())
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=42,
            text="turn on logging in my lemon config for me",
            reply_to_message_id=900,
            reply_to_text="done\n`codex resume c-123`",
            sender_id=123,
            chat_type="private",
        )
        # Telegram can occasionally redeliver the same message id with less reply metadata.
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=42,
            text="turn on logging in my lemon config for me",
            reply_to_message_id=900,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    assert len(codex_runner.calls) == 1
    assert codex_runner.calls[0][1] == ResumeToken(engine=CODEX_ENGINE, value="c-123")
    assert claude_runner.calls == []


@pytest.mark.anyio
async def test_run_main_loop_ignores_duplicate_update_id() -> None:
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(router=_make_router(runner), projects=_empty_projects())
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="first",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            update_id=9001,
        )
        # Same Telegram update id redelivered.
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="second",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            update_id=9001,
        )

    await run_main_loop(cfg, poller)

    assert len(runner.calls) == 1
    assert runner.calls[0][0] == "first"


@pytest.mark.anyio
async def test_run_main_loop_persists_topic_sessions_in_project_scope(
    tmp_path: Path,
) -> None:
    project_chat_id = -100
    resume_value = "resume-123"

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value=resume_value,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    projects = ProjectsConfig(
        projects={
            "takopi": ProjectConfig(
                alias="takopi",
                path=Path("."),
                worktrees_dir=Path(".worktrees"),
                chat_id=project_chat_id,
            )
        },
        default_project=None,
        chat_map={project_chat_id: "takopi"},
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=projects,
        config_path=tmp_path / "takopi.toml",
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        topics=TelegramTopicsSettings(
            enabled=True,
            scope="projects",
        ),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=project_chat_id,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            thread_id=77,
        )

    with anyio.fail_after(2):
        await run_main_loop(cfg, poller)

    state_path = resolve_state_path(runtime.config_path or tmp_path / "takopi.toml")
    store = TopicStateStore(state_path)
    stored = await store.get_session_resume(project_chat_id, 77, CODEX_ENGINE)
    assert stored == ResumeToken(engine=CODEX_ENGINE, value=resume_value)


@pytest.mark.anyio
async def test_run_main_loop_auto_resumes_topic_default_engine(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "takopi.toml"
    topic_path = resolve_state_path(state_path)
    store = TopicStateStore(topic_path)
    await store.set_session_resume(
        123, 77, ResumeToken(engine=CODEX_ENGINE, value="resume-codex")
    )
    await store.set_session_resume(
        123, 77, ResumeToken(engine="claude", value="resume-claude")
    )
    await store.set_default_engine(123, 77, "claude")

    transport = FakeTransport()
    bot = FakeBot()
    codex_runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    claude_runner = ScriptRunner([Return(answer="ok")], engine="claude")
    router = AutoRouter(
        entries=[
            RunnerEntry(engine=codex_runner.engine, runner=codex_runner),
            RunnerEntry(engine=claude_runner.engine, runner=claude_runner),
        ],
        default_engine=codex_runner.engine,
    )
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
                chat_id=123,
            )
        },
        default_project=None,
        chat_map={123: "proj"},
    )
    runtime = TransportRuntime(
        router=router,
        projects=projects,
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        topics=TelegramTopicsSettings(
            enabled=True,
            scope="main",
        ),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            thread_id=77,
        )

    await run_main_loop(cfg, poller)

    assert codex_runner.calls == []
    assert len(claude_runner.calls) == 1
    assert claude_runner.calls[0][1] == ResumeToken(
        engine="claude", value="resume-claude"
    )


@pytest.mark.anyio
async def test_run_main_loop_auto_resumes_chat_sessions(tmp_path: Path) -> None:
    resume_value = "resume-123"
    state_path = tmp_path / "takopi.toml"

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value=resume_value,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
            )
        },
        default_project="proj",
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=projects,
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    store = ChatSessionStore(resolve_sessions_path(state_path))
    stored = await store.get_session_resume(123, None, CODEX_ENGINE)
    assert stored == ResumeToken(engine=CODEX_ENGINE, value=resume_value)

    runner2 = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime2 = TransportRuntime(
        router=_make_router(runner2),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg2 = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime2,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
    )

    async def poller2(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="followup",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg2, poller2)

    assert runner2.calls[0][1] == ResumeToken(engine=CODEX_ENGINE, value=resume_value)


@pytest.mark.anyio
async def test_run_main_loop_prompt_upload_uses_caption_directives(
    tmp_path: Path,
) -> None:
    payload = b"hello"
    proj_dir = tmp_path / "proj"
    other_dir = tmp_path / "other"
    proj_dir.mkdir()
    other_dir.mkdir()

    class _UploadBot(FakeBot):
        async def get_file(self, file_id: str) -> File | None:
            _ = file_id
            return File(file_path="files/hello.txt")

        async def download_file(self, file_path: str) -> bytes | None:
            _ = file_path
            return payload

    transport = FakeTransport()
    bot = _UploadBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=proj_dir,
                worktrees_dir=Path(".worktrees"),
            ),
            "other": ProjectConfig(
                alias="other",
                path=other_dir,
                worktrees_dir=Path(".worktrees"),
            ),
        },
        default_project="proj",
    )
    runtime = TransportRuntime(router=_make_router(runner), projects=projects)
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        files=TelegramFilesSettings(
            enabled=True,
            auto_put=True,
            auto_put_mode="prompt",
        ),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/other do thing",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            document=TelegramDocument(
                file_id="doc-1",
                file_name="hello.txt",
                mime_type="text/plain",
                file_size=len(payload),
                raw={"file_id": "doc-1"},
            ),
        )

    await run_main_loop(cfg, poller)

    saved_path = other_dir / "incoming" / "hello.txt"
    assert saved_path.read_bytes() == payload
    assert runner.calls
    prompt_text, _ = runner.calls[0]
    assert prompt_text.startswith("do thing")
    assert "/other" not in prompt_text
    assert "Attached files:" in prompt_text
    assert "- incoming/hello.txt" in prompt_text


@pytest.mark.anyio
async def test_run_main_loop_voice_transcript_preserves_directive(
    monkeypatch: pytest.MonkeyPatch,
) -> None:
    codex_runner = ScriptRunner([Return(answer="codex")], engine=CODEX_ENGINE)
    claude_runner = ScriptRunner([Return(answer="claude")], engine="claude")
    router = AutoRouter(
        entries=[
            RunnerEntry(engine=claude_runner.engine, runner=claude_runner),
            RunnerEntry(engine=codex_runner.engine, runner=codex_runner),
        ],
        default_engine=claude_runner.engine,
    )
    runtime = TransportRuntime(router=router, projects=_empty_projects())
    transport = FakeTransport()
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=FakeBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        voice_transcription=True,
    )

    async def _fake_transcribe(
        *,
        bot: BotClient,
        msg: TelegramIncomingMessage,
        enabled: bool,
        model: str,
        max_bytes: int | None = None,
        reply,
        base_url: str | None = None,
        api_key: str | None = None,
    ) -> str:
        _ = bot, msg, enabled, model, max_bytes, reply, base_url, api_key
        return "/codex do thing"

    monkeypatch.setattr(telegram_loop, "transcribe_voice", _fake_transcribe)
    monkeypatch.setattr(telegram_loop, "list_command_ids", lambda **_: [])

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            voice=TelegramVoice(
                file_id="voice-1",
                mime_type=None,
                file_size=None,
                duration=None,
                raw={"file_id": "voice-1"},
            ),
        )

    await run_main_loop(cfg, poller)

    assert not claude_runner.calls
    assert len(codex_runner.calls) == 1
    assert codex_runner.calls[0][0].startswith("(voice transcribed) do thing")


@pytest.mark.anyio
async def test_run_main_loop_debounces_forwarded_messages_preserves_directives() -> (
    None
):
    codex_runner = ScriptRunner([Return(answer="codex")], engine=CODEX_ENGINE)
    claude_runner = ScriptRunner([Return(answer="claude")], engine="claude")
    router = AutoRouter(
        entries=[
            RunnerEntry(engine=claude_runner.engine, runner=claude_runner),
            RunnerEntry(engine=codex_runner.engine, runner=codex_runner),
        ],
        default_engine=claude_runner.engine,
    )
    runtime = TransportRuntime(router=router, projects=_empty_projects())
    transport = FakeTransport()
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=FakeBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=DEBOUNCE_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/codex summarize these",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )
        await anyio.sleep(_cfg.forward_coalesce_s / 2)
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="a",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            raw={"forward_origin": {"type": "user"}},
        )
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=3,
            text="b",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            raw={"forward_origin": {"type": "user"}},
        )
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=4,
            text="c",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            raw={"forward_origin": {"type": "user"}},
        )

    await run_main_loop(cfg, poller)

    assert not claude_runner.calls
    assert len(codex_runner.calls) == 1
    prompt_text, _ = codex_runner.calls[0]
    assert prompt_text == "summarize these\n\na\n\nb\n\nc"


@pytest.mark.anyio
async def test_run_main_loop_ignores_forwarded_without_prompt() -> None:
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(router=_make_router(runner), projects=_empty_projects())
    transport = FakeTransport()
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=FakeBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="a",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            raw={"forward_origin": {"type": "user"}},
        )
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="b",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            raw={"forward_origin": {"type": "user"}},
        )

    await run_main_loop(cfg, poller)

    assert runner.calls == []


@pytest.mark.anyio
async def test_run_main_loop_forwarded_document_still_uploads(
    tmp_path: Path,
) -> None:
    payload = b"hello"

    class _UploadBot(FakeBot):
        async def get_file(self, file_id: str) -> File | None:
            _ = file_id
            return File(file_path="files/hello.txt")

        async def download_file(self, file_path: str) -> bytes | None:
            _ = file_path
            return payload

    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
            )
        },
        default_project="proj",
    )
    runtime = TransportRuntime(router=_make_router(runner), projects=projects)
    transport = FakeTransport()
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=_UploadBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        files=TelegramFilesSettings(
            enabled=True,
            auto_put=True,
            auto_put_mode="prompt",
        ),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="do thing",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            document=TelegramDocument(
                file_id="doc-1",
                file_name="hello.txt",
                mime_type="text/plain",
                file_size=len(payload),
                raw={"file_id": "doc-1"},
            ),
            raw={"forward_origin": {"type": "user"}},
        )

    await run_main_loop(cfg, poller)

    saved_path = tmp_path / "incoming" / "hello.txt"
    assert saved_path.read_bytes() == payload
    assert runner.calls
    prompt_text, _ = runner.calls[0]
    assert prompt_text.startswith("do thing")
    assert "Attached files:" in prompt_text
    assert "- incoming/hello.txt" in prompt_text


@pytest.mark.anyio
async def test_run_main_loop_prompt_upload_auto_resumes_chat_sessions(
    tmp_path: Path,
) -> None:
    payload = b"hello"
    resume_value = "resume-123"
    state_path = tmp_path / "takopi.toml"
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    class _UploadBot(FakeBot):
        async def get_file(self, file_id: str) -> File | None:
            _ = file_id
            return File(file_path="files/hello.txt")

        async def download_file(self, file_path: str) -> bytes | None:
            _ = file_path
            return payload

    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=project_dir,
                worktrees_dir=Path(".worktrees"),
            )
        },
        default_project="proj",
    )
    bot = _UploadBot()

    transport = FakeTransport()
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value=resume_value,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=projects,
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
        files=TelegramFilesSettings(
            enabled=True,
            auto_put=True,
            auto_put_mode="prompt",
        ),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            document=TelegramDocument(
                file_id="doc-1",
                file_name="hello.txt",
                mime_type="text/plain",
                file_size=len(payload),
                raw={"file_id": "doc-1"},
            ),
        )

    await run_main_loop(cfg, poller)

    store = ChatSessionStore(resolve_sessions_path(state_path))
    stored = await store.get_session_resume(123, None, CODEX_ENGINE)
    assert stored == ResumeToken(engine=CODEX_ENGINE, value=resume_value)

    transport2 = FakeTransport()
    runner2 = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg2 = ExecBridgeConfig(
        transport=transport2,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime2 = TransportRuntime(
        router=_make_router(runner2),
        projects=projects,
        config_path=state_path,
    )
    cfg2 = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime2,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg2,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
        files=TelegramFilesSettings(
            enabled=True,
            auto_put=True,
            auto_put_mode="prompt",
        ),
    )

    async def poller2(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="followup",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            document=TelegramDocument(
                file_id="doc-2",
                file_name="hello2.txt",
                mime_type="text/plain",
                file_size=len(payload),
                raw={"file_id": "doc-2"},
            ),
        )

    await run_main_loop(cfg2, poller2)

    assert runner2.calls[0][1] == ResumeToken(
        engine=CODEX_ENGINE,
        value=resume_value,
    )


@pytest.mark.anyio
async def test_run_main_loop_image_upload_without_caption_prompts_runner(
    tmp_path: Path,
) -> None:
    payload = b"image-bytes"
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    class _UploadBot(FakeBot):
        async def get_file(self, file_id: str) -> File | None:
            _ = file_id
            return File(file_path="photos/image.jpg")

        async def download_file(self, file_path: str) -> bytes | None:
            _ = file_path
            return payload

    transport = FakeTransport()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=ProjectsConfig(
            projects={
                "proj": ProjectConfig(
                    alias="proj",
                    path=project_dir,
                    worktrees_dir=Path(".worktrees"),
                )
            },
            default_project="proj",
        ),
    )
    cfg = TelegramBridgeConfig(
        bot=_UploadBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        files=TelegramFilesSettings(enabled=True, auto_put=True),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            document=TelegramDocument(
                file_id="img-1",
                file_name=None,
                mime_type="image/jpeg",
                file_size=len(payload),
                raw={"file_id": "img-1"},
            ),
        )

    await run_main_loop(cfg, poller)

    assert runner.calls
    prompt_text, _ = runner.calls[0]
    assert prompt_text.startswith("Describe this image.")
    assert "Attached images:" in prompt_text


@pytest.mark.anyio
async def test_run_main_loop_image_upload_without_caption_forwards_to_channel(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    payload = b"image-bytes"
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    class _UploadBot(FakeBot):
        async def get_file(self, file_id: str) -> File | None:
            _ = file_id
            return File(file_path="photos/image.jpg")

        async def download_file(self, file_path: str) -> bytes | None:
            _ = file_path
            return payload

    forwarded: list[str] = []

    async def fake_forward_to_channel(cfg, **kwargs):
        _ = cfg
        forwarded.append(kwargs["text"])
        return True

    monkeypatch.setattr(telegram_loop, "forward_to_channel", fake_forward_to_channel)

    async def fake_run_reply_server(_cfg):
        return None

    monkeypatch.setattr(telegram_loop, "run_reply_server", fake_run_reply_server)

    transport = FakeTransport()
    runner = ScriptRunner([Return(answer="ok")], engine="claude")
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=ProjectsConfig(
            projects={
                "proj": ProjectConfig(
                    alias="proj",
                    path=project_dir,
                    worktrees_dir=Path(".worktrees"),
                    default_engine="claude",
                )
            },
            default_project="proj",
        ),
    )
    cfg = TelegramBridgeConfig(
        bot=_UploadBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        files=TelegramFilesSettings(enabled=True, auto_put=True),
        channel_bridge=TelegramChannelBridgeSettings(enabled=True, send_progress=False),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            document=TelegramDocument(
                file_id="img-1",
                file_name=None,
                mime_type="image/jpeg",
                file_size=len(payload),
                raw={"file_id": "img-1"},
            ),
        )

    await run_main_loop(cfg, poller)

    assert forwarded
    assert forwarded[0].startswith("Describe this image.")
    assert "Attached images:" in forwarded[0]
    assert ".takopi-uploads/telegram/123/1/" in forwarded[0]
    assert "/.takopi-uploads/" in (project_dir / ".gitignore").read_text()
    assert runner.calls == []


@pytest.mark.anyio
async def test_run_main_loop_media_group_images_without_caption_prompts_runner(
    tmp_path: Path,
) -> None:
    payloads = {
        "photos/one.jpg": b"one",
        "photos/two.jpg": b"two",
    }
    file_map = {
        "img-1": "photos/one.jpg",
        "img-2": "photos/two.jpg",
    }
    project_dir = tmp_path / "proj"
    project_dir.mkdir()

    class _UploadBot(FakeBot):
        async def get_file(self, file_id: str) -> File | None:
            file_path = file_map.get(file_id)
            if file_path is None:
                return None
            return File(file_path=file_path)

        async def download_file(self, file_path: str) -> bytes | None:
            return payloads.get(file_path)

    transport = FakeTransport()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=ProjectsConfig(
            projects={
                "proj": ProjectConfig(
                    alias="proj",
                    path=project_dir,
                    worktrees_dir=Path(".worktrees"),
                )
            },
            default_project="proj",
        ),
    )
    cfg = TelegramBridgeConfig(
        bot=_UploadBot(),
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=BATCH_MEDIA_GROUP_DEBOUNCE_S,
        files=TelegramFilesSettings(enabled=True, auto_put=True),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            media_group_id="grp-1",
            document=TelegramDocument(
                file_id="img-1",
                file_name=None,
                mime_type="image/jpeg",
                file_size=len(payloads["photos/one.jpg"]),
                raw={"file_id": "img-1"},
            ),
        )
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
            media_group_id="grp-1",
            document=TelegramDocument(
                file_id="img-2",
                file_name=None,
                mime_type="image/jpeg",
                file_size=len(payloads["photos/two.jpg"]),
                raw={"file_id": "img-2"},
            ),
        )

    await run_main_loop(cfg, poller)

    assert runner.calls
    prompt_text, _ = runner.calls[0]
    assert prompt_text.startswith("Describe these images.")
    assert "Attached images:" in prompt_text


@pytest.mark.anyio
async def test_run_main_loop_command_updates_chat_session_resume(
    tmp_path: Path,
    monkeypatch,
) -> None:
    class _Command:
        id = "run_cmd"
        description = "run command"

        async def handle(self, ctx):
            await ctx.executor.run_one(commands.RunRequest(prompt="hello"))
            return commands.CommandResult(text="done")

    entrypoints = [
        FakeEntryPoint(
            "run_cmd",
            "takopi.commands.run_cmd:BACKEND",
            plugins.COMMAND_GROUP,
            loader=_Command,
        )
    ]
    install_entrypoints(monkeypatch, entrypoints)

    resume_value = "resume-123"
    state_path = tmp_path / "takopi.toml"

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value=resume_value,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
        show_resume_line=False,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/run_cmd",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    store = ChatSessionStore(resolve_sessions_path(state_path))
    stored = await store.get_session_resume(123, None, CODEX_ENGINE)
    assert stored == ResumeToken(engine=CODEX_ENGINE, value=resume_value)

    transport2 = FakeTransport()
    runner2 = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg2 = ExecBridgeConfig(
        transport=transport2,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime2 = TransportRuntime(
        router=_make_router(runner2),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg2 = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime2,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg2,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
        show_resume_line=False,
    )

    async def poller2(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="followup",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg2, poller2)

    assert runner2.calls[0][1] == ResumeToken(
        engine=CODEX_ENGINE,
        value=resume_value,
    )


@pytest.mark.anyio
async def test_run_main_loop_hides_resume_line_when_disabled(
    tmp_path: Path,
) -> None:
    resume_value = "resume-123"
    state_path = tmp_path / "takopi.toml"

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value=resume_value,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
            )
        },
        default_project="proj",
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=projects,
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
        show_resume_line=False,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    assert transport.send_calls
    final_text = transport.send_calls[-1]["message"].text
    assert resume_value not in final_text


@pytest.mark.anyio
async def test_run_main_loop_hides_resume_line_without_context(
    tmp_path: Path,
) -> None:
    resume_value = "resume-ctxless"
    state_path = tmp_path / "takopi.toml"

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value=resume_value,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
        show_resume_line=False,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    assert transport.send_calls
    final_text = transport.send_calls[-1]["message"].text
    assert resume_value not in final_text


@pytest.mark.anyio
async def test_run_main_loop_applies_chat_bound_context(
    tmp_path: Path,
) -> None:
    state_path = tmp_path / "takopi.toml"

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    projects = ProjectsConfig(
        projects={
            "alpha": ProjectConfig(
                alias="Alpha",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
            ),
            "beta": ProjectConfig(
                alias="Beta",
                path=tmp_path / "beta",
                worktrees_dir=Path(".worktrees"),
            ),
        },
        default_project="alpha",
    )
    (tmp_path / "beta").mkdir()
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=projects,
        config_path=state_path,
    )
    prefs = ChatPrefsStore(resolve_prefs_path(state_path))
    await prefs.set_context(123, RunContext(project="beta"))
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
        show_resume_line=False,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    assert transport.send_calls
    final_text = transport.send_calls[-1]["message"].text
    assert "`ctx: Beta`" in final_text


@pytest.mark.anyio
async def test_run_main_loop_chat_sessions_isolate_group_senders(
    tmp_path: Path,
) -> None:
    resume_value = "resume-group"
    state_path = tmp_path / "takopi.toml"

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner(
        [Return(answer="ok")],
        engine=CODEX_ENGINE,
        resume_value=resume_value,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=-100,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=111,
            chat_type="supergroup",
        )

    await run_main_loop(cfg, poller)

    runner2 = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime2 = TransportRuntime(
        router=_make_router(runner2),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg2 = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime2,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
    )

    async def poller2(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=-100,
            message_id=2,
            text="followup",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=222,
            chat_type="supergroup",
        )

    await run_main_loop(cfg2, poller2)

    assert runner2.calls[0][1] is None


@pytest.mark.anyio
async def test_run_main_loop_new_clears_chat_sessions(tmp_path: Path) -> None:
    state_path = tmp_path / "takopi.toml"
    store = ChatSessionStore(resolve_sessions_path(state_path))
    await store.set_session_resume(
        123, None, ResumeToken(engine=CODEX_ENGINE, value="resume-1")
    )

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/new",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    store2 = ChatSessionStore(resolve_sessions_path(state_path))
    assert await store2.get_session_resume(123, None, CODEX_ENGINE) is None


@pytest.mark.anyio
async def test_run_main_loop_usage_uses_channel_slash_passthrough(monkeypatch) -> None:
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(router=_make_router(runner), projects=_empty_projects())
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="stateless",
    )
    cfg.channel_bridge.enabled = True

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/usage",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    async def fake_usage(cfg: TelegramBridgeConfig, command: str) -> str:
        _ = cfg
        assert command == "/usage"
        return "Claude Code /usage:\n\nTotal cost: $0.42"

    monkeypatch.setattr(telegram_loop, "channel_bridge_slash_command_text", fake_usage)
    monkeypatch.setattr(telegram_loop, "run_reply_server", _noop_reply_server)

    await run_main_loop(cfg, poller)

    assert runner.calls == []
    assert transport.send_calls
    assert "Total cost: $0.42" in transport.send_calls[-1]["message"].text


@pytest.mark.anyio
async def test_run_main_loop_status_uses_channel_slash_passthrough(monkeypatch) -> None:
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(router=_make_router(runner), projects=_empty_projects())
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="stateless",
    )
    cfg.channel_bridge.enabled = True

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/status",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    async def fake_status(cfg: TelegramBridgeConfig, command: str) -> str:
        _ = cfg
        assert command == "/status"
        return "Claude Code /status:\n\nClaude Code v2.1.173"

    monkeypatch.setattr(telegram_loop, "channel_bridge_slash_command_text", fake_status)
    monkeypatch.setattr(telegram_loop, "run_reply_server", _noop_reply_server)

    await run_main_loop(cfg, poller)

    assert runner.calls == []
    assert transport.send_calls
    assert "Claude Code v2.1.173" in transport.send_calls[-1]["message"].text


@pytest.mark.anyio
async def test_run_main_loop_stats_uses_channel_slash_passthrough(monkeypatch) -> None:
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(router=_make_router(runner), projects=_empty_projects())
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="stateless",
    )
    cfg.channel_bridge.enabled = True

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/stats",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    async def fake_stats(cfg: TelegramBridgeConfig, command: str) -> str:
        _ = cfg
        assert command == "/stats"
        return "Claude Code /stats:\n\n· Total tokens: 4.1m"

    monkeypatch.setattr(telegram_loop, "channel_bridge_slash_command_text", fake_stats)
    monkeypatch.setattr(telegram_loop, "run_reply_server", _noop_reply_server)

    await run_main_loop(cfg, poller)

    assert runner.calls == []
    assert transport.send_calls
    assert "Total tokens: 4.1m" in transport.send_calls[-1]["message"].text


@pytest.mark.anyio
async def test_run_main_loop_bridge_status_is_local(monkeypatch) -> None:
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(router=_make_router(runner), projects=_empty_projects())
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="stateless",
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/bridge_status",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    monkeypatch.setattr(
        telegram_loop,
        "channel_bridge_status_text",
        lambda cfg: "Takopi channel bridge:\nenabled: yes",
    )

    await run_main_loop(cfg, poller)

    assert runner.calls == []
    assert transport.send_calls
    assert "Takopi channel bridge:" in transport.send_calls[-1]["message"].text


@pytest.mark.anyio
async def test_run_main_loop_model_uses_channel_passthrough(monkeypatch) -> None:
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(router=_make_router(runner), projects=_empty_projects())
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="stateless",
    )
    cfg.channel_bridge.enabled = True

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/model 2",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    async def fake_model(cfg: TelegramBridgeConfig, args_text: str) -> str:
        _ = cfg
        assert args_text == "2"
        return "Claude Code /model:\n\nSwitched model to Sonnet."

    monkeypatch.setattr(telegram_loop, "channel_bridge_model_command_text", fake_model)
    monkeypatch.setattr(telegram_loop, "run_reply_server", _noop_reply_server)

    await run_main_loop(cfg, poller)

    assert runner.calls == []
    assert transport.send_calls
    assert "Switched model to Sonnet" in transport.send_calls[-1]["message"].text


@pytest.mark.anyio
async def test_run_main_loop_verbose_on_updates_chat_prefs(
    tmp_path: Path, monkeypatch: pytest.MonkeyPatch
) -> None:
    state_path = tmp_path / "takopi.toml"
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=ExecBridgeConfig(
            transport=transport,
            presenter=MarkdownPresenter(),
            final_notify=True,
        ),
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="stateless",
    )
    cfg.channel_bridge.enabled = True
    monkeypatch.setattr(telegram_loop, "run_reply_server", _noop_reply_server)

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/verbose on",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    prefs = ChatPrefsStore(resolve_prefs_path(state_path))
    assert await prefs.get_channel_verbose(123) is True
    assert runner.calls == []
    assert "verbose mode: on" in transport.send_calls[-1]["message"].text


@pytest.mark.anyio
async def test_run_main_loop_compact_clears_chat_sessions(tmp_path: Path) -> None:
    state_path = tmp_path / "takopi.toml"
    store = ChatSessionStore(resolve_sessions_path(state_path))
    await store.set_session_resume(
        123, None, ResumeToken(engine=CODEX_ENGINE, value="resume-1")
    )

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        session_mode="chat",
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/compact",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    store2 = ChatSessionStore(resolve_sessions_path(state_path))
    assert await store2.get_session_resume(123, None, CODEX_ENGINE) is None
    assert runner.calls == []


@pytest.mark.anyio
async def test_run_main_loop_usage_is_handled_locally() -> None:
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/usage",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            chat_type="private",
        )

    await run_main_loop(cfg, poller)

    assert runner.calls == []
    assert transport.send_calls
    assert "not proxied through Telegram" in transport.send_calls[-1]["message"].text


@pytest.mark.anyio
async def test_run_main_loop_new_clears_topic_sessions(tmp_path: Path) -> None:
    state_path = tmp_path / "takopi.toml"
    store = TopicStateStore(resolve_state_path(state_path))
    await store.set_session_resume(
        123, 77, ResumeToken(engine=CODEX_ENGINE, value="resume-1")
    )

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
        config_path=state_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        topics=TelegramTopicsSettings(enabled=True, scope="main"),
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/new",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            thread_id=77,
            chat_type="supergroup",
        )

    with anyio.fail_after(2):
        await run_main_loop(cfg, poller)

    store2 = TopicStateStore(resolve_state_path(state_path))
    assert await store2.get_session_resume(123, 77, CODEX_ENGINE) is None


@pytest.mark.anyio
async def test_run_main_loop_replies_in_same_thread() -> None:
    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            thread_id=77,
        )

    await run_main_loop(cfg, poller)

    reply_calls = [
        call
        for call in transport.send_calls
        if call["options"] is not None and call["options"].reply_to is not None
    ]
    assert reply_calls
    assert all(call["options"].thread_id == 77 for call in reply_calls)


@pytest.mark.anyio
async def test_run_main_loop_batches_media_group_upload(
    tmp_path: Path,
) -> None:
    payloads = {
        "photos/file_1.jpg": b"one",
        "photos/file_2.jpg": b"two",
    }
    file_map = {
        "doc-1": "photos/file_1.jpg",
        "doc-2": "photos/file_2.jpg",
    }

    class _MediaBot(FakeBot):
        async def get_file(self, file_id: str) -> File | None:
            file_path = file_map.get(file_id)
            if file_path is None:
                return None
            return File(file_path=file_path)

        async def download_file(self, file_path: str) -> bytes | None:
            return payloads.get(file_path)

    transport = FakeTransport()
    bot = _MediaBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=tmp_path,
                worktrees_dir=Path(".worktrees"),
            )
        },
        default_project=None,
    )
    runtime = TransportRuntime(router=_make_router(runner), projects=projects)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=BATCH_MEDIA_GROUP_DEBOUNCE_S,
        files=TelegramFilesSettings(enabled=True, auto_put=True),
    )
    msg1 = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=1,
        text="/file put /proj incoming/test1",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=321,
        chat_type="private",
        media_group_id="grp-1",
        document=TelegramDocument(
            file_id="doc-1",
            file_name=None,
            mime_type="image/jpeg",
            file_size=len(payloads["photos/file_1.jpg"]),
            raw={"file_id": "doc-1"},
        ),
    )
    msg2 = TelegramIncomingMessage(
        transport="telegram",
        chat_id=123,
        message_id=2,
        text="",
        reply_to_message_id=None,
        reply_to_text=None,
        sender_id=321,
        chat_type="private",
        media_group_id="grp-1",
        document=TelegramDocument(
            file_id="doc-2",
            file_name=None,
            mime_type="image/jpeg",
            file_size=len(payloads["photos/file_2.jpg"]),
            raw={"file_id": "doc-2"},
        ),
    )

    stop_polling = anyio.Event()

    async def poller(_cfg: TelegramBridgeConfig):
        yield msg1
        yield msg2
        await stop_polling.wait()

    async with anyio.create_task_group() as tg:
        tg.start_soon(run_main_loop, cfg, poller)
        try:
            with anyio.fail_after(3):
                while len(transport.send_calls) < 1:
                    await anyio.sleep(0.05)
            assert len(transport.send_calls) == 1
            text = transport.send_calls[0]["message"].text
            assert "saved file_1.jpg, file_2.jpg" in text
            assert "to incoming/test1/" in text
            target_dir = tmp_path / "incoming" / "test1"
            assert (target_dir / "file_1.jpg").read_bytes() == payloads[
                "photos/file_1.jpg"
            ]
            assert (target_dir / "file_2.jpg").read_bytes() == payloads[
                "photos/file_2.jpg"
            ]
        finally:
            stop_polling.set()
            tg.cancel_scope.cancel()


@pytest.mark.anyio
async def test_run_main_loop_handles_command_plugins(monkeypatch) -> None:
    class _Command:
        id = "echo_cmd"
        description = "echo"

        async def handle(self, ctx):
            return commands.CommandResult(text=f"echo:{ctx.args_text}")

    entrypoints = [
        FakeEntryPoint(
            "echo_cmd",
            "takopi.commands.echo:BACKEND",
            plugins.COMMAND_GROUP,
            loader=_Command,
        )
    ]
    install_entrypoints(monkeypatch, entrypoints)

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/echo_cmd hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert runner.calls == []
    assert transport.send_calls
    assert transport.send_calls[-1]["message"].text == "echo:hello"


def test_parse_callback_data() -> None:
    assert telegram_loop.parse_callback_data("echo_cmd:hello world") == (
        "echo_cmd",
        "hello world",
    )
    assert telegram_loop.parse_callback_data("Echo_Cmd") == ("echo_cmd", "")
    assert telegram_loop.parse_callback_data("echo_cmd:a:b:c") == ("echo_cmd", "a:b:c")


@pytest.mark.anyio
async def test_run_main_loop_routes_callback_to_command_plugins(monkeypatch) -> None:
    seen: dict[str, Any] = {}

    class _Command:
        id = "callback_cmd"
        description = "callback"

        async def handle(self, ctx):
            seen["text"] = ctx.text
            seen["args"] = ctx.args
            seen["message"] = ctx.message
            return commands.CommandResult(text=f"callback:{ctx.args_text}")

    entrypoints = [
        FakeEntryPoint(
            "callback_cmd",
            "takopi.commands.callback:BACKEND",
            plugins.COMMAND_GROUP,
            loader=_Command,
        )
    ]
    install_entrypoints(monkeypatch, entrypoints)

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramCallbackQuery(
            transport="telegram",
            chat_id=123,
            message_id=42,
            callback_query_id="cbq-1",
            data="callback_cmd:hello world",
            sender_id=123,
            raw={
                "id": "cbq-1",
                "message": {
                    "message_id": 42,
                    "message_thread_id": 77,
                    "chat": {"id": 123, "type": "private"},
                },
            },
        )

    await run_main_loop(cfg, poller)

    assert runner.calls == []
    assert bot.callback_calls
    assert bot.callback_calls[-1]["callback_query_id"] == "cbq-1"
    assert transport.send_calls[-1]["message"].text == "callback:hello world"
    assert seen["text"] == "callback_cmd:hello world"
    assert seen["args"] == ("hello", "world")
    message_ref = cast(MessageRef, seen["message"])
    assert message_ref.message_id == 42
    assert message_ref.thread_id == 77
    assert message_ref.sender_id == 123


@pytest.mark.anyio
async def test_run_main_loop_command_uses_project_default_engine(
    monkeypatch,
) -> None:
    class _Command:
        id = "use_project"
        description = "use project default"

        async def handle(self, ctx):
            result = await ctx.executor.run_one(
                commands.RunRequest(
                    prompt="hello",
                    context=RunContext(project="proj"),
                ),
                mode="capture",
            )
            return commands.CommandResult(text=f"ran:{result.engine}")

    entrypoints = [
        FakeEntryPoint(
            "use_project",
            "takopi.commands.use_project:BACKEND",
            plugins.COMMAND_GROUP,
            loader=_Command,
        )
    ]
    install_entrypoints(monkeypatch, entrypoints)

    transport = FakeTransport()
    bot = FakeBot()
    codex_runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    pi_runner = ScriptRunner([Return(answer="ok")], engine="pi")
    router = AutoRouter(
        entries=[
            RunnerEntry(engine=codex_runner.engine, runner=codex_runner),
            RunnerEntry(engine=pi_runner.engine, runner=pi_runner),
        ],
        default_engine=codex_runner.engine,
    )
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=Path("."),
                worktrees_dir=Path(".worktrees"),
                default_engine=pi_runner.engine,
            )
        },
        default_project=None,
    )
    runtime = TransportRuntime(
        router=router,
        projects=projects,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/use_project",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert codex_runner.calls == []
    assert len(pi_runner.calls) == 1
    assert transport.send_calls[-1]["message"].text == "ran:pi"


@pytest.mark.anyio
async def test_run_main_loop_command_defaults_to_chat_project(
    monkeypatch,
) -> None:
    class _Command:
        id = "auto_ctx"
        description = "auto context"

        async def handle(self, ctx):
            result = await ctx.executor.run_one(
                commands.RunRequest(prompt="hello"),
                mode="capture",
            )
            return commands.CommandResult(text=f"ran:{result.engine}")

    entrypoints = [
        FakeEntryPoint(
            "auto_ctx",
            "takopi.commands.auto_ctx:BACKEND",
            plugins.COMMAND_GROUP,
            loader=_Command,
        )
    ]
    install_entrypoints(monkeypatch, entrypoints)

    transport = FakeTransport()
    bot = FakeBot()
    codex_runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    pi_runner = ScriptRunner([Return(answer="ok")], engine="pi")
    router = AutoRouter(
        entries=[
            RunnerEntry(engine=codex_runner.engine, runner=codex_runner),
            RunnerEntry(engine=pi_runner.engine, runner=pi_runner),
        ],
        default_engine=codex_runner.engine,
    )
    projects = ProjectsConfig(
        projects={
            "proj": ProjectConfig(
                alias="proj",
                path=Path("."),
                worktrees_dir=Path(".worktrees"),
                default_engine=pi_runner.engine,
                chat_id=-42,
            )
        },
        default_project=None,
        chat_map={-42: "proj"},
    )
    runtime = TransportRuntime(
        router=router,
        projects=projects,
    )
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=-42,
            message_id=1,
            text="/auto_ctx",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert codex_runner.calls == []
    assert len(pi_runner.calls) == 1
    assert transport.send_calls[-1]["message"].text == "ran:pi"


@pytest.mark.anyio
async def test_run_main_loop_refreshes_command_ids(monkeypatch) -> None:
    class _Command:
        id = "late_cmd"
        description = "late command"

        async def handle(self, ctx):
            return commands.CommandResult(text="late")

    entrypoints = [
        FakeEntryPoint(
            "late_cmd",
            "takopi.commands.late:BACKEND",
            plugins.COMMAND_GROUP,
            loader=_Command,
        )
    ]
    install_entrypoints(monkeypatch, entrypoints)

    calls = {"count": 0}

    def _list_command_ids(*, allowlist=None):
        _ = allowlist
        calls["count"] += 1
        if calls["count"] == 1:
            return []
        return ["late_cmd"]

    monkeypatch.setattr(telegram_loop, "list_command_ids", _list_command_ids)

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="/late_cmd hello",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
        )

    await run_main_loop(cfg, poller)

    assert calls["count"] >= 2
    assert transport.send_calls[-1]["message"].text == "late"


@pytest.mark.anyio
async def test_run_main_loop_mentions_only_skips_voice_and_files(
    monkeypatch, tmp_path
) -> None:
    calls = {"voice": 0, "file": 0}

    async def fake_transcribe_voice(**kwargs):
        _ = kwargs
        calls["voice"] += 1
        return "hello"

    async def fake_handle_file_put_default(*args, **kwargs):
        _ = args, kwargs
        calls["file"] += 1
        return None

    monkeypatch.setattr(telegram_loop, "transcribe_voice", fake_transcribe_voice)
    monkeypatch.setattr(
        telegram_loop, "_handle_file_put_default", fake_handle_file_put_default
    )

    transport = FakeTransport()
    bot = FakeBot()
    runner = ScriptRunner([Return(answer="ok")], engine=CODEX_ENGINE)
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    config_path = tmp_path / "takopi.toml"
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
        config_path=config_path,
    )
    cfg = TelegramBridgeConfig(
        bot=bot,
        runtime=runtime,
        chat_id=123,
        startup_msg="",
        exec_cfg=exec_cfg,
        forward_coalesce_s=FAST_FORWARD_COALESCE_S,
        media_group_debounce_s=FAST_MEDIA_GROUP_DEBOUNCE_S,
        voice_transcription=True,
        files=TelegramFilesSettings(enabled=True, auto_put=True),
    )

    prefs = ChatPrefsStore(resolve_prefs_path(config_path))
    await prefs.set_trigger_mode(123, "mentions")

    voice = TelegramVoice(
        file_id="voice-id",
        mime_type="audio/ogg",
        file_size=5,
        duration=1,
        raw={},
    )
    document = TelegramDocument(
        file_id="doc-id",
        file_name="doc.txt",
        mime_type="text/plain",
        file_size=5,
        raw={},
    )

    async def poller(_cfg: TelegramBridgeConfig):
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=1,
            text="",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            voice=voice,
            raw={},
        )
        yield TelegramIncomingMessage(
            transport="telegram",
            chat_id=123,
            message_id=2,
            text="",
            reply_to_message_id=None,
            reply_to_text=None,
            sender_id=123,
            document=document,
            raw={},
        )

    await run_main_loop(cfg, poller)

    assert calls["voice"] == 0
    assert calls["file"] == 0
    assert runner.calls == []


@pytest.mark.anyio
async def test_run_engine_sanitizes_claude_slash_commands_with_attachments() -> None:
    transport = _CaptureTransport()
    runner = ScriptRunner([Return(answer="ok")], engine="claude")
    exec_cfg = ExecBridgeConfig(
        transport=transport,
        presenter=MarkdownPresenter(),
        final_notify=True,
    )
    runtime = TransportRuntime(
        router=_make_router(runner),
        projects=_empty_projects(),
    )

    await _run_engine(
        exec_cfg=exec_cfg,
        runtime=runtime,
        running_tasks={},
        chat_id=123,
        user_msg_id=1,
        text=(
            "/ usage\n\n"
            "Attached images:\n"
            "- /tmp/file.jpg\n"
            "[Telegram artifact delivery] screenshot"
        ),
        resume_token=None,
        context=None,
        reply_ref=None,
        on_thread_known=None,
        engine_override=None,
        thread_id=None,
        show_resume_line=False,
    )

    assert runner.calls[0][0] == "/usage"
