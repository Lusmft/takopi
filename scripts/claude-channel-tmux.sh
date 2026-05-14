#!/usr/bin/env bash
set -euo pipefail
SESSION=${TAKOPI_CLAUDE_CHANNEL_SESSION:-takopi_channel_usegateway}
WORKDIR=${TAKOPI_CLAUDE_CHANNEL_CWD:-/root/usegateway}
CHANNEL=${TAKOPI_CLAUDE_CHANNEL_NAME:-takopi}
CMD="claude --dangerously-load-development-channels server:${CHANNEL}"

tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -c "$WORKDIR" "$CMD"
# Claude Code currently prompts once per launch for development channels.
# Confirm the local-development warning so the channel server actually starts.
sleep "${TAKOPI_CLAUDE_CHANNEL_CONFIRM_DELAY:-3}"
tmux send-keys -t "$SESSION" Enter || true

while tmux has-session -t "$SESSION" 2>/dev/null; do
  sleep 5
done
exit 1
