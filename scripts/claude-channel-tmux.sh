#!/usr/bin/env bash
set -euo pipefail
SESSION=${TAKOPI_CLAUDE_CHANNEL_SESSION:-takopi_channel_usegateway}
WORKDIR=${TAKOPI_CLAUDE_CHANNEL_CWD:-/root/usegateway}
CHANNEL=${TAKOPI_CLAUDE_CHANNEL_NAME:-takopi}
RESUME=${TAKOPI_CLAUDE_CHANNEL_RESUME:-none}
ALLOWED_TOOLS=${TAKOPI_CLAUDE_ALLOWED_TOOLS:-}
SCRIPT_DIR=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
CHANNEL_SERVER_DIR=${TAKOPI_CLAUDE_CHANNEL_SERVER_DIR:-${SCRIPT_DIR}/../channel-server}

project_slug() {
  local path=${1%/}
  printf "%s" "-${path#/}" | tr "/" "-"
}

latest_session_id() {
  local slug session_file
  slug=$(project_slug "$WORKDIR")
  session_file=$(
    find "/root/.claude/projects/${slug}" -maxdepth 1 -type f -name "*.jsonl" \
      -printf "%T@ %p\n" 2>/dev/null \
      | sort -nr \
      | awk "NR == 1 {print \$2}"
  )
  if [[ -n "$session_file" ]]; then
    basename "$session_file" .jsonl
  fi
}


if [[ -f "${CHANNEL_SERVER_DIR}/package-lock.json" && ! -d "${CHANNEL_SERVER_DIR}/node_modules/@modelcontextprotocol" ]]; then
  npm ci --prefix "$CHANNEL_SERVER_DIR" --omit=dev --silent
fi

CMD=(claude --dangerously-load-development-channels "server:${CHANNEL}")
if [[ -n "$ALLOWED_TOOLS" ]]; then
  CMD+=(--allowedTools "$ALLOWED_TOOLS")
fi
case "${TAKOPI_CLAUDE_SKIP_PERMISSIONS:-false}" in
  1|true|yes|on)
    if [[ ${EUID:-$(id -u)} -eq 0 ]]; then
      echo "warning: --dangerously-skip-permissions is not supported by Claude Code under root; using allowedTools only" >&2
    else
      CMD+=(--dangerously-skip-permissions)
    fi
    ;;
esac
case "$RESUME" in
  ""|none|false|off)
    ;;
  latest)
    RESUME_ID=$(latest_session_id || true)
    if [[ -n "${RESUME_ID:-}" ]]; then
      CMD+=(--resume "$RESUME_ID")
    fi
    ;;
  continue)
    CMD+=(--continue)
    ;;
  *)
    CMD+=(--resume "$RESUME")
    ;;
esac

printf -v CMD_TEXT "%q " "${CMD[@]}"

tmux kill-session -t "$SESSION" 2>/dev/null || true
tmux new-session -d -s "$SESSION" -c "$WORKDIR" "$CMD_TEXT"
# Fresh project sessions can prompt first for MCP server trust, then for
# development channels. Confirm both so the channel server actually starts.
sleep "${TAKOPI_CLAUDE_CHANNEL_CONFIRM_DELAY:-3}"
tmux send-keys -t "$SESSION" Enter || true
sleep "${TAKOPI_CLAUDE_CHANNEL_CONFIRM_DELAY:-3}"
tmux send-keys -t "$SESSION" Enter || true

while tmux has-session -t "$SESSION" 2>/dev/null; do
  sleep 5
done
exit 1
