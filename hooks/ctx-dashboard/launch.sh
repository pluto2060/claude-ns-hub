#!/usr/bin/env bash
# Launch the CTX dashboard server (idempotent) and open it in the default browser.
# Usage: ctx            # port 8787
#        ctx 9000       # custom port

set -euo pipefail

PORT="${1:-${CTX_DASHBOARD_PORT:-8787}}"
HERE="$HOME/.claude/hooks/ctx-dashboard"
PIDFILE="/tmp/ctx-dashboard.pid"
LOG="/tmp/ctx-dashboard.log"

start_server() {
  CTX_DASHBOARD_PORT="$PORT" nohup python3 "$HERE/server.py" > "$LOG" 2>&1 &
  echo $! > "$PIDFILE"
  sleep 1
}

# If a pidfile exists and the process is alive, reuse it
if [[ -f "$PIDFILE" ]] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
  EXISTING_PID="$(cat "$PIDFILE")"
  if lsof -Pan -p "$EXISTING_PID" -iTCP -sTCP:LISTEN 2>/dev/null | grep -q ":$PORT"; then
    echo "CTX Dashboard already running (pid $EXISTING_PID) → http://127.0.0.1:$PORT"
  else
    echo "Stale pidfile — restarting…"
    kill "$EXISTING_PID" 2>/dev/null || true
    rm -f "$PIDFILE"
    start_server
    echo "CTX Dashboard → http://127.0.0.1:$PORT"
  fi
else
  # Check if port is already LISTENING (ignore stray ESTABLISHED connections
  # from a prior server that has since died)
  if lsof -iTCP:"$PORT" -sTCP:LISTEN >/dev/null 2>&1; then
    echo "Port $PORT is in use by another process. Pick a different port: ctx <port>"
    exit 1
  fi
  start_server
  echo "CTX Dashboard → http://127.0.0.1:$PORT"
fi

# Try to open browser (best-effort, no error if headless)
URL="http://127.0.0.1:$PORT"
WSL_EXPLORER="/mnt/c/Windows/explorer.exe"
if command -v wslview    >/dev/null 2>&1; then wslview    "$URL" >/dev/null 2>&1 || true
elif [[ -x "$WSL_EXPLORER" ]];              then "$WSL_EXPLORER" "$URL" >/dev/null 2>&1 || true
elif command -v xdg-open >/dev/null 2>&1; then xdg-open "$URL" >/dev/null 2>&1 || true
elif command -v open       >/dev/null 2>&1; then open       "$URL" >/dev/null 2>&1 || true
fi

echo "Logs: $LOG    Stop: ctx-stop   (or: kill \$(cat $PIDFILE))"
