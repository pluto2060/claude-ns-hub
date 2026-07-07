#!/usr/bin/env bash
# task-worker.sh — inotify-based Claude task queue worker
# Watches task-queue/*.md → runs claude --print → writes results/
# Start: bash ~/.hub/task-worker.sh &

set -euo pipefail

QUEUE_DIR="$HOME/.hub/task-queue"
RESULT_DIR="$HOME/.hub/task-results"
LOCK_DIR="$HOME/.hub/task-locks"
LOG_FILE="$HOME/.hub/worker.log"
PID_FILE="/tmp/hub-task-worker.pid"

mkdir -p "$QUEUE_DIR" "$RESULT_DIR" "$LOCK_DIR"
echo $$ > "$PID_FILE"

log() { echo "[$(date -Iseconds)] $*" | tee -a "$LOG_FILE"; }

process_task() {
    local task_file="$1"
    local task_id
    task_id=$(basename "$task_file" .md)
    local lock="$LOCK_DIR/$task_id.lock"
    local result="$RESULT_DIR/$task_id.json"

    # Skip if already processing or done
    [ -f "$lock" ] && return
    [ -f "$result" ] && return

    # Claim the task
    touch "$lock"
    log "Processing task: $task_id"

    # Run claude --print
    local exit_code=0
    local output
    output=$(claude --print --dangerously-skip-permissions \
        --no-session-persistence \
        < "$task_file" 2>&1) || exit_code=$?

    # Save result
    printf '%s' "$output" | python3 -c "
import sys, json
from datetime import datetime
output = sys.stdin.read()
result = {
    'task_id': '$task_id',
    'status': 'done' if $exit_code == 0 else 'failed',
    'exit_code': $exit_code,
    'output': output,
    'completed_at': datetime.now().isoformat()
}
print(json.dumps(result, ensure_ascii=False, indent=2))
" > "$result"

    rm -f "$lock"
    log "Task $task_id done (exit=$exit_code)"
}

log "Task worker started (PID=$$), watching $QUEUE_DIR"

# Process any existing pending tasks on startup
for f in "$QUEUE_DIR"/*.md; do
    [ -f "$f" ] && process_task "$f" &
done

# Watch for new tasks
inotifywait -m -e close_write --format '%w%f' "$QUEUE_DIR" 2>/dev/null | \
while IFS= read -r file; do
    [[ "$file" == *.md ]] && process_task "$file" &
done
