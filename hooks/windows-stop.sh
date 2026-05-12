#!/bin/bash
# Windows WinForms Notification - Stop hook (blocking until user closes)

# When invoked by the OffClaw Telegram bridge, exit immediately (avoid blocking)
if [ "${OFFCLAW_BRIDGE:-}" = "1" ]; then
    exit 0
fi

input=$(cat 2>/dev/null)
transcript_path=$(echo "$input" | jq -r '.transcript_path // empty' 2>/dev/null)
current_path=$(echo "$input" | jq -r '.cwd // empty' 2>/dev/null)
[ -z "$current_path" ] && current_path="$(pwd)"
dir_name=$(basename "$current_path")
current_time=$(date +"%H:%M")

# Project index: pos-map by dir_name (most reliable) → zellij session → tmux window index
# M110: dir_name lookup takes priority to fix inherited TMUX env showing wrong window index
POS_MAP="${HOME}/.config/celi-pos-map.conf"
proj_num=""
# 1. First priority: exact pos-map match by directory name (works for exec sessions too)
dir_pos=$(grep -i "${dir_name}" "$POS_MAP" 2>/dev/null | grep -v '^#' | head -1 | cut -d= -f2)
if [ -n "$dir_pos" ]; then
    proj_num=$(printf '%02d' "$((10#${dir_pos}))")
elif [ -n "$ZELLIJ_SESSION_NAME" ]; then
    # 2. Fallback: exact zellij session name match in pos-map
    pos_num=$(grep "^${ZELLIJ_SESSION_NAME}=" "$POS_MAP" 2>/dev/null | cut -d= -f2)
    [ -n "$pos_num" ] && proj_num=$(printf '%02d' "$((10#${pos_num}))")
elif [ -n "$TMUX" ]; then
    # 3. Last resort: tmux window index (unreliable in exec sessions — inherited env)
    proj_num=$(tmux display-message -p '#I' 2>/dev/null | awk '{printf "%02d", $1}')
fi
if [ -n "$transcript_path" ]; then
    proj_key=$(basename "$(dirname "$transcript_path")")
else
    proj_key="$dir_name"
fi
[ -n "$proj_num" ] && dir_name="[$proj_num] ${dir_name}"

last_activity="Work complete"
if git rev-parse --git-dir > /dev/null 2>&1; then
    changed=$(git status --short 2>/dev/null | awk '{print $NF}' | xargs -I{} basename {} 2>/dev/null | head -4 | tr '\n' ' ' | sed 's/ $//')
    [ -n "$changed" ] && last_activity="$changed"
fi

# Remote notify via Tailscale direct POST to each online Windows client on port 6789.
# Activate by exporting CLAUDE_REMOTE_NOTIFY_URL (any non-empty value enables it).
# Set CLAUDE_REMOTE_NOTIFY_ONLY=1 to skip the local WinForms popup on a headless host.
if [ -n "${CLAUDE_REMOTE_NOTIFY_URL:-}" ]; then
    (
        payload=$(jq -cn --arg t "$dir_name" --arg m "$last_activity  $current_time" --arg k "stop" \
            '{title:$t,message:$m,kind:$k,sound:true}')
        # Enumerate online Windows tailnet peers and POST directly — single common port 6789
        _notify_token=$(cat "$HOME/.claude/.notify-secret" 2>/dev/null || echo "")
        {
            tailscale status --json 2>/dev/null | \
                jq -r '.Peer[] | select(.Online==true and (.OS=="windows")) | .TailscaleIPs[0]' 2>/dev/null
            # Fallback: static peer list for Windows machines not visible in Tailscale peers
            grep -v '^#' "$HOME/.claude/.windows-peers" 2>/dev/null | awk '{print $1}'
        } | sort -u | while read -r ip; do
            [ -n "$ip" ] && curl -sf -m 5 -X POST "http://${ip}:6789/notify" \
                 -H 'Content-Type: application/json' \
                 -H "X-Notify-Token: ${_notify_token}" \
                 --data "$payload" >/dev/null 2>&1 || true
        done
        # Bark (iPhone + iPad)
        while IFS= read -r _bark_key; do
            [ -z "$_bark_key" ] && continue
            bark_payload=$(jq -cn --arg k "$_bark_key" --arg t "$dir_name" --arg b "$last_activity  $current_time" \
                '{device_key:$k,title:$t,body:$b,sound:"default"}')
            curl -sf -m 8 -X POST "https://api.day.app/push" \
                -H 'Content-Type: application/json' \
                --data "$bark_payload" >/dev/null 2>&1 || true
        done < "$HOME/.claude/.bark-keys"
    ) </dev/null &
    disown 2>/dev/null || true
fi
if [ "${CLAUDE_REMOTE_NOTIFY_ONLY:-0}" = "1" ]; then
    # still emit the console banner at the end; just skip the WinForms popup
    echo -e "\033[0;32m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m"
    echo -e "\033[0;32m [$dir_name] $last_activity\033[0m"
    echo -e "\033[0;32m $current_time\033[0m"
    echo -e "\033[0;32m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m"
    exit 0
fi

# notify/stop shared slots (up to 10, no overlap)
LOCK_DIR="/tmp/claude-notify-stack"
MAX_SLOTS=10
mkdir -p "$LOCK_DIR"

# Clean stale slots (detect dead PIDs)
for i in $(seq 0 $((MAX_SLOTS-1))); do
    if [ -d "$LOCK_DIR/slot_$i" ]; then
        pid_file="$LOCK_DIR/slot_$i/pid"
        if [ -f "$pid_file" ]; then
            pid=$(cat "$pid_file" 2>/dev/null)
            if [ -z "$pid" ] || ! kill -0 "$pid" 2>/dev/null; then
                rm -f "$LOCK_DIR/slot_$i"/*
                rmdir "$LOCK_DIR/slot_$i" 2>/dev/null
            fi
        else
            rm -f "$LOCK_DIR/slot_$i"/*
            rmdir "$LOCK_DIR/slot_$i" 2>/dev/null
        fi
    fi
done

# Remove any existing popup for the same project (sentinel → PowerShell self-terminates)
for i in $(seq 0 $((MAX_SLOTS-1))); do
    proj_file="$LOCK_DIR/slot_$i/proj"
    if [ -f "$proj_file" ] && [ "$(cat "$proj_file" 2>/dev/null)" = "$proj_key" ]; then
        touch "$LOCK_DIR/slot_$i/close"
        pid=$(cat "$LOCK_DIR/slot_$i/pid" 2>/dev/null)
        [ -n "$pid" ] && kill "$pid" 2>/dev/null
        sleep 0.5
        rm -f "$LOCK_DIR/slot_$i/pid" "$LOCK_DIR/slot_$i/proj" "$LOCK_DIR/slot_$i/close"
        rmdir "$LOCK_DIR/slot_$i" 2>/dev/null
    fi
done

# Acquire a slot
SLOT=$MAX_SLOTS
for i in $(seq 0 $((MAX_SLOTS-1))); do
    if mkdir "$LOCK_DIR/slot_$i" 2>/dev/null; then
        SLOT=$i
        echo $$ > "$LOCK_DIR/slot_$i/pid"
        echo "$proj_key" > "$LOCK_DIR/slot_$i/proj"
        break
    fi
done
[ $SLOT -eq $MAX_SLOTS ] && SLOT=0
SQ_SZ=60
GAP=6
SLOT_OFFSET=$(( SLOT * (SQ_SZ + GAP) ))
display_label="${proj_num:-?}"
CLASS="WinS$$"
WIN_SENTINEL=$(wslpath -w "$LOCK_DIR/slot_$SLOT/close" 2>/dev/null)

# WinForms popup (non-blocking — runs in background)
(
    echo $BASHPID > "$LOCK_DIR/slot_$SLOT/pid"
    trap "rm -f '$LOCK_DIR/slot_$SLOT/pid' '$LOCK_DIR/slot_$SLOT/proj' '$LOCK_DIR/slot_$SLOT/close'; rmdir '$LOCK_DIR/slot_$SLOT' 2>/dev/null" EXIT
    /mnt/c/Windows/System32/WindowsPowerShell/v1.0/powershell.exe -STA -Command "
Add-Type -AssemblyName System.Windows.Forms
Add-Type -AssemblyName System.Drawing
Add-Type @'
using System; using System.Runtime.InteropServices;
public class $CLASS {
    [DllImport(\"Gdi32.dll\")]
    public static extern IntPtr CreateRoundRectRgn(int x1,int y1,int x2,int y2,int cx,int cy);
    [DllImport(\"user32.dll\")]
    public static extern bool SetWindowPos(IntPtr hWnd, IntPtr hInsertAfter, int x, int y, int cx, int cy, uint flags);
}
'@
\$form = New-Object System.Windows.Forms.Form
\$form.FormBorderStyle = 'None'
\$form.BackColor = [System.Drawing.Color]::FromArgb(22,22,35)
\$form.Width = $SQ_SZ; \$form.Height = $SQ_SZ
\$form.Opacity = 0.95; \$form.TopMost = \$true
\$form.ShowInTaskbar = \$false
\$form.StartPosition = 'Manual'
\$form.Cursor = [System.Windows.Forms.Cursors]::Hand
\$sw = [System.Windows.Forms.Screen]::PrimaryScreen.WorkingArea
\$form.Left = [int](\$sw.Width / 8) + $SLOT_OFFSET
\$form.Top  = \$sw.Bottom - $SQ_SZ - [int](\$sw.Height / 16)
\$form.Add_Shown({
    \$rgn = [$CLASS]::CreateRoundRectRgn(0,0,$SQ_SZ,$SQ_SZ,14,14)
    \$form.Region = [System.Drawing.Region]::FromHrgn(\$rgn)
    [$CLASS]::SetWindowPos(\$form.Handle, [IntPtr](-1), 0, 0, 0, 0, 0x0013)
})
\$form.Add_Click({ \$form.Close() })
\$lbl = New-Object System.Windows.Forms.Label
\$lbl.Text = '$display_label'
\$lbl.ForeColor = [System.Drawing.Color]::FromArgb(144,238,144)
\$lbl.BackColor = [System.Drawing.Color]::Transparent
\$lbl.Font = New-Object System.Drawing.Font('Segoe UI',22,[System.Drawing.FontStyle]::Bold)
\$lbl.TextAlign = [System.Drawing.ContentAlignment]::MiddleCenter
\$lbl.Location = '0,0'; \$lbl.Size = '$SQ_SZ,$SQ_SZ'
\$lbl.Cursor = [System.Windows.Forms.Cursors]::Hand
\$lbl.Add_Click({ \$form.Close() })
\$form.Controls.Add(\$lbl)
[System.Media.SystemSounds]::Asterisk.Play()
\$topTimer = New-Object System.Windows.Forms.Timer
\$topTimer.Interval = 500
\$topTimer.Add_Tick({
    [$CLASS]::SetWindowPos(\$form.Handle, [IntPtr](-1), 0, 0, 0, 0, 0x0013)
    if ([System.IO.File]::Exists('$WIN_SENTINEL')) { \$form.Close() }
})
\$topTimer.Start()
\$startTime = [DateTime]::Now
\$fadeTimer = New-Object System.Windows.Forms.Timer
\$fadeTimer.Interval = 1000
\$fadeTimer.Add_Tick({
    \$elapsed = ([DateTime]::Now - \$startTime).TotalSeconds
    if (\$elapsed -ge 1800) { \$form.Opacity = 0.40; \$fadeTimer.Stop() }
    else { \$form.Opacity = 0.95 - (0.55 * [Math]::Sqrt(\$elapsed / 1800)) }
})
\$fadeTimer.Start()
[System.Windows.Forms.Application]::Run(\$form)
" 2>/dev/null
) </dev/null &
CHILD_PID=$!
echo $CHILD_PID > "$LOCK_DIR/slot_$SLOT/pid"
disown $CHILD_PID

# Console notification
echo -e "\033[0;32m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m"
echo -e "\033[0;32m [$dir_name] $last_activity\033[0m"
echo -e "\033[0;32m $current_time\033[0m"
echo -e "\033[0;32m━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━━\033[0m"
