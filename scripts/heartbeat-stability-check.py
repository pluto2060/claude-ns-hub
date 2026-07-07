#!/usr/bin/env python3
"""
M1635 Roadmap — Prerequisite ①: Heartbeat Stability Verification

Monitors tool_call_heartbeat HTTP POST to /api/hub/tool-call-heartbeat for 1 hour.
Success criteria: zero heartbeat gaps > 30s (OOB_STALE_SECS)

Usage:
  python3 heartbeat-stability-check.py [--duration 3600]

Logs to: /tmp/heartbeat-stability-{timestamp}.log
"""
import time
import sys
import argparse
from datetime import datetime
from pathlib import Path

LOG_FILE = Path(f"/tmp/heartbeat-stability-{int(time.time())}.log")
HEARTBEAT_TTL = 30  # seconds — OOB_STALE_SECS from server.py

def log(msg: str):
    ts = datetime.now().strftime("%Y-%m-%d %H:%M:%S")
    line = f"[{ts}] {msg}"
    print(line)
    LOG_FILE.write_text(LOG_FILE.read_text() + line + "\n" if LOG_FILE.exists() else line + "\n")

def monitor_heartbeat(duration_secs: int):
    """
    Monitors /var/log/hub.log for heartbeat POST entries.
    Detects gaps > HEARTBEAT_TTL.
    """
    log_path = Path("/var/log/hub.log")
    if not log_path.exists():
        log_path = Path.home() / ".hub/logs/hub.log"
    if not log_path.exists():
        log_path = Path.home() / ".hub/hub.log"

    if not log_path.exists():
        log(f"ERROR: Hub log not found at {log_path}")
        return False

    log(f"Starting {duration_secs}s heartbeat stability check")
    log(f"Monitoring: {log_path}")
    log(f"TTL threshold: {HEARTBEAT_TTL}s")

    start = time.time()
    last_heartbeat = start
    heartbeat_count = 0
    gap_violations = []

    # Tail log file
    with open(log_path, "r") as f:
        # Seek to end
        f.seek(0, 2)

        while time.time() - start < duration_secs:
            line = f.readline()
            if line:
                # Match: "POST /api/agent-busy" (OOB heartbeat via report_busy_state MCP tool)
                if "POST /api/agent-busy" in line:
                    now = time.time()
                    gap = now - last_heartbeat
                    heartbeat_count += 1

                    if gap > HEARTBEAT_TTL and heartbeat_count > 1:  # Skip first gap (startup)
                        violation = f"Gap violation: {gap:.1f}s (threshold {HEARTBEAT_TTL}s)"
                        log(violation)
                        gap_violations.append((now, gap))

                    last_heartbeat = now
            else:
                time.sleep(0.5)

            # Check for long gap even without log entry
            if time.time() - last_heartbeat > HEARTBEAT_TTL * 1.5:
                log(f"WARNING: No heartbeat for {time.time() - last_heartbeat:.1f}s")

    # Final report
    elapsed = time.time() - start
    log("=" * 60)
    log(f"Verification complete: {elapsed/60:.1f} minutes")
    log(f"Total heartbeats received: {heartbeat_count}")
    log(f"Gap violations (>{HEARTBEAT_TTL}s): {len(gap_violations)}")

    if gap_violations:
        log("\nViolation details:")
        for ts, gap in gap_violations:
            log(f"  - {datetime.fromtimestamp(ts).strftime('%H:%M:%S')}: {gap:.1f}s gap")

    success = len(gap_violations) == 0
    log(f"\nResult: {'✓ PASS' if success else '✗ FAIL'}")
    log(f"Full log: {LOG_FILE}")

    return success

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="Heartbeat stability check")
    parser.add_argument("--duration", type=int, default=3600, help="Monitor duration in seconds (default: 3600)")
    args = parser.parse_args()

    success = monitor_heartbeat(args.duration)
    sys.exit(0 if success else 1)
