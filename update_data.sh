#!/bin/bash
# Daily incremental data update for sector relative strength dashboard.
# Intended to be run via cron on weekday evenings after US market close.

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
LOG_DIR="$SCRIPT_DIR/logs"

mkdir -p "$LOG_DIR"

echo "========================================" >> "$LOG_DIR/update.log"
echo "Run: $(date '+%Y-%m-%d %H:%M:%S')" >> "$LOG_DIR/update.log"
echo "========================================" >> "$LOG_DIR/update.log"

cd "$SCRIPT_DIR"
python3 fetch_data.py --mode incremental >> "$LOG_DIR/update.log" 2>&1

echo "" >> "$LOG_DIR/update.log"
