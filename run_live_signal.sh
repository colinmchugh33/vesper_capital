#!/bin/bash
# =============================================================
# Vesper Capital — Daily Signal Cron Wrapper
# Runs live_signal_monitor.ipynb and logs output.
# Install path: /home/colin/projects/blockchain_alpha/vesper_capital/run_live_signal.sh
# Crontab line: 0 8 * * * /home/colin/projects/blockchain_alpha/vesper_capital/run_live_signal.sh
# =============================================================

# ── Absolute paths (cron has a minimal PATH) ─────────────────
BASE_DIR="/home/colin/projects/blockchain_alpha/vesper_capital"
NOTEBOOK="$BASE_DIR/notebooks/live_signal_monitor.ipynb"
EXECUTED="$BASE_DIR/notebooks/executed_live_signal.ipynb"
LOG_DIR="$BASE_DIR/data/live_signals"
LOG_FILE="$LOG_DIR/cron.log"
LOCK_FILE="/tmp/vesper_live_signal.lock"

# Locate jupyter — check common install locations
for CANDIDATE in \
  /usr/bin/jupyter \
  /usr/local/bin/jupyter \
  "$HOME/.local/bin/jupyter" \
  "$HOME/miniconda3/bin/jupyter" \
  "$HOME/anaconda3/bin/jupyter"
do
  if [ -x "$CANDIDATE" ]; then
    JUPYTER="$CANDIDATE"
    break
  fi
done

if [ -z "$JUPYTER" ]; then
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] ERROR: jupyter not found. Check install path." >> "$LOG_FILE"
  exit 1
fi

# ── Lock guard: prevents overlapping runs ────────────────────
if [ -f "$LOCK_FILE" ]; then
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] SKIP: previous run still in progress (lock: $LOCK_FILE)" >> "$LOG_FILE"
  exit 0
fi
touch "$LOCK_FILE"
trap "rm -f $LOCK_FILE" EXIT

# ── Setup ────────────────────────────────────────────────────
mkdir -p "$LOG_DIR"
cd "$BASE_DIR" || { echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] ERROR: Cannot cd to $BASE_DIR" >> "$LOG_FILE"; exit 1; }

# Ensure core utils are importable (notebook imports from same dir)
export PYTHONPATH="$BASE_DIR/notebooks:$PYTHONPATH"

echo "" >> "$LOG_FILE"
echo "============================================================" >> "$LOG_FILE"
echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Starting live signal run" >> "$LOG_FILE"
echo "  Jupyter: $JUPYTER" >> "$LOG_FILE"
echo "  Notebook: $NOTEBOOK" >> "$LOG_FILE"

# ── Execute notebook ─────────────────────────────────────────
"$JUPYTER" nbconvert \
  --to notebook \
  --execute \
  --ExecutePreprocessor.timeout=600 \
  --output "$EXECUTED" \
  "$NOTEBOOK" \
  >> "$LOG_FILE" 2>&1

EXIT_CODE=$?

if [ $EXIT_CODE -eq 0 ]; then
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] SUCCESS (exit 0)" >> "$LOG_FILE"
else
  echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] FAILED (exit $EXIT_CODE) — see output above" >> "$LOG_FILE"
  # Keep last 5 executed notebooks for debugging
  for i in 4 3 2 1; do
    [ -f "${EXECUTED%.ipynb}_${i}.ipynb" ] && mv "${EXECUTED%.ipynb}_${i}.ipynb" "${EXECUTED%.ipynb}_$((i+1)).ipynb"
  done
  [ -f "$EXECUTED" ] && cp "$EXECUTED" "${EXECUTED%.ipynb}_1.ipynb"
fi

echo "[$(date -u '+%Y-%m-%d %H:%M:%S UTC')] Run complete" >> "$LOG_FILE"
echo "============================================================" >> "$LOG_FILE"

exit $EXIT_CODE
