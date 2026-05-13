#!/bin/bash
# =============================================================
# Vesper Capital — Deployment Script
# Run once from any directory. Does everything in order:
#   1. Places notebook + wrapper at correct paths
#   2. Verifies git remote
#   3. Creates live_signals/ dir
#   4. Installs crontab entry
#   5. Does a dry-run notebook execution to confirm setup
#
# Usage:
#   bash deploy.sh [--skip-dryrun]
# =============================================================

set -e

BASE="/home/colin/projects/blockchain_alpha/vesper_capital"
NB_SRC="$(dirname "$0")/live_signal_monitor.ipynb"   # adjust if needed
WRAPPER_SRC="$(dirname "$0")/run_live_signal.sh"

SKIP_DRYRUN=false
[[ "${1:-}" == "--skip-dryrun" ]] && SKIP_DRYRUN=true

log() { echo "[deploy] $*"; }
die() { echo "[deploy] ERROR: $*" >&2; exit 1; }

# ── 1. Directories ────────────────────────────────────────────
log "Creating directory structure..."
mkdir -p "$BASE/data/live_signals"
mkdir -p "$BASE/notebooks"

# ── 2. Place notebook ─────────────────────────────────────────
NOTEBOOK="$BASE/notebooks/live_signal_monitor.ipynb"
if [ -f "$NB_SRC" ]; then
  cp "$NB_SRC" "$NOTEBOOK"
  log "Notebook placed at $NOTEBOOK"
elif [ -f "$NOTEBOOK" ]; then
  log "Notebook already at $NOTEBOOK — skipping copy"
else
  die "live_signal_monitor.ipynb not found. Place it next to deploy.sh or at $NOTEBOOK"
fi

# ── 3. Place cron wrapper ─────────────────────────────────────
WRAPPER="$BASE/run_live_signal.sh"
if [ -f "$WRAPPER_SRC" ]; then
  cp "$WRAPPER_SRC" "$WRAPPER"
  chmod +x "$WRAPPER"
  log "Wrapper placed and made executable: $WRAPPER"
elif [ -f "$WRAPPER" ]; then
  chmod +x "$WRAPPER"
  log "Wrapper already at $WRAPPER — ensured executable"
else
  die "run_live_signal.sh not found. Place it next to deploy.sh."
fi

# ── 4. Verify git remote ──────────────────────────────────────
log "Checking git remote..."
cd "$BASE"

if ! git rev-parse --is-inside-work-tree &>/dev/null; then
  log "Initialising git repo..."
  git init
fi

REMOTE=$(git remote get-url origin 2>/dev/null || true)
if [ -z "$REMOTE" ]; then
  log "No remote configured. Adding vesper_capital..."
  git remote add origin git@github.com:colinmchugh33/vesper_capital.git
elif echo "$REMOTE" | grep -q "vesper_capital"; then
  log "Remote OK: $REMOTE"
else
  log "WARNING: Remote is '$REMOTE', expected vesper_capital repo. Update manually if needed."
fi

# Ensure git user is configured (required for commits in cron)
if ! git config user.email &>/dev/null; then
  log "Setting placeholder git user (update to your real identity)..."
  git config user.email "signal-bot@vespercapital.io"
  git config user.name  "Vesper Signal Bot"
fi

# ── 5. Install crontab ────────────────────────────────────────
log "Installing cron job (daily at 08:00 UTC)..."

CRON_LINE="0 8 * * * $WRAPPER"
EXISTING=$(crontab -l 2>/dev/null || true)

if echo "$EXISTING" | grep -qF "$WRAPPER"; then
  log "Cron entry already present — no change."
else
  (echo "$EXISTING"; echo "$CRON_LINE") | crontab -
  log "Cron entry installed: $CRON_LINE"
fi

log "Current crontab:"
crontab -l | grep -v '^#' | grep -v '^$' | sed 's/^/  /'

# ── 6. Dry-run notebook execution ────────────────────────────
if $SKIP_DRYRUN; then
  log "Skipping dry-run (--skip-dryrun flag set)."
else
  log "Running notebook dry-run (this trains the model — ~5 min)..."

  JUPYTER=$(which jupyter 2>/dev/null || true)
  [ -z "$JUPYTER" ] && die "jupyter not found. Install with: pip install jupyter nbconvert"

  LOG="$BASE/data/live_signals/deploy_dryrun.log"
  "$JUPYTER" nbconvert \
    --to notebook \
    --execute \
    --ExecutePreprocessor.timeout=600 \
    --output "$BASE/notebooks/executed_live_signal.ipynb" \
    "$NOTEBOOK" \
    2>&1 | tee "$LOG"

  if [ "${PIPESTATUS[0]}" -eq 0 ]; then
    log "Dry-run SUCCEEDED."
    log "Check output files:"
    ls -lh "$BASE/data/live_signals/" 2>/dev/null | sed 's/^/  /'
    log "Check GitHub for committed signal:"
    git -C "$BASE" log --oneline -3 | sed 's/^/  /'
  else
    die "Dry-run FAILED. See $LOG for details."
  fi
fi

# ── Done ──────────────────────────────────────────────────────
echo
echo "================================================================"
echo "  DEPLOYMENT COMPLETE"
echo "================================================================"
echo
echo "  Next run: tomorrow at 08:00 UTC (or run manually):"
echo "    $WRAPPER"
echo
echo "  Monitor logs:"
echo "    tail -f $BASE/data/live_signals/cron.log"
echo
echo "  Verify signals on GitHub:"
echo "    https://github.com/colinmchugh33/vesper_capital/commits"
echo
