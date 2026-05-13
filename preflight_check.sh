#!/bin/bash
# =============================================================
# Vesper Capital — Pre-flight Check
# Run this BEFORE the first manual notebook execution.
# Every check either prints OK or a specific fix command.
# =============================================================

BASE=/home/colin/projects/blockchain_alpha/vesper_capital
DATA_ROOT=/home/colin/projects/blockchain_alpha  # large data files live here, outside repo
PASS=0; FAIL=0

ok()   { echo "  ✅  $1"; ((PASS++)); }
fail() { echo "  ❌  $1"; echo "     FIX: $2"; ((FAIL++)); }
hdr()  { echo; echo "── $1 ──────────────────────────────────────────"; }

echo "============================================================"
echo "  VESPER CAPITAL — PRE-FLIGHT CHECK"
echo "  $(date -u '+%Y-%m-%d %H:%M UTC')"
echo "============================================================"

# ── 1. Project structure ──────────────────────────────────────
hdr "Project structure"

[ -d "$BASE" ] \
  && ok  "Project root exists: $BASE" \
  || fail "Project root missing" "mkdir -p $BASE"

for f in \
  "notebooks/live_signal_monitor.ipynb" \
  "notebooks/claude_refactor_core_utils.py"
do
  [ -f "$BASE/$f" ] \
    && ok  "$f" \
    || fail "$f not found" "cp <source> $BASE/$f"
done

# Large data files live outside the repo
for f in \
  "data/balance_store_hourly_top200.csv" \
  "data/hourly_block_map.csv"
do
  [ -f "$DATA_ROOT/$f" ] \
    && ok  "$f (outside repo — correct)" \
    || fail "$f not found" "Copy validated baseline to $DATA_ROOT/$f"
done

[ -d "$BASE/data/live_signals" ] \
  && ok  "data/live_signals/ exists" \
  || { mkdir -p "$BASE/data/live_signals" && ok "data/live_signals/ created"; }

# ── 2. Git / GitHub ───────────────────────────────────────────
hdr "Git & GitHub"

git -C "$BASE" rev-parse --is-inside-work-tree &>/dev/null \
  && ok  "Git repo initialised" \
  || fail "Not a git repo" "cd $BASE && git init"

REMOTE=$(git -C "$BASE" remote get-url origin 2>/dev/null)
if echo "$REMOTE" | grep -q "vesper_capital"; then
  ok "Remote points to vesper_capital ($REMOTE)"
else
  fail "Remote missing or wrong: '$REMOTE'" \
       "git -C $BASE remote add origin git@github.com:colinmchugh33/vesper_capital.git"
fi

# Test push auth (dry-run)
git -C "$BASE" ls-remote origin &>/dev/null \
  && ok  "GitHub auth works (SSH/token)" \
  || fail "Cannot reach GitHub remote — push will fail" \
          "Ensure SSH key is in ~/.ssh and added to GitHub, or set GITHUB_TOKEN"

# Check git user config
git -C "$BASE" config user.email &>/dev/null \
  && ok  "git user.email configured: $(git -C $BASE config user.email)" \
  || fail "git user.email not set — commits will fail" \
          "git config --global user.email 'you@example.com'"

git -C "$BASE" config user.name &>/dev/null \
  && ok  "git user.name configured: $(git -C $BASE config user.name)" \
  || fail "git user.name not set" \
          "git config --global user.name 'Your Name'"

# ── 3. Python environment ─────────────────────────────────────
hdr "Python environment"

PYTHON=$(which python3 2>/dev/null || which python 2>/dev/null)
[ -n "$PYTHON" ] \
  && ok  "Python found: $PYTHON ($(${PYTHON} --version 2>&1))" \
  || fail "Python not found" "Install Python 3.10+"

JUPYTER=$(which jupyter 2>/dev/null)
[ -n "$JUPYTER" ] \
  && ok  "jupyter found: $JUPYTER" \
  || fail "jupyter not found" "pip install jupyter nbconvert"

NBCONVERT=$(which jupyter-nbconvert 2>/dev/null || jupyter nbconvert --version &>/dev/null && echo "ok")
[ -n "$NBCONVERT" ] \
  && ok  "nbconvert available" \
  || fail "nbconvert not available" "pip install nbconvert"

# Check Python packages
for pkg in xgboost pandas numpy requests; do
  ${PYTHON} -c "import ${pkg}" 2>/dev/null \
    && ok  "Python package: ${pkg}" \
    || fail "Missing package: ${pkg}" "pip install ${pkg}"
done

# ── 4. Alchemy API ────────────────────────────────────────────
hdr "Alchemy API"

API_KEY="8J0Ou3_XsuWVbOiZ0xpEvioGx7k0wab9"
RESP=$(curl -s -X POST "https://eth-mainnet.g.alchemy.com/v2/${API_KEY}" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}' 2>/dev/null)

if echo "$RESP" | grep -q '"result"'; then
  BLOCK=$(echo "$RESP" | python3 -c "import sys,json; d=json.load(sys.stdin); print(int(d['result'],16))" 2>/dev/null)
  ok "Alchemy API reachable — current block: $BLOCK"
else
  fail "Alchemy API not responding" \
       "Check API key is valid at https://dashboard.alchemy.com"
fi

# ── 5. Balance store health ───────────────────────────────────
hdr "Balance store"

BAL_CSV="$DATA_ROOT/data/balance_store_hourly_top200.csv"
if [ -f "$BAL_CSV" ]; then
  ROWS=$(wc -l < "$BAL_CSV")
  LAST=$(tail -1 "$BAL_CSV" | cut -d',' -f1)
  ok "balance_store_hourly_top200.csv: ~$ROWS rows, last entry: $LAST"
else
  fail "balance_store_hourly_top200.csv missing" "Copy validated baseline CSV to $BAL_CSV"
fi

BM_CSV="$DATA_ROOT/data/hourly_block_map.csv"
if [ -f "$BM_CSV" ]; then
  BM_ROWS=$(wc -l < "$BM_CSV")
  ok "hourly_block_map.csv: ~$BM_ROWS rows"
else
  fail "hourly_block_map.csv missing" "Copy block map CSV to $BM_CSV"
fi

# ── 6. Cron wrapper ───────────────────────────────────────────
hdr "Cron wrapper"

WRAPPER="$BASE/run_live_signal.sh"
[ -f "$WRAPPER" ] \
  && ok  "run_live_signal.sh exists" \
  || fail "run_live_signal.sh missing" "Run deploy.sh to create it"

[ -x "$WRAPPER" ] \
  && ok  "run_live_signal.sh is executable" \
  || fail "run_live_signal.sh not executable" "chmod +x $WRAPPER"

CRON_ENTRY=$(crontab -l 2>/dev/null | grep "run_live_signal")
[ -n "$CRON_ENTRY" ] \
  && ok  "Cron entry found: $CRON_ENTRY" \
  || fail "No cron entry for run_live_signal.sh" "Run: crontab -e  and add:  0 8 * * * $WRAPPER"

# ── Summary ───────────────────────────────────────────────────
echo
echo "============================================================"
echo "  RESULT: $PASS passed, $FAIL failed"
echo "============================================================"

if [ "$FAIL" -eq 0 ]; then
  echo
  echo "  All checks passed. You're ready to run:"
  echo
  echo "    jupyter nbconvert --to notebook --execute \\"
  echo "      --output $BASE/notebooks/executed_live_signal.ipynb \\"
  echo "      $BASE/notebooks/live_signal_monitor.ipynb"
  echo
else
  echo
  echo "  Fix the $FAIL issue(s) above before running the notebook."
  echo
fi
