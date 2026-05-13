# Vesper Capital — Live Signal Monitor: Operations Runbook

## Overview

The live signal monitor generates a daily ETH directional signal, commits it to GitHub before the 24-hour return resolves, and maintains a cryptographically timestamped forward track record.

**Repository:** https://github.com/colinmchugh33/vesper_capital  
**Cron schedule:** `0 8 * * *` (08:00 UTC daily)  
**Expected runtime:** 3–10 min (first run: ~5 min for model training)

---

## File Map

```
/home/colin/projects/blockchain_alpha/
├── notebooks/
│   ├── live_signal_monitor.ipynb       ← main notebook
│   ├── claude_refactor_core_utils.py   ← pipeline utilities
│   └── executed_live_signal.ipynb      ← last execution output (auto-overwritten)
├── data/
│   ├── balance_store_hourly_top200.csv ← validated 200-wallet baseline (append-only)
│   ├── hourly_block_map.csv            ← block→timestamp map (auto-extended daily)
│   └── live_signals/
│       ├── daily_signals.csv           ← running log of all signals
│       ├── signal_YYYY-MM-DD.md        ← per-day human-readable signal
│       ├── live_model.pkl              ← cached XGBoost model (retrains every 30d)
│       ├── feature_cols.pkl            ← feature column list matching model
│       ├── cron.log                    ← cron execution log
│       └── deploy_dryrun.log           ← first-run log
└── run_live_signal.sh                  ← cron wrapper script
```

---

## First-Time Setup

### Prerequisites

```bash
# Python packages
pip install jupyter nbconvert xgboost pandas numpy requests

# Git identity (required for commits)
git config --global user.email "you@example.com"
git config --global user.name  "Your Name"
```

### SSH / GitHub Auth

The cron job pushes to GitHub non-interactively. One-time setup:

```bash
# Option A: SSH key (recommended)
ssh-keygen -t ed25519 -C "vesper-signal-bot"
cat ~/.ssh/id_ed25519.pub   # paste into GitHub → Settings → SSH keys
ssh -T git@github.com       # should print "Hi colinmchugh33!"

# Option B: HTTPS with token
git -C /home/colin/projects/blockchain_alpha \
  remote set-url origin https://colinmchugh33:<TOKEN>@github.com/colinmchugh33/vesper_capital.git
```

### Deploy

```bash
# 1. Run pre-flight check
bash preflight_check.sh

# 2. Deploy (places files, installs cron, runs first execution)
bash deploy.sh

# 3. Or skip the 5-min dry-run and let cron handle first run
bash deploy.sh --skip-dryrun
```

---

## Daily Operations

### Check last run

```bash
tail -50 /home/colin/projects/blockchain_alpha/data/live_signals/cron.log
```

### Check today's signal

```bash
cat /home/colin/projects/blockchain_alpha/data/live_signals/signal_$(date +%Y-%m-%d).md
```

### View running signal log

```bash
tail /home/colin/projects/blockchain_alpha/data/live_signals/daily_signals.csv
```

### View recent GitHub commits

```bash
git -C /home/colin/projects/blockchain_alpha log --oneline -10
```

Or on GitHub: `https://github.com/colinmchugh33/vesper_capital/commits`

### Manual run (on demand)

```bash
/home/colin/projects/blockchain_alpha/run_live_signal.sh
```

---

## Troubleshooting

### Cron not firing

```bash
# Verify cron entry exists
crontab -l | grep run_live_signal

# Test cron environment explicitly
env -i HOME=$HOME /bin/bash /home/colin/projects/blockchain_alpha/run_live_signal.sh
```

Cron has a stripped PATH — `run_live_signal.sh` uses absolute paths for jupyter, but if it still fails, find your jupyter location and hardcode it:

```bash
which jupyter          # e.g. /home/colin/.local/bin/jupyter
# Then edit run_live_signal.sh: JUPYTER="/home/colin/.local/bin/jupyter"
```

### Notebook fails with import error on `claude_refactor_core_utils`

The notebook runs from the `notebooks/` directory. If the import fails:

```bash
ls /home/colin/projects/blockchain_alpha/notebooks/claude_refactor_core_utils.py
# If missing, restore from your backup or source
```

Alternatively, add to `run_live_signal.sh` before the nbconvert call:
```bash
export PYTHONPATH="/home/colin/projects/blockchain_alpha/notebooks:$PYTHONPATH"
```

### Alchemy API failures / balance fetch errors

```bash
# Test API key directly
curl -s -X POST "https://eth-mainnet.g.alchemy.com/v2/8J0Ou3_XsuWVbOiZ0xpEvioGx7k0wab9" \
  -H "Content-Type: application/json" \
  -d '{"jsonrpc":"2.0","method":"eth_blockNumber","params":[],"id":1}'
# Should return {"result":"0x..."}
```

If rate limited, reduce `max_concurrency` and `rps` in the notebook config cell.

### Git push fails in cron

```bash
# Test push manually (same user as cron)
git -C /home/colin/projects/blockchain_alpha push

# Common causes:
# 1. SSH key not loaded — add to ~/.ssh/config:
#    Host github.com
#      IdentityFile ~/.ssh/id_ed25519
#      AddKeysToAgent yes
# 2. HTTPS token expired — rotate token and update remote URL
# 3. Nothing to commit — harmless, signal already logged locally
```

### Block map gap on first run

On the first run after a long gap, the notebook auto-extends `hourly_block_map.csv` by interpolating ~300 blocks/hour from the last known entry to the current block. This is logged:

```
Block map extended by Xh to <timestamp>
```

This is expected behaviour. The interpolated block numbers are used only for Alchemy `eth_getBalance` at-block queries — small interpolation error is acceptable.

### Model retrain every 30 days

`live_model.pkl` mtime is checked each run. When it's ≥ 30 days old, the full training loop re-runs on `balance_store_hourly_top200.csv` + CryptoCompare price data. This adds ~5 min to that day's run — normal behaviour.

### Duplicate signals in CSV

The CSV deduplicates on `timestamp` (keep last) so re-runs on the same hour overwrite rather than stack. This is intentional.

---

## Signal CSV Schema

| Column | Type | Description |
|---|---|---|
| `timestamp` | ISO datetime | Hour the signal was generated for |
| `generated_at` | ISO datetime | Actual wall-clock time of generation |
| `eth_close` | float | ETH/USD close at signal timestamp |
| `predicted_24h_return` | float | Model output (%) |
| `threshold` | float | Fire threshold (default 0.50%) |
| `signal_fires` | bool | True if pred > threshold |
| `direction` | str | LONG or FLAT |
| `pool_buy_fraction` | float | Fraction of wallets buying this hour |
| `pool_net_pressure` | float | Net ETH flow normalised by volume |
| `wallet_count` | int | Wallets in pool this run |
| `model_version` | str | e.g. `v1_200wallet_baseline` |

---

## Track Record Milestones

| Days | Signals (≈20% fire rate) | Status |
|---|---|---|
| 30 | ~6 active signals | Early forward evidence |
| 90 | ~18 active signals | Statistically meaningful |
| 180 | ~36 active signals | Credible for commercialisation |

Each GitHub commit provides cryptographic proof the prediction preceded the 24-hour resolution window. Commit hash + timestamp is verifiable by any third party.

---

## Config Reference

Defined in Cell 1 of the notebook:

```python
ALCHEMY_API_KEY = "8J0Ou3_XsuWVbOiZ0xpEvioGx7k0wab9"
MIN_THRESHOLD   = 0.50    # signal fires if predicted return > 0.50%
TRAIN_START     = date(2023, 1, 1)
TRAIN_END       = date(2024, 12, 31)
MIN_DELTA_ETH   = 0.005
ROLLING_WINDOWS = [6, 24, 72, 168]
MODEL_PARAMS    = dict(
    n_estimators=200, max_depth=3, learning_rate=0.02,
    subsample=0.6, colsample_bytree=0.3, min_child_weight=50,
    reg_alpha=1.0, reg_lambda=5.0, random_state=42,
    n_jobs=-1, verbosity=0
)
```

To change the fire threshold without retraining, edit `MIN_THRESHOLD` — the model prediction is continuous; the threshold is just a filter on output.

---

*Last updated: auto-generated by deploy setup*
