"""
core_utils.py
-------------
Utility functions for the blockchain alpha pipeline.

Sections:
  1. Infrastructure
  2. Price data
  3. Block map
  4. Address discovery (Geth)
  5. Balance fetching (Alchemy)
  6. Balance store (incremental persistence)
  7. Wallet log persistence
  8. Backtest utilities
"""

import os
import socket
import time
import asyncio
import requests
import httpx
import numpy as np
import pandas as pd
from datetime import datetime, timedelta, date as _date
from bisect import bisect_left
from collections import defaultdict
from concurrent.futures import ThreadPoolExecutor, as_completed
from itertools import product
from typing import Iterable, Optional, Dict, List, Any, Tuple, Sequence, Callable, Set

import nest_asyncio
nest_asyncio.apply()


# =============================================================================
# 1. INFRASTRUCTURE
# =============================================================================

def is_port_open(host: str, port: int, timeout: float = 1.0) -> bool:
    """
    Check if a port is open. Used to verify the local Geth node is active.

    Inputs:
    -------
    - host : str
    - port : int
    - timeout : float (seconds)

    Outputs:
    --------
    - bool : True if open, False otherwise
    """
    try:
        with socket.create_connection((host, port), timeout=timeout):
            return True
    except OSError:
        return False


# =============================================================================
# 2. PRICE DATA (Yahoo Finance)
# =============================================================================

def load_eth_usd_cryptocompare(
    start: str = '2022-01-01',
    end: Optional[str] = None,
    interval: str = '1d',
    symbol: str = 'ETH',
    currency: str = 'USD',
    api_key: Optional[str] = None
) -> pd.DataFrame:
    """
    Load ETH-USD OHLCV from CryptoCompare public API.
    Free tier: 100k calls/month, no geo-restrictions, full history back to 2015.
    Supports daily, hourly, and minute granularity with proper pagination.
    Primary price data function — replaces load_eth_usd_yahoo.

    Inputs:
    -------
    - start    : str  ('YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS')
    - end      : str or None  (defaults to now)
    - interval : str  '1d' (daily), '1h' (hourly), '1m' (minute)
                      Also accepts: 'day', 'hour', 'minute'
    - symbol   : str  (default 'ETH')
    - currency : str  (default 'USD')
    - api_key  : str or None  (optional — increases rate limits if provided)

    Outputs:
    --------
    - pd.DataFrame with columns:
        ['date', 'open', 'high', 'low', 'close', 'volume']
      where 'date' is:
        - datetime.date  for daily interval
        - pd.Timestamp   for hourly/minute intervals
    """
    import requests
    import time as _time

    # Interval map
    interval_map = {
        '1d': 'histoday',  'day':    'histoday',
        '1h': 'histohour', 'hour':   'histohour',
        '1m': 'histominute','minute': 'histominute',
    }
    endpoint = interval_map.get(str(interval).lower())
    if not endpoint:
        raise ValueError(f"Unknown interval '{interval}'. Use '1d','1h','1m'.")

    if end is None:
        end = datetime.today().strftime('%Y-%m-%d %H:%M:%S')

    start_ts = int(pd.Timestamp(start).timestamp())
    end_ts   = int(pd.Timestamp(end).timestamp())

    base_url = f'https://min-api.cryptocompare.com/data/v2/{endpoint}'
    limit    = 2000  # max per request
    all_rows = []
    cursor   = end_ts

    headers = {}
    if api_key:
        headers['authorization'] = f'Apikey {api_key}'

    while cursor > start_ts:
        params = {
            'fsym':    symbol,
            'tsym':    currency,
            'limit':   limit,
            'toTs':    cursor,
        }
        resp = requests.get(base_url, params=params, headers=headers, timeout=30)
        resp.raise_for_status()
        data = resp.json()

        if data.get('Response') == 'Error':
            raise ValueError(f"CryptoCompare API error: {data.get('Message')}")

        rows = data.get('Data', {}).get('Data', [])
        if not rows:
            break

        # Filter to requested window
        rows_in_window = [r for r in rows if start_ts <= r['time'] <= end_ts]
        all_rows.extend(rows_in_window)

        # Step backward — oldest timestamp in this batch
        oldest_ts = rows[0]['time']
        if oldest_ts <= start_ts:
            break
        if oldest_ts >= cursor:
            break  # no progress — safety exit

        cursor = oldest_ts
        _time.sleep(0.1)

    if not all_rows:
        return pd.DataFrame(columns=['date','open','high','low','close','volume'])

    df = pd.DataFrame(all_rows)
    df['date']   = pd.to_datetime(df['time'], unit='s')
    df['open']   = df['open'].astype(float)
    df['high']   = df['high'].astype(float)
    df['low']    = df['low'].astype(float)
    df['close']  = df['close'].astype(float)
    df['volume'] = df['volumefrom'].astype(float)

    # Daily interval — use date only
    if endpoint == 'histoday':
        df['date'] = df['date'].dt.date

    df = (df[['date','open','high','low','close','volume']]
          .drop_duplicates('date')
          .sort_values('date')
          .reset_index(drop=True))

    return df


def load_eth_usd_yahoo(start: str = '2017-01-01', end: Optional[str] = None) -> pd.DataFrame:
    """
    DEPRECATED — use load_eth_usd_cryptocompare() instead.
    Kept for backward compatibility. Will be removed in a future version.

    load_eth_usd_cryptocompare() provides identical output structure, supports
    daily, hourly, and minute resolution, requires no API key, and has
    complete history back to 2015 with no geo-restrictions.
    """
    import warnings
    warnings.warn(
        "load_eth_usd_yahoo() is deprecated. Use load_eth_usd_cryptocompare() instead. "
        "It provides the same output structure plus hourly/minute resolution.",
        DeprecationWarning, stacklevel=2
    )
    import yfinance as yf

    if end is None:
        end = datetime.today().strftime('%Y-%m-%d')

    start_date = pd.to_datetime(start)
    end_date   = pd.to_datetime(end)
    all_data   = []
    current    = start_date

    while current < end_date:
        chunk_end = min(current + pd.DateOffset(months=1), end_date)
        ticker = yf.Ticker("ETH-USD")
        hist = ticker.history(
            start=current.strftime('%Y-%m-%d'),
            end=chunk_end.strftime('%Y-%m-%d'),
            interval='1d',
            auto_adjust=False
        )
        if not hist.empty:
            df = hist.reset_index()[['Date', 'Open', 'High', 'Low', 'Close', 'Volume']]
            df.columns = ['date', 'open', 'high', 'low', 'close', 'volume']
            df['date'] = pd.to_datetime(df['date']).dt.date
            all_data.append(df)
        current = chunk_end + timedelta(days=1)

    return (pd.concat(all_data)
              .drop_duplicates('date')
              .sort_values('date')
              .reset_index(drop=True))


def identify_large_movements(df: pd.DataFrame, pct_threshold: float = 2.0) -> pd.DataFrame:
    """
    Flag days where the closing price change exceeds pct_threshold (absolute).

    Inputs:
    -------
    - df            : pd.DataFrame with ['date', 'close']
    - pct_threshold : float (e.g. 3.5 for 3.5%)

    Outputs:
    --------
    - pd.DataFrame with added columns:
        ['prior_close', 'price_delta', 'pct_change', 'is_large_movement']
    """
    df = df.sort_values('date').copy()
    df['prior_close']       = df['close'].shift(1)
    df['price_delta']       = df['close'] - df['prior_close']
    df['pct_change']        = (df['price_delta'] / df['prior_close']) * 100
    df['is_large_movement'] = df['pct_change'].abs() >= pct_threshold
    return df


# =============================================================================
# 3. BLOCK MAP (Etherscan V2)
# =============================================================================

V2_BASE = "https://api.etherscan.io/v2/api"


def generate_timestamps(start_date: str, end_date: str, period: str = "day") -> List[datetime]:
    """
    Generate a list of datetime objects between start_date and end_date at the given period.

    Inputs:
    -------
    - start_date : str ('YYYY-MM-DD')
    - end_date   : str ('YYYY-MM-DD')
    - period     : str ('day', 'hour', 'min', 'month')

    Outputs:
    --------
    - list[datetime]
    """
    units = {"min": timedelta(minutes=1), "hour": timedelta(hours=1),
             "day": timedelta(days=1), "month": None}
    assert period in units, f"Invalid period: {period}"

    start = datetime.strptime(start_date, "%Y-%m-%d")
    end   = datetime.strptime(end_date,   "%Y-%m-%d")
    timestamps = []

    if period == "month":
        curr = start
        while curr <= end:
            timestamps.append(curr)
            curr = curr.replace(month=curr.month % 12 + 1,
                                year=curr.year + (1 if curr.month == 12 else 0))
    else:
        delta = units[period]
        curr  = start
        while curr <= end:
            timestamps.append(curr)
            curr += delta

    return timestamps


def get_block_numbers_for_timestamps(
    timestamps: Iterable[pd.Timestamp],
    etherscan_api_key: str,
    *,
    sleep_sec: float = 0.51,
    chain_id: int = 1,
    closest: str = "before",
    max_retries: int = 5,
) -> pd.DataFrame:
    """
    Query Etherscan V2 for block numbers at each provided timestamp.

    Inputs:
    -------
    - timestamps        : Iterable[pd.Timestamp]
    - etherscan_api_key : str
    - sleep_sec         : float  (rate-limit pause between calls)
    - chain_id          : int    (1 = Ethereum mainnet)
    - closest           : str    ('before' or 'after')
    - max_retries       : int

    Outputs:
    --------
    - pd.DataFrame with columns: ['timestamp' (date), 'block_number' (Int64)]
    """
    rows: List[Dict[str, Any]] = []

    for ts_in in timestamps:
        ts = pd.Timestamp(ts_in)
        ts = ts.tz_localize("UTC") if ts.tzinfo is None else ts.tz_convert("UTC")
        epoch = int(ts.timestamp())

        params = {
            "chainid": chain_id,
            "module": "block",
            "action": "getblocknobytime",
            "timestamp": epoch,
            "closest": closest,
            "apikey": etherscan_api_key,
        }

        backoff = max(sleep_sec, 0.25)
        for attempt in range(1, max_retries + 1):
            try:
                r = requests.get(V2_BASE, params=params, timeout=30)
                r.raise_for_status()
                data = r.json()

                if str(data.get("status")) == "1" and "result" in data:
                    rows.append({
                        "timestamp": ts.floor("D").tz_localize(None),
                        "block_number": int(data["result"])
                    })
                    break

                msg = (data.get("message") or "").lower()
                res = (data.get("result") or "")
                if "rate" in msg or "too many" in msg or "rate" in res.lower():
                    time.sleep(backoff)
                    backoff = min(backoff * 2, 8.0)
                    continue

                raise RuntimeError(
                    f"Etherscan V2 error (attempt {attempt}): "
                    f"status={data.get('status')} message={data.get('message')}"
                )

            except (requests.RequestException, ValueError):
                if attempt == max_retries:
                    raise
                time.sleep(backoff)
                backoff = min(backoff * 2, 8.0)

        if sleep_sec:
            time.sleep(sleep_sec)

    out = pd.DataFrame(rows, columns=["timestamp", "block_number"])
    if not out.empty:
        # Preserve full timestamp precision — callers floor to appropriate granularity
        out["timestamp"]    = pd.to_datetime(out["timestamp"], utc=False)
        out["block_number"] = pd.to_numeric(out["block_number"], errors="coerce").astype("Int64")
    return out


def build_or_update_block_map(
    start_date: str,
    end_date: str,
    *,
    period: str = "day",
    etherscan_api_key: str,
    existing_csv_path: Optional[str] = None,
    sleep_sec: float = 0.51,
    write_back: bool = True,
    return_map: bool = False,
    chain_id: int = 1,
    closest: str = "before",
    max_retries: int = 5,
):
    """
    Build or extend a timestamp -> block_number map using Etherscan V2.
    Only queries Etherscan for timestamps not already in the existing CSV.
    Supports daily and hourly resolution.

    Inputs:
    -------
    - start_date        : str ('YYYY-MM-DD' or 'YYYY-MM-DD HH:MM:SS')
    - end_date          : str
    - period            : str ('day' or 'hour')
    - etherscan_api_key : str
    - existing_csv_path : str or None
    - sleep_sec         : float
    - write_back        : bool
    - return_map        : bool  (if True, return dict[timestamp -> int])
    - chain_id          : int
    - closest           : str
    - max_retries       : int

    Outputs:
    --------
    - pd.DataFrame with ['timestamp', 'block_number']
      OR dict[timestamp -> int] if return_map=True
    """
    period = period.lower()
    if period not in ('day', 'hour'):
        raise ValueError("period must be 'day' or 'hour'")

    freq       = 'h' if period == 'hour' else 'D'
    floor_freq = 'h' if period == 'hour' else 'D'

    start = pd.to_datetime(start_date)
    end   = pd.to_datetime(end_date)
    if end < start:
        raise ValueError("end_date must be on/after start_date.")

    required_ts = pd.date_range(start=start, end=end, freq=freq)

    if existing_csv_path and os.path.exists(existing_csv_path):
        existing_df = pd.read_csv(existing_csv_path)
        print(f"Warm-start: loaded {len(existing_df)} rows from {existing_csv_path}")
    else:
        existing_df = pd.DataFrame(columns=["timestamp", "block_number"])

    if not existing_df.empty:
        existing_df["timestamp"] = (
            pd.to_datetime(existing_df["timestamp"], errors="coerce", utc=True)
              .dt.tz_convert(None).dt.floor(floor_freq)
        )
        existing_df["block_number"] = (
            pd.to_numeric(existing_df["block_number"], errors="coerce").astype("Int64")
        )
        existing_df = existing_df.dropna(subset=["timestamp"])

    # Build set of already-covered timestamps at the right granularity
    have_ts = set(existing_df["timestamp"].dt.floor(floor_freq)) if not existing_df.empty else set()
    need_ts_raw = [ts for ts in required_ts if ts.floor(floor_freq) not in have_ts]

    if need_ts_raw:
        print(f"Fetching {len(need_ts_raw)} missing {period} timestamps from Etherscan...")
        need_ts_utc = [pd.Timestamp(ts).tz_localize("UTC") for ts in need_ts_raw]
        new_df = get_block_numbers_for_timestamps(
            need_ts_utc, etherscan_api_key,
            sleep_sec=sleep_sec, chain_id=chain_id,
            closest=closest, max_retries=max_retries,
        )
        # Preserve sub-daily precision for hourly mode
        if period == 'hour' and not new_df.empty:
            new_df["timestamp"] = pd.to_datetime(
                new_df["timestamp"], errors="coerce", utc=True
            ).dt.tz_convert(None).dt.floor('h')
    else:
        print("Block map is up to date — no Etherscan queries needed.")
        new_df = pd.DataFrame(columns=["timestamp", "block_number"])

    combined = pd.concat([existing_df, new_df], ignore_index=True)

    if not combined.empty:
        combined["timestamp"] = (
            pd.to_datetime(combined["timestamp"], errors="coerce", utc=True)
              .dt.tz_convert(None).dt.floor(floor_freq)
        )
        combined["block_number"] = (
            pd.to_numeric(combined["block_number"], errors="coerce").astype("Int64")
        )
        combined = (combined
                    .dropna(subset=["timestamp"])
                    .drop_duplicates(subset=["timestamp"], keep="last")
                    .sort_values("timestamp")
                    .reset_index(drop=True))

        # For daily period, store as date only for backward compatibility
        if period == 'day':
            combined["timestamp"] = combined["timestamp"].dt.date

    if write_back and existing_csv_path:
        combined.to_csv(existing_csv_path, index=False)
        print(f"Block map written to {existing_csv_path} ({len(combined)} rows)")

    if return_map:
        return {ts: int(bn) for ts, bn in zip(combined["timestamp"], combined["block_number"])}
    return combined


# =============================================================================
# 4. ADDRESS DISCOVERY (Alchemy — no local node required)
# =============================================================================

def get_movement_day_block_ranges(
    df_with_flags: pd.DataFrame,
    block_map: dict,
    day_lag: int = 1
) -> pd.DataFrame:
    """
    For each large-movement day, find the block range for the prior day.

    Inputs:
    -------
    - df_with_flags : pd.DataFrame  with ['date', 'is_large_movement']
    - block_map     : dict          {datetime.date -> block_number}
    - day_lag       : int           (blocks before movement day to stop)

    Outputs:
    --------
    - pd.DataFrame with ['movement_date', 'pre_date', 'start_block', 'end_block']
    """
    df_with_flags = df_with_flags.copy()
    df_with_flags['date'] = pd.to_datetime(df_with_flags['date']).dt.date
    results = []

    for movement_date in df_with_flags[df_with_flags['is_large_movement']]['date']:
        pre_date = movement_date - timedelta(days=1)
        try:
            start_block = block_map[pre_date]
            end_block   = block_map[movement_date] - day_lag
            results.append({
                'movement_date': movement_date,
                'pre_date':      pre_date,
                'start_block':   start_block,
                'end_block':     end_block
            })
        except KeyError as e:
            print(f"Skipping {pre_date}: missing block info ({e})")

    return pd.DataFrame(results)


def _fetch_eth_transfers_for_block_range(
    start_block: int,
    end_block: int,
    alchemy_api_key: str,
    max_retries: int = 3,
    retry_backoff: float = 0.5,
) -> List[dict]:
    """
    Fetch all external ETH transfers in a block range via alchemy_getAssetTransfers.
    Handles pagination automatically via pageKey.

    Inputs:
    -------
    - start_block     : int
    - end_block       : int
    - alchemy_api_key : str
    - max_retries     : int
    - retry_backoff   : float

    Outputs:
    --------
    - list of transfer dicts, each containing at minimum:
        {'from': str, 'to': str, 'value': float (ETH)}
    """
    url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_api_key}"
    transfers = []
    page_key = None

    while True:
        params = {
            "fromBlock": hex(start_block),
            "toBlock":   hex(end_block),
            "category":  ["external"],
            "excludeZeroValue": True,
            "maxCount":  "0x3e8",  # 1000 per page (max)
        }
        if page_key:
            params["pageKey"] = page_key

        payload = {
            "jsonrpc": "2.0",
            "id": 1,
            "method": "alchemy_getAssetTransfers",
            "params": [params]
        }

        for attempt in range(max_retries + 1):
            try:
                r = requests.post(url, json=payload, timeout=30)
                if r.status_code == 429:
                    time.sleep(retry_backoff * (2 ** attempt))
                    continue
                r.raise_for_status()
                data = r.json()

                if "error" in data:
                    raise RuntimeError(f"Alchemy error: {data['error']}")

                result = data.get("result", {})
                transfers.extend(result.get("transfers", []))
                page_key = result.get("pageKey")
                break

            except (requests.RequestException, RuntimeError) as e:
                if attempt == max_retries:
                    print(f"  Failed after {max_retries} retries: {e}")
                    return transfers
                time.sleep(retry_backoff * (2 ** attempt))

        if not page_key:
            break  # all pages exhausted

    return transfers


def _aggregate_address_roles_from_transfers(transfers: List[dict]) -> dict:
    """
    Aggregate per-address tx counts and total ETH value from a list of transfers.
    Produces the same schema as the old Geth-based address_roles dict.

    Inputs:
    -------
    - transfers : list of dicts from alchemy_getAssetTransfers
        Each transfer has 'from', 'to', 'value' (ETH float)

    Outputs:
    --------
    - dict[address] = {'sent': int, 'received': int, 'value_sent': int, 'value_received': int}
      Note: value_sent/value_received are in wei (int) to match downstream filter expectations
    """
    addr_stats: Dict[str, dict] = {}

    for tx in transfers:
        sender    = (tx.get("from") or "").lower().strip()
        recipient = (tx.get("to")   or "").lower().strip()
        value_eth = float(tx.get("value") or 0.0)
        value_wei = int(value_eth * 1e18)

        if sender:
            s = addr_stats.setdefault(sender, {'sent': 0, 'received': 0, 'value_sent': 0, 'value_received': 0})
            s['sent']       += 1
            s['value_sent'] += value_wei

        if recipient:
            r = addr_stats.setdefault(recipient, {'sent': 0, 'received': 0, 'value_sent': 0, 'value_received': 0})
            r['received']       += 1
            r['value_received'] += value_wei

    return addr_stats


def collect_movement_addresses(
    movement_block_df: pd.DataFrame,
    alchemy_api_key: str,
    max_days: Optional[int] = None,
    max_retries: int = 3,
    retry_backoff: float = 0.5,
) -> list:
    """
    For each movement day's block range, fetch all external ETH transfers via
    alchemy_getAssetTransfers and aggregate per-address activity stats.

    Replaces the previous Geth-based collect_recent_movement_addresses().
    No local node required. Output schema is identical to the old function
    so all downstream code (filter_results_by_activity, etc.) is unchanged.

    Inputs:
    -------
    - movement_block_df : pd.DataFrame  with ['movement_date', 'pre_date', 'start_block', 'end_block']
    - alchemy_api_key   : str
    - max_days          : int or None   (limit number of movement days processed)
    - max_retries       : int
    - retry_backoff     : float

    Outputs:
    --------
    - list of dicts:
        [{'pre_date': date, 'start_block': int, 'end_block': int,
          'address_roles': {addr: {'sent': int, 'received': int,
                                   'value_sent': int, 'value_received': int}}
         }, ...]
    """
    results        = []
    days_processed = 0
    total          = len(movement_block_df) if max_days is None else min(max_days, len(movement_block_df))

    # Process most recent days first (reverse chronological)
    for _, row in movement_block_df[::-1].iterrows():
        pre_date    = row['pre_date']
        start_block = int(row['start_block'])
        end_block   = int(row['end_block'])

        print(f"[{days_processed + 1}/{total}] Fetching transfers for {pre_date} "
              f"(blocks {start_block:,} -> {end_block:,})")

        try:
            transfers    = _fetch_eth_transfers_for_block_range(
                start_block, end_block, alchemy_api_key,
                max_retries=max_retries, retry_backoff=retry_backoff
            )
            address_roles = _aggregate_address_roles_from_transfers(transfers)

            print(f"  -> {len(transfers):,} transfers, {len(address_roles):,} unique addresses")

            results.append({
                'pre_date':      pre_date,
                'start_block':   start_block,
                'end_block':     end_block,
                'address_roles': address_roles
            })

            days_processed += 1
            if max_days and days_processed >= max_days:
                print(f"Reached max_days={max_days}, stopping.")
                break

        except Exception as e:
            print(f"  Error processing {pre_date}: {e}")
            continue  # skip this day and keep going (unlike Geth version which stopped)

    return results


def filter_results_by_activity(
    results: list,
    min_tx: int = 2,
    max_tx: int = 15,
    min_eth: float = 1.0,
    max_eth: float = 70.0
) -> list:
    """
    Filter address_roles within each day's result by tx count and ETH transacted.

    Inputs:
    -------
    - results : list of dicts (from collect_recent_movement_addresses)
    - min_tx  : int
    - max_tx  : int
    - min_eth : float
    - max_eth : float

    Outputs:
    --------
    - filtered list of dicts with address_roles trimmed to passing addresses
    """
    min_wei = int(min_eth * 1e18)
    max_wei = int(max_eth * 1e18)
    filtered = []

    for entry in results:
        filtered_roles = {
            addr: stats
            for addr, stats in entry['address_roles'].items()
            if (min_tx <= stats['sent'] + stats['received'] <= max_tx and
                min_wei <= stats['value_sent'] + stats['value_received'] <= max_wei)
        }
        if filtered_roles:
            copy = entry.copy()
            copy['address_roles'] = filtered_roles
            filtered.append(copy)

    return filtered


# =============================================================================
# 5. BALANCE FETCHING (Alchemy)
# =============================================================================

def fetch_balances_from_alchemy(
    addresses: List[str],
    block_number: int,
    alchemy_api_key: str,
    num_workers: int = 10
) -> Dict[str, int]:
    """
    Query Alchemy for ETH balances of multiple addresses at a block, in parallel.

    Inputs:
    -------
    - addresses         : list[str]
    - block_number      : int
    - alchemy_api_key   : str
    - num_workers       : int

    Outputs:
    --------
    - dict[address -> balance_wei (int)]
    """
    url     = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_api_key}"
    headers = {"Content-Type": "application/json"}
    balances: Dict[str, int] = {}

    def query_one(addr: str) -> Tuple[str, Optional[int]]:
        payload = {
            "jsonrpc": "2.0",
            "method": "eth_getBalance",
            "params": [addr, hex(block_number)],
            "id": 1
        }
        try:
            r = requests.post(url, json=payload, headers=headers, timeout=10)
            result = r.json().get("result")
            return addr, int(result, 16) if result else None
        except Exception:
            return addr, None

    print(f"Fetching balances for {len(addresses)} addresses at block {block_number}...")
    start = time.time()

    with ThreadPoolExecutor(max_workers=num_workers) as executor:
        futures = {executor.submit(query_one, addr): addr for addr in addresses}
        for i, future in enumerate(as_completed(futures), 1):
            addr, balance = future.result()
            if balance is not None:
                balances[addr] = balance
            if i % 100 == 0 or i == len(addresses):
                print(f"  {i}/{len(addresses)} complete")

    print(f"Done in {round(time.time() - start, 2)}s. Got {len(balances)} balances.")
    return balances


def get_addresses_with_nonzero_balance(
    addresses: List[str],
    block_number: int,
    alchemy_api_key: str,
    max_addresses: Optional[int] = None
) -> List[str]:
    """
    Return only addresses with a non-zero ETH balance at the given block.

    Inputs:
    -------
    - addresses         : list[str]
    - block_number      : int
    - alchemy_api_key   : str
    - max_addresses     : int or None

    Outputs:
    --------
    - list[str]
    """
    if max_addresses is not None:
        addresses = addresses[:max_addresses]
    balances = fetch_balances_from_alchemy(addresses, block_number, alchemy_api_key)
    return [addr for addr, bal in balances.items() if bal > 0]


# =============================================================================
# 6. BALANCE STORE (incremental persistence)
# =============================================================================

def filter_already_fetched(
    schedule_df: pd.DataFrame,
    store_csv_path: str,
    cohort: str,
    granularity: str = 'day'
) -> pd.DataFrame:
    """
    Remove (address, date) pairs from schedule_df that already exist in the
    balance store for the given cohort, enabling incremental fetching.
    Supports daily and hourly granularity.

    Inputs:
    -------
    - schedule_df   : pd.DataFrame  with ['address', 'date']
    - store_csv_path: str           path to the unified balance store CSV
    - cohort        : str           'high' or 'low' (used to filter existing store)
    - granularity   : str           'day' (default) or 'hour'
                      Controls timestamp precision for deduplication.
                      Use 'day' for daily balance fetches (normalizes to midnight).
                      Use 'hour' for hourly balance fetches (floors to hour).

    Outputs:
    --------
    - pd.DataFrame  (subset of schedule_df with already-fetched rows removed)
    """
    if not os.path.exists(store_csv_path):
        return schedule_df

    existing = pd.read_csv(store_csv_path, usecols=['address', 'date', 'cohort'])
    existing = existing[existing['cohort'] == cohort].copy()
    existing['address'] = existing['address'].str.lower().str.strip()

    # Apply appropriate timestamp precision
    if granularity == 'hour':
        existing['date'] = pd.to_datetime(existing['date'], format='mixed').dt.floor('h')
    else:
        existing['date'] = pd.to_datetime(existing['date'], format='mixed').dt.normalize()

    already_fetched = set(zip(existing['address'], existing['date']))

    sched = schedule_df.copy()
    sched['address'] = sched['address'].str.lower().str.strip()

    if granularity == 'hour':
        sched['date'] = pd.to_datetime(sched['date']).dt.floor('h')
    else:
        sched['date'] = pd.to_datetime(sched['date']).dt.normalize()

    mask = sched.apply(
        lambda r: (r['address'], r['date']) not in already_fetched, axis=1
    )
    n_skipped = (~mask).sum()
    if n_skipped:
        print(f"Skipping {n_skipped:,} already-fetched (address, date) pairs for cohort='{cohort}'")

    return sched[mask].reset_index(drop=True)


def append_balances_to_store(
    balances_df: pd.DataFrame,
    store_csv_path: str,
    cohort: str
) -> None:
    """
    Append new balance records to the unified balance store CSV.
    Adds a 'cohort' column and deduplicates on (address, date, cohort).
    Creates the file if it doesn't exist.

    Inputs:
    -------
    - balances_df    : pd.DataFrame  with ['address', 'date', 'block_number', 'balance_wei']
    - store_csv_path : str
    - cohort         : str  ('high' or 'low')

    Outputs:
    --------
    - None (writes to file)
    """
    required = {'address', 'date', 'block_number', 'balance_wei'}
    if not required.issubset(balances_df.columns):
        raise ValueError(f"balances_df must contain: {required}")

    df_new = balances_df.copy()
    df_new['cohort']     = cohort
    df_new['date_added'] = _date.today().isoformat()

    if os.path.exists(store_csv_path):
        existing    = pd.read_csv(store_csv_path)
        combined    = pd.concat([existing, df_new], ignore_index=True)
        combined    = combined.drop_duplicates(
            subset=['address', 'date', 'cohort'], keep='first'
        )
    else:
        combined = df_new

    combined.to_csv(store_csv_path, index=False)
    print(f"Balance store updated: {store_csv_path} ({len(combined):,} total rows)")


def load_balance_store(
    store_csv_path: str,
    granularity: str = 'day'
) -> pd.DataFrame:
    """
    Load the unified balance store and return a clean DataFrame.
    Supports daily and hourly granularity.

    Inputs:
    -------
    - store_csv_path : str
    - granularity    : str  'day' (default) or 'hour'
                       Controls timestamp precision.
                       'day'  — normalizes timestamps to midnight (daily resolution)
                       'hour' — floors timestamps to the hour (hourly resolution)

    Outputs:
    --------
    - pd.DataFrame with ['address', 'date', 'block_number', 'balance_wei', 'cohort']
    """
    if not os.path.exists(store_csv_path):
        raise FileNotFoundError(f"Balance store not found: {store_csv_path}")

    df = pd.read_csv(store_csv_path)

    if granularity == 'hour':
        df['date'] = pd.to_datetime(df['date'], format='mixed').dt.floor('h')
    else:
        df['date'] = pd.to_datetime(df['date'], format='mixed').dt.normalize()

    df['address']      = df['address'].str.lower().str.strip()
    df['balance_wei']  = pd.to_numeric(df['balance_wei'], errors='coerce').fillna(0).astype('int64')
    df['block_number'] = pd.to_numeric(df['block_number'], errors='coerce').astype('Int64')
    return df


# =============================================================================
# 7. WALLET LOG PERSISTENCE
# =============================================================================

def append_top_wallets_to_csv(
    top_wallets_df: pd.DataFrame,
    output_csv_path: str
) -> None:
    """
    Append qualifying wallets to the rolling expert wallet log.
    Deduplicates on 'address' (keeps first occurrence).
    Adds 'date_added' column.

    Inputs:
    -------
    - top_wallets_df  : pd.DataFrame  (must contain 'address')
    - output_csv_path : str

    Outputs:
    --------
    - None (writes to file)
    """
    if 'address' not in top_wallets_df.columns:
        raise ValueError("top_wallets_df must contain an 'address' column")

    df_new = top_wallets_df.copy()
    df_new['date_added'] = _date.today().isoformat()

    if os.path.exists(output_csv_path):
        existing = pd.read_csv(output_csv_path)
        combined = pd.concat([existing, df_new], ignore_index=True)
        combined = combined.drop_duplicates(subset=['address'], keep='first')
    else:
        combined = df_new

    combined.to_csv(output_csv_path, index=False)
    print(f"Wallet log updated: {output_csv_path} "
          f"({len(df_new)} new, {len(combined)} total unique addresses)")


# =============================================================================
# 8. BACKTEST UTILITIES
# =============================================================================

def split_wallet_df_by_date(
    df: pd.DataFrame,
    cutoff_date,
    date_col: str = 'date'
) -> Tuple[pd.DataFrame, pd.DataFrame]:
    """
    Split a DataFrame into train and test sets by a cutoff date.

    Inputs:
    -------
    - df          : pd.DataFrame
    - cutoff_date : str or pd.Timestamp
    - date_col    : str

    Outputs:
    --------
    - (train_df, test_df) : both pd.DataFrame
    """
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    cutoff       = pd.to_datetime(cutoff_date)
    return df[df[date_col] < cutoff], df[df[date_col] >= cutoff]


# =============================================================================
# 5. CONVEXITY SCORING
# =============================================================================

# ======================================================================
# 0) HELPERS TO ADAPT INPUT SCHEMAS
# ======================================================================

def build_trades_from_positions(
    df_pos: pd.DataFrame,
    *,
    date_col: str = "date",
    address_col: str = "address",
    action_col: str = "action_label",
    delta_eth_col: str = "delta_eth",
    buy_labels: Sequence[str] = ("buy",),
    sell_labels: Sequence[str] = ("sell",),
    infer_from_delta_when_missing: bool = True,
    min_abs_delta: float = 0.0
) -> pd.DataFrame:
    """
    Description:
    ------------
    Create a trade-level DataFrame (buys/sells only) from a daily position table.
    Uses `action_label` when available; optionally infers side from `delta_eth`
    if the label is missing or 'hold/None'.

    Inputs:
    -------
    - df_pos : pd.DataFrame
        Must include daily rows with at least [address_col, date_col, action_col, delta_eth_col].
    - date_col : str
    - address_col : str
    - action_col : str
        Values like {'buy','sell','hold',None}. Case-insensitive.
    - delta_eth_col : str
        Used to infer side when label is missing/hold (optional).
    - buy_labels : Sequence[str]
        Action labels that count as 'buy'.
    - sell_labels : Sequence[str]
        Action labels that count as 'sell'.
    - infer_from_delta_when_missing : bool
        If True, when action is not in buy/sell, infer:
          delta_eth > +min_abs_delta  -> 'buy'
          delta_eth < -min_abs_delta  -> 'sell'
          else                         -> ignore row
    - min_abs_delta : float
        Threshold for inferring a trade from delta_eth magnitude.

    Outputs:
    --------
    - pd.DataFrame
        Columns: ['date','address','side','size'] where:
          side in {'buy','sell'}; size = delta_eth (float, may be abs if you prefer).
    """
    df = df_pos.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    # Normalize labels
    act = df[action_col].astype(str).str.lower()

    is_buy_lbl  = act.isin([s.lower() for s in buy_labels])
    is_sell_lbl = act.isin([s.lower() for s in sell_labels])

    side = pd.Series(np.where(is_buy_lbl, "buy", np.where(is_sell_lbl, "sell", None)), index=df.index, dtype=object)

    if infer_from_delta_when_missing:
        # Only for rows that don't yet have side
        missing = side.isna()
        deltas = pd.to_numeric(df[delta_eth_col], errors="coerce")
        side.loc[missing & (deltas >  +float(min_abs_delta))] = "buy"
        side.loc[missing & (deltas <  -float(min_abs_delta))] = "sell"
        # If near-zero delta or NaN, we leave as NaN (no trade)

    out = pd.DataFrame({
        "date": df[date_col],
        "address": df[address_col],
        "side": side,
        "size": pd.to_numeric(df[delta_eth_col], errors="coerce")  # keep signed, or use abs if preferred
    })
    out = out[out["side"].isin(["buy","sell"])].dropna(subset=["date","address"]).reset_index(drop=True)
    return out


# ======================================================================
# 1) PRICE PREP & DERIVATIVE ESTIMATORS (UNCHANGED CORE)
# ======================================================================

def _ensure_datetime_sorted(df: pd.DataFrame, date_col: str) -> pd.DataFrame:
    df = df.copy()
    df[date_col] = pd.to_datetime(df[date_col])
    return df.sort_values(date_col).reset_index(drop=True)

def prepare_price_frame(
    df_price: pd.DataFrame,
    *,
    date_col: str = "date",
    price_col: str = "close"
) -> pd.DataFrame:
    """
    Description:
    ------------
    Prepare a price frame with log-price and daily log-returns.

    Inputs:
    -------
    - df_price : pd.DataFrame
        Must contain [date_col, price_col].
    - date_col : str
    - price_col : str

    Outputs:
    --------
    - pd.DataFrame with columns: [date_col, price_col, 'logp', 'ret1']
    """
    df = _ensure_datetime_sorted(df_price, date_col)
    df["logp"] = np.log(pd.to_numeric(df[price_col], errors="coerce").astype(float))
    df["ret1"] = df["logp"].diff()
    return df

def local_quad_derivatives(
    logp: pd.Series,
    window: int = 7,
    min_periods: Optional[int] = None
) -> Tuple[pd.Series, pd.Series]:
    """
    Description:
    ------------
    Estimate local slope (1st derivative) and curvature (2nd derivative)
    of log-price using a rolling quadratic fit centered at each date.

    Inputs:
    -------
    - logp : pd.Series
    - window : int (odd)
    - min_periods : int or None

    Outputs:
    --------
    - (slope, curvature) : (pd.Series, pd.Series)
    """
    if window % 2 == 0:
        raise ValueError("window must be odd for centered quadratic fit")
    if min_periods is None: min_periods = window

    n = len(logp)
    half = window // 2
    x = np.arange(-half, half + 1)

    slopes = np.full(n, np.nan)
    curvs  = np.full(n, np.nan)
    vals = logp.values.astype(float)

    for i in range(n):
        lo = max(0, i - half); hi = min(n, i + half + 1)
        xx = x[(half - (i - lo)):(half + (hi - i))]
        yy = vals[lo:hi]
        if len(yy) >= min_periods:
            a, b, c = np.polyfit(xx, yy, deg=2)  # y=a x^2 + b x + c
            slopes[i] = b
            curvs[i]  = 2 * a
    return pd.Series(slopes, index=logp.index), pd.Series(curvs, index=logp.index)

def multi_horizon_slopes(
    logp: pd.Series,
    horizons: Iterable[int] = (1,3,7)
) -> Dict[str, pd.Series]:
    """
    Description:
    ------------
    Compute forward/backward average slopes over multiple horizons.

    Inputs:
    -------
    - logp : pd.Series
    - horizons : Iterable[int]

    Outputs:
    --------
    - dict of pd.Series with keys 'fwd_k','bak_k'
    """
    out = {}
    for k in horizons:
        out[f"fwd_{k}"] = (logp.shift(-k) - logp) / float(k)
        out[f"bak_{k}"] = (logp - logp.shift(k)) / float(k)
    return out

def zscore_by_rolling_vol(
    s: pd.Series,
    vol_ref: pd.Series,
    window: int = 20,
    eps: float = 1e-12
) -> pd.Series:
    """
    Description:
    ------------
    Z-normalize a signal using rolling volatility of a reference (e.g., returns).

    Inputs:
    -------
    - s : pd.Series
    - vol_ref : pd.Series
    - window : int
    - eps : float

    Outputs:
    --------
    - pd.Series
    """
    vol = vol_ref.rolling(window, min_periods=5).std()
    return s / (vol + eps)

def detect_local_extrema(
    slope: pd.Series,
    curvature: pd.Series
) -> pd.Series:
    """
    Description:
    ------------
    Label local minima/maxima using slope zero-crossings and curvature sign.

    Inputs:
    -------
    - slope : pd.Series
    - curvature : pd.Series

    Outputs:
    --------
    - pd.Series with values {'min','max',None}
    """
    s = slope.copy()
    up_cross   = (s.shift(1) < 0) & (s > 0) & (curvature > 0)
    down_cross = (s.shift(1) > 0) & (s < 0) & (curvature < 0)

    out = pd.Series(index=s.index, dtype=object)
    out[up_cross]   = "min"
    out[down_cross] = "max"
    return out

def extremum_proximity_bonus(
    dates: pd.Series,
    trade_side: pd.Series,     # 'buy' or 'sell'
    extrema_kind: pd.Series,   # 'min'/'max'/None
    slope_fwd_z: pd.Series,
    slope_bak_z: pd.Series,
    tau_days: float = 3.0
) -> pd.Series:
    """
    Description:
    ------------
    Proximity bonus to the nearest appropriate extremum (min for buys, max for sells),
    scaled by event sharpness (|fwd_z| + |bak_z|) and decaying with distance.

    Inputs:
    -------
    - dates, trade_side, extrema_kind, slope_fwd_z, slope_bak_z, tau_days

    Outputs:
    --------
    - pd.Series of float
    """
    idx = dates.reset_index(drop=True)
    ek  = extrema_kind.reset_index(drop=True)

    min_idx = set(np.where(ek.values == "min")[0])
    max_idx = set(np.where(ek.values == "max")[0])

    def nearest_distance(i: int, targets: set) -> Optional[int]:
        if not targets: return None
        return min(abs(i - t) for t in targets)

    ts = trade_side.reset_index(drop=True).astype(str).str.lower()
    bonus = np.zeros(len(idx), dtype=float)
    sharp = (np.abs(slope_fwd_z.reset_index(drop=True).values) +
             np.abs(slope_bak_z.reset_index(drop=True).values))

    for i in range(len(idx)):
        side = ts.iloc[i]
        targets = min_idx if side == "buy" else max_idx if side == "sell" else set()
        d = nearest_distance(i, targets)
        if d is None:
            bonus[i] = 0.0
        else:
            bonus[i] = np.exp(-float(d)/float(tau_days)) * sharp[i]
    return pd.Series(bonus, index=dates.index)


# ======================================================================
# 2) TRADE-LEVEL SCORING (ADAPTED TO YOUR SCHEMA)
# ======================================================================

def score_trades_by_convexity(
    df_price: pd.DataFrame,
    df_trades: pd.DataFrame,
    *,
    date_col_price: str = "date",
    price_col: str = "close",
    date_col_trade: str = "date",
    address_col: str = "address",
    side_col: str = "side",
    size_col: Optional[str] = "size",
    poly_window: int = 7,
    slope_horizons: Iterable[int] = (1,3,7),
    vol_window: int = 20,
    weights: Dict[str, float] = None,
    tau_days: float = 3.0
) -> pd.DataFrame:
    if weights is None:
        weights = dict(w_fwd=1.0, w_bak=0.5, w_curv=0.5, w_prox=0.5)

    # --- Price features ---
    px = prepare_price_frame(df_price, date_col=date_col_price, price_col=price_col)
    slope_local, curv_local = local_quad_derivatives(px["logp"], window=poly_window)
    slopes = multi_horizon_slopes(px["logp"], horizons=slope_horizons)

    fwd_med = pd.concat([slopes[f"fwd_{k}"] for k in slope_horizons], axis=1).median(axis=1)
    bak_med = pd.concat([slopes[f"bak_{k}"] for k in slope_horizons], axis=1).median(axis=1)

    slope_fwd_z = zscore_by_rolling_vol(fwd_med, px["ret1"], window=vol_window)
    slope_bak_z = zscore_by_rolling_vol(bak_med, px["ret1"], window=vol_window)
    curvature_z = zscore_by_rolling_vol(curv_local, px["ret1"], window=vol_window)
    extrema_kind = detect_local_extrema(slope_local, curv_local)

    ref = px[[date_col_price]].copy().rename(columns={date_col_price: "price_date"})
    ref["slope_fwd_z"] = slope_fwd_z.values
    ref["slope_bak_z"] = slope_bak_z.values
    ref["curvature_z"] = curvature_z.values
    ref["extrema_kind"] = extrema_kind.values

    # --- Trades ---
    t = df_trades.copy()
    t[date_col_trade] = pd.to_datetime(t[date_col_trade])
    t = t[t[side_col].isin(["buy","sell"])].copy()
    t = t.sort_values([address_col, date_col_trade]).reset_index(drop=True)
    t = t.rename(columns={date_col_trade: "trade_date"})

    # Join price features onto trades
    scored = t.merge(ref, left_on="trade_date", right_on="price_date", how="left")

    # --- Proximity bonus (per-trade) ---
    idx_map = {d: i for i, d in enumerate(ref["price_date"].values)}
    trade_pos = scored["price_date"].map(idx_map).astype("float64")

    min_idx = np.where(ref["extrema_kind"].values == "min")[0]
    max_idx = np.where(ref["extrema_kind"].values == "max")[0]
    sharp_series = (np.abs(ref["slope_fwd_z"].values) + np.abs(ref["slope_bak_z"].values))

    def nearest_dist(arr_pos: np.ndarray, targets: np.ndarray) -> np.ndarray:
        if targets.size == 0:
            return np.full(arr_pos.shape, np.inf, dtype=float)
        diffs = np.abs(arr_pos[:, None] - targets[None, :])
        return np.nanmin(diffs, axis=1)

    pos_arr = trade_pos.values
    is_buy = (scored[side_col].values == "buy")
    dist = np.where(is_buy, nearest_dist(pos_arr, min_idx), nearest_dist(pos_arr, max_idx))

    sharp_at_trade = np.zeros_like(pos_arr, dtype=float)
    valid = np.isfinite(pos_arr)
    pos_int = pos_arr[valid].astype(int)
    sharp_at_trade[valid] = sharp_series[pos_int]

    scored["prox_bonus"] = np.exp(-dist / float(tau_days)) * sharp_at_trade
    scored.loc[~np.isfinite(dist), "prox_bonus"] = 0.0

    # --- Directional shaping & composite ---
    is_buy_int = is_buy.astype(int); is_sell_int = (~is_buy).astype(int)
    fwd_adj = scored["slope_fwd_z"] * (is_buy_int * 1.0 + is_sell_int * -1.0)
    bak_adj = scored["slope_bak_z"] * (is_buy_int * -1.0 + is_sell_int * 1.0)
    curv_adj = scored["curvature_z"] * (is_buy_int * 1.0 + is_sell_int * -1.0)

    scored["convexity_score"] = (
        weights["w_fwd"] * fwd_adj.fillna(0.0) +
        weights["w_bak"] * bak_adj.fillna(0.0) +
        weights["w_curv"] * curv_adj.fillna(0.0) +
        weights["w_prox"] * scored["prox_bonus"].fillna(0.0)
    )

    # Keep both dates explicitly; drop helper if you like
    return scored.drop(columns=["price_date"])



# ======================================================================
# 3) WALLET-LEVEL AGGREGATION (UNCHANGED)
# ======================================================================

def aggregate_wallet_scores(
    df_scored: pd.DataFrame,
    *,
    address_col: str = "address",
    score_col: str = "convexity_score",
    side_col: str = "side",
    size_col: Optional[str] = "size",
    min_trades: int = 8
) -> pd.DataFrame:
    """
    Description:
    ------------
    Aggregate trade-level convexity scores to wallet-level metrics.

    Inputs:
    -------
    - df_scored : pd.DataFrame
    - address_col : str
    - score_col : str
    - side_col : str
    - size_col : Optional[str]
    - min_trades : int

    Outputs:
    --------
    - pd.DataFrame with wallet-level stats and ranking.
    """
    df = df_scored.copy()
    df = df[np.isfinite(df[score_col])]

    grp = df.groupby(address_col)
    agg = grp[score_col].agg(
        n_trades="count",
        mean_score="mean",
        median_score="median",
        p25_score=lambda x: np.nanpercentile(x, 25),
        p75_score=lambda x: np.nanpercentile(x, 75),
    ).reset_index()

    # Mean scores by side
    buy_means  = df[df[side_col] == "buy"].groupby(address_col)[score_col].mean()
    sell_means = df[df[side_col] == "sell"].groupby(address_col)[score_col].mean()
    agg = agg.merge(buy_means.rename("mean_buy_score"), on=address_col, how="left")
    agg = agg.merge(sell_means.rename("mean_sell_score"), on=address_col, how="left")

    # Trade counts by side
    side_counts = df.groupby([address_col, side_col]).size().unstack(fill_value=0)
    side_counts = side_counts.rename(
        columns={"buy": "n_buys", "sell": "n_sells"}
    ).reset_index()
    agg = agg.merge(side_counts, on=address_col, how="left")

    # Optional size-weighted score
    if size_col is not None and size_col in df.columns:
        sw = df.groupby(address_col).apply(
            lambda g: np.average(
                g[score_col],
                weights=np.abs(pd.to_numeric(g[size_col], errors="coerce")) + 1e-12
            )
        ).rename("size_wt_score")
        agg = agg.merge(sw, on=address_col, how="left")

    agg = agg[agg["n_trades"] >= int(min_trades)].sort_values("mean_score", ascending=False).reset_index(drop=True)
    return agg


# =============================================================================
# 6. SIMULATION & SCHEDULING
# =============================================================================

def simulate_daily_buys_and_score(
    df_price: pd.DataFrame,
    *,
    date_col: str = "date",
    price_col: str = "close",
    poly_window: int = 9,
    slope_horizons=(1,3,7),
    vol_window: int = 20,
    weights: Dict[str, float] = None
) -> pd.DataFrame:
    # Create synthetic trades: buy every day
    trades = pd.DataFrame({
        "date": pd.to_datetime(df_price[date_col]),
        "address": "synthetic_wallet",
        "side": "buy",
        "size": 1.0
    })

    scored = score_trades_by_convexity(
        df_price=df_price,
        df_trades=trades,
        date_col_price=date_col,
        price_col=price_col,
        date_col_trade="date",
        address_col="address",
        side_col="side",
        size_col="size",
        poly_window=poly_window,
        slope_horizons=slope_horizons,
        vol_window=vol_window,
        weights=weights
    )

    # Ensure both keys are datetime before merge
    scored["trade_date"] = pd.to_datetime(scored["trade_date"])
    df_price[date_col] = pd.to_datetime(df_price[date_col])

    # Merge back the price column
    scored = scored.merge(
        df_price[[date_col, price_col]],
        left_on="trade_date", right_on=date_col,
        how="left"
    ).drop(columns=[date_col])

    return scored

def simulate_daily_scores(
    df_price: pd.DataFrame,
    *,
    side: str = "buy",               # "buy" or "sell"
    date_col: str = "date",
    price_col: str = "close",
    poly_window: int = 17,
    slope_horizons: Iterable[int] = (3,7,15,21),
    vol_window: int = 30,
    weights: Dict[str, float] = None,
    score_fn: Callable = None        # pass score_trades_by_convexity
) -> pd.DataFrame:
    if score_fn is None:
        raise ValueError("Provide score_fn=score_trades_by_convexity")

    px = df_price.copy()
    px[date_col] = pd.to_datetime(px[date_col])

    # synthetic “trade every day” for the given side
    trades = pd.DataFrame({
        "date": px[date_col],
        "address": "synthetic_wallet",
        "side": side,
        "size": 1.0
    })

    scored = score_fn(
        df_price=px,
        df_trades=trades,
        date_col_price=date_col,
        price_col=price_col,
        date_col_trade="date",
        address_col="address",
        side_col="side",
        size_col="size",
        poly_window=poly_window,
        slope_horizons=slope_horizons,
        vol_window=vol_window,
        weights=weights
    )

    scored["trade_date"] = pd.to_datetime(scored["trade_date"])
    # carry price for convenience
    scored = scored.merge(
        px[[date_col, price_col]],
        left_on="trade_date", right_on=date_col, how="left"
    ).drop(columns=[date_col])

    return scored


def select_event_dates(
    sim_df: pd.DataFrame,
    *,
    score_col: str = "convexity_score",
    date_col: str = "trade_date",
    quantile: float = 0.90
) -> List[pd.Timestamp]:
    q = sim_df[score_col].quantile(quantile)
    ev = sim_df.loc[sim_df[score_col] >= q, date_col].dropna()
    return sorted(pd.to_datetime(ev).dt.normalize().unique().tolist())



def build_balance_query_schedule(
    *,
    candidate_wallets: List[str],
    buy_windows: List[pd.Timestamp],
    sell_windows: List[pd.Timestamp],
    include_buy: bool = True,
    include_sell: bool = True,
    dedupe: bool = True
) -> pd.DataFrame:
    """
    Returns a DataFrame of (address, date, cohort) rows to query balances for.
    - cohort: 'buy' if date came from buy windows, 'sell' if from sell windows (or 'both' if deduped overlap)
    """
    rows = []
    if include_buy and buy_windows:
        rows += [(w, d, "buy") for w, d in product(candidate_wallets, buy_windows)]
    if include_sell and sell_windows:
        rows += [(w, d, "sell") for w, d in product(candidate_wallets, sell_windows)]

    sched = pd.DataFrame(rows, columns=["address", "date", "cohort"])
    if sched.empty:
        return sched

    sched["date"] = pd.to_datetime(sched["date"]).dt.normalize()

    if dedupe:
        # If the same (wallet, date) appears in both, keep one and label cohort='both'
        sched = (sched
                 .groupby(["address", "date"], as_index=False)["cohort"]
                 .apply(lambda s: "both" if set(s) == {"buy", "sell"} else s.iloc[0])
                 .reset_index(drop=True))
    return sched.sort_values(["address", "date"]).reset_index(drop=True)


# =============================================================================
# 7. BALANCE FETCH (async, Alchemy)
# =============================================================================

# ---------- you already have these building blocks ----------
# - simulate_daily_scores (buy/sell)  -> like your simulate_daily_buys_and_score but param side
# - score_trades_by_convexity         -> your scorer
# - build_to_query_df                 -> robust version we fixed
# - fetch_wallet_balances_unified_async_wrapper -> your async fetcher wrapper

# ---------- tiny helpers ----------

def _select_band_dates(
    scored_df: pd.DataFrame,
    *,
    date_col: str = "trade_date",
    score_col: str = "convexity_score",
    band: Tuple[float, float] = (0.95, 1.00)
) -> List[pd.Timestamp]:
    """
    Return core event dates whose score percentile lies within [lo, hi].
    band values are in [0,1], inclusive.
    """
    df = scored_df[[date_col, score_col]].dropna().copy()
    df[date_col] = pd.to_datetime(df[date_col]).dt.normalize()
    # empirical percentiles
    df["_pct"] = df[score_col].rank(pct=True, method="average")
    lo, hi = float(band[0]), float(band[1])
    core = df.loc[(df["_pct"] >= lo) & (df["_pct"] <= hi), date_col].drop_duplicates()
    return core.sort_values().tolist()

def build_windows(dates: List[pd.Timestamp], pad_days: int = 1) -> List[pd.Timestamp]:
    if not dates:
        return []
    base = []
    for d in pd.to_datetime(pd.Series(dates)).dt.normalize().unique():
        for k in range(-pad_days, pad_days + 1):
            base.append(pd.Timestamp(d) + pd.Timedelta(days=k))
    return sorted(pd.to_datetime(pd.Series(base)).dt.normalize().unique().tolist())

def _dedupe_addresses(addresses: List[str]) -> List[str]:
    return (pd.Series(addresses, dtype="object")
            .dropna().map(str).str.lower().str.strip().drop_duplicates().tolist())

def _concat_unique_dates(*lists_of_dates: List[pd.Timestamp]) -> pd.DataFrame:
    if not lists_of_dates:
        return pd.DataFrame({"date": []})
    cat = pd.concat([pd.DataFrame({"date": lst}) for lst in lists_of_dates], ignore_index=True)
    cat["date"] = pd.to_datetime(cat["date"]).dt.normalize()
    return cat.drop_duplicates().sort_values("date").reset_index(drop=True)

# ---------- v2 scheduler: bands in, schedule out (NO fetching) ----------

def prepare_convexity_event_schedule_v2(
    *,
    # price + scoring params
    df_price: pd.DataFrame,
    date_col: str = "date",
    price_col: str = "close",
    poly_window: int = 17,
    slope_horizons: Iterable[int] = (3,7,15,21),
    vol_window: int = 30,
    weights: Dict[str, float] = dict(w_fwd=2.5, w_bak=0.5, w_curv=0.8, w_prox=0.5),

    # NEW: bands (range of percentiles) instead of single quantile
    buy_band: Optional[Tuple[float,float]] = (0.90, 1.00),
    sell_band: Optional[Tuple[float,float]] = (0.90, 1.00),
    pad_days: int = 1,
    include_buy: bool = True,
    include_sell: bool = True,

    # provide addresses directly (already discovered elsewhere)
    addresses: List[str],

    # optional cheap wallet filters
    last_known_balances: Optional[dict] = None,  # {address: balance_eth}
    min_balance_eth: float = 0.0,
    blacklist: Optional[Set[str]] = None,

    # scorer
    score_fn: Callable = None  # e.g., score_trades_by_convexity
) -> Dict[str, pd.DataFrame]:
    """
    Build a balance-query schedule using a pre-supplied address list.
    Returns:
      {
        'buy_sim','sell_sim',
        'buy_events','sell_events',
        'buy_windows','sell_windows',
        'candidate_wallets',
        'balance_query_schedule'
      }
    """
    if score_fn is None:
        raise ValueError("Provide score_fn=score_trades_by_convexity (or compatible).")

    # 1) Simulate daily scores (buy & sell)
    buy_sim = None
    sell_sim = None
    if include_buy:
        buy_sim = simulate_daily_scores(
            df_price=df_price, side="buy",
            date_col=date_col, price_col=price_col,
            poly_window=poly_window, slope_horizons=slope_horizons,
            vol_window=vol_window, weights=weights, score_fn=score_fn
        )
    if include_sell:
        sell_sim = simulate_daily_scores(
            df_price=df_price, side="sell",
            date_col=date_col, price_col=price_col,
            poly_window=poly_window, slope_horizons=slope_horizons,
            vol_window=vol_window, weights=weights, score_fn=score_fn
        )

    # 2) Select core dates by percentile band(s)
    buy_events = []
    sell_events = []
    if include_buy and buy_sim is not None and buy_band is not None:
        buy_events = _select_band_dates(buy_sim, date_col="trade_date",
                                        score_col="convexity_score", band=buy_band)
    if include_sell and sell_sim is not None and sell_band is not None:
        sell_events = _select_band_dates(sell_sim, date_col="trade_date",
                                         score_col="convexity_score", band=sell_band)

    # 3) Build padded windows
    buy_windows  = build_windows(buy_events,  pad_days=pad_days) if include_buy else []
    sell_windows = build_windows(sell_events, pad_days=pad_days) if include_sell else []

    # 4) Candidate wallets (filter provided list)
    candidates = _dedupe_addresses(addresses)
    if last_known_balances is not None and min_balance_eth > 0.0:
        candidates = [a for a in candidates if last_known_balances.get(a, 0.0) >= min_balance_eth]
    if blacklist:
        bl = {x.lower() for x in blacklist}
        candidates = [a for a in candidates if a not in bl]

    # 5) Build (wallet × date) schedule (no cohort/band needed for fetching)
    schedule_dates_df = _concat_unique_dates(
        pd.to_datetime(buy_windows) if include_buy else pd.to_datetime([]),
        pd.to_datetime(sell_windows) if include_sell else pd.to_datetime([])
    )

    # produce final product: a simple cartesian product (address × date)
    if schedule_dates_df.empty or not candidates:
        balance_query_schedule = pd.DataFrame(columns=["address","date"])
    else:
        balance_query_schedule = pd.DataFrame(
            list(product(candidates, schedule_dates_df["date"].tolist())),
            columns=["address","date"]
        )
    balance_query_schedule["address"] = balance_query_schedule["address"].astype(str).str.lower().str.strip()
    balance_query_schedule["date"] = pd.to_datetime(balance_query_schedule["date"]).dt.normalize()

    return dict(
        buy_sim=buy_sim,
        sell_sim=sell_sim,
        buy_events=pd.DataFrame({"date": buy_events}),
        sell_events=pd.DataFrame({"date": sell_events}),
        buy_windows=pd.DataFrame({"date": buy_windows}),
        sell_windows=pd.DataFrame({"date": sell_windows}),
        candidate_wallets=pd.DataFrame({"address": candidates}),
        balance_query_schedule=balance_query_schedule  # [address,date]
    )













# ---------------------------------------------------------------------
# Optional: tiny guard (you can delete if you already have one)
# ---------------------------------------------------------------------
def _ensure_columns(df: pd.DataFrame, cols: list, df_name: str = "DataFrame"):
    """
    Description:
    ------------
    Ensures required columns exist on a DataFrame.

    Inputs:
    -------
    - df : pd.DataFrame
    - cols : list[str]
    - df_name : str

    Outputs:
    --------
    - None; raises ValueError if missing
    """
    missing = [c for c in cols if c not in df.columns]
    if missing:
        raise ValueError(f"{df_name} is missing required columns: {missing}")

# ---------------------------------------------------------------------
# Token bucket for polite rate limiting
# ---------------------------------------------------------------------
class _TokenBucket:
    """
    Description:
    ------------
    Simple token bucket for request rate limiting.
    """
    def __init__(self, rate_per_sec: float, burst: int):
        self.rate = float(rate_per_sec)
        self.capacity = int(max(1, burst))
        self.tokens = float(self.capacity)
        self.ts = time.monotonic()

    async def take(self, n: int = 1):
        while True:
            now = time.monotonic()
            self.tokens = min(self.capacity, self.tokens + (now - self.ts) * self.rate)
            self.ts = now
            if self.tokens >= n:
                self.tokens -= n
                return
            await asyncio.sleep(max(0.01, (n - self.tokens) / self.rate))

async def _gather_limited(concurrency: int, *coros):
    """
    Description:
    ------------
    Run coroutines with a concurrency limit.

    Inputs:
    -------
    - concurrency : int
    - *coros : coroutine

    Outputs:
    --------
    - list of results
    """
    sem = asyncio.Semaphore(concurrency)
    async def _wrap(coro):
        async with sem:
            return await coro
    return await asyncio.gather(*(_wrap(c) for c in coros))

    
# ---------------------------------------------------------------------
# Jupyter-safe sync wrapper around the async fetcher
# ---------------------------------------------------------------------
def fetch_wallet_balances_unified_async_wrapper(**kwargs) -> pd.DataFrame:
    """
    Description:
    ------------
    Sync-friendly wrapper that works both in scripts and Jupyter.

    Behavior:
    ---------
    - If no event loop is running: uses asyncio.run(...)
    - If an event loop is already running (e.g., Jupyter): re-enters it using nest_asyncio
    """
    coro = fetch_balances_batched_async(**kwargs)
    try:
        loop = asyncio.get_running_loop()  # raises if no running loop
    except RuntimeError:
        return asyncio.run(coro)
    else:
        nest_asyncio.apply(loop)
        return loop.run_until_complete(coro)

nest_asyncio.apply()


# ---------------------------------------------------------------------
# Core async fetcher with batching + progress printing
# ---------------------------------------------------------------------
async def fetch_balances_batched_async(
    to_query_df: pd.DataFrame,
    alchemy_api_key: str,
    *,
    block_col: str = "block_number",
    address_col: str = "address",
    date_col: str = "date",
    batch_size: int = 80,
    max_concurrency: int = 12,
    rps: float = 15.0,
    max_retries: int = 3,
    retry_backoff: float = 0.5,
    progress_every: int = 10,
    label: str = "fetch"
) -> pd.DataFrame:
    """
    Description:
    ------------
    Async JSON-RPC batched balance fetcher.
    - Groups by block_number
    - Dedupes addresses per block
    - Uses HTTP/2 keep-alive + token-bucket rate limiting
    - Maps results by request ID (order-proof)
    - Returns None for failed/missing responses (not 0)

    Inputs:
    -------
    - to_query_df : pd.DataFrame
        Must contain [address_col, date_col, block_col].
    - alchemy_api_key : str
    - block_col, address_col, date_col : str
    - batch_size, max_concurrency, rps, max_retries, retry_backoff : tuning params
    - progress_every : int
    - label : str

    Outputs:
    --------
    - pd.DataFrame with columns [address, date, block_number, balance_wei]
    """
    needed = {address_col, date_col, block_col}
    missing = needed - set(to_query_df.columns)
    if missing:
        raise ValueError(f"to_query_df missing required columns: {missing}")
    if to_query_df.empty:
        return pd.DataFrame(columns=[address_col, date_col, block_col, "balance_wei"])

    df = to_query_df[list(needed)].copy()
    df[address_col] = df[address_col].astype(str).str.lower().str.strip()
    df[date_col] = pd.to_datetime(df[date_col])
    df = df.drop_duplicates(subset=[address_col, date_col, block_col], ignore_index=True)

    # Group addresses by block
    by_block: Dict[int, List[str]] = {
        int(bn): g[address_col].drop_duplicates().tolist()
        for bn, g in df.groupby(block_col, sort=False)
    }

    # Progress accounting
    total_batches = sum((len(addrs) + batch_size - 1) // batch_size for addrs in by_block.values())
    done_batches = 0
    print_flusher_lock = asyncio.Lock()

    def _progress_maybe_print():
        pct = (done_batches / total_batches * 100.0) if total_batches else 100.0
        print(f"\r[{label}] {done_batches:,}/{total_batches:,} batches ({pct:.1f}%)", end="", flush=True)

    url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_api_key}"
    bucket = _TokenBucket(rate_per_sec=rps, burst=max(2, int(rps)))

    # ---------------- PATCHED BATCH POST ----------------
    async def _post_batch(client: httpx.AsyncClient, block_num: int, addrs: List[str]) -> List[Tuple[str, int, Optional[int]]]:
        nonlocal done_batches
        reqs = []
        for i, addr in enumerate(addrs):
            rid = f"{block_num}|{addr}|{i}"
            reqs.append({
                "jsonrpc": "2.0",
                "id": rid,
                "method": "eth_getBalance",
                "params": [addr, hex(block_num)]
            })

        for attempt in range(max_retries + 1):
            await bucket.take(1)
            try:
                r = await client.post(url, json=reqs, timeout=30)
                if r.status_code in (429, 503):
                    await asyncio.sleep(retry_backoff * (2 ** attempt))
                    continue
                r.raise_for_status()
                data = r.json()

                by_id = {}
                if isinstance(data, list):
                    for item in data:
                        if isinstance(item, dict) and "id" in item:
                            by_id[item["id"]] = item

                out = []
                for i, addr in enumerate(addrs):
                    rid = f"{block_num}|{addr}|{i}"
                    item = by_id.get(rid)
                    if not item or "error" in item:
                        bal = None
                    else:
                        res = item.get("result")
                        bal = int(res, 16) if res is not None else None
                    out.append((addr, block_num, bal))

                async with print_flusher_lock:
                    done_batches += 1
                    if (done_batches % progress_every) == 0 or (done_batches == total_batches):
                        _progress_maybe_print()
                return out

            except Exception:
                if attempt == max_retries:
                    async with print_flusher_lock:
                        done_batches += 1
                        if (done_batches % progress_every) == 0 or (done_batches == total_batches):
                            _progress_maybe_print()
                    return [(addr, block_num, None) for addr in addrs]
                await asyncio.sleep(retry_backoff * (2 ** attempt))

    # ----------------------------------------------------

    async with httpx.AsyncClient(http2=True, timeout=30, headers={"Connection": "keep-alive"}) as client:
        tasks = []
        for block_num, addrs in by_block.items():
            for i in range(0, len(addrs), batch_size):
                tasks.append(_post_batch(client, block_num, addrs[i:i+batch_size]))
        results = await _gather_limited(max_concurrency, *tasks)

    if total_batches:
        print()  # newline after final progress

    # ---------------- PATCHED ASSEMBLY ----------------
    rows = [row for group in results for row in group]
    out = pd.DataFrame(rows, columns=[address_col, block_col, "balance_wei"])
    key_df = df[[address_col, date_col, block_col]].drop_duplicates()
    out = out.merge(key_df, on=[address_col, block_col], how="inner")
    out = out[[address_col, date_col, block_col, "balance_wei"]]

    # Enforce uniqueness
    assert not out.duplicated([address_col, date_col, block_col]).any(), "Duplicate keys in fetched results"

    return out



# ---------- fetcher that takes the schedule and only fetches ----------

def fetch_balances_for_schedule(
    *,
    schedule: dict | pd.DataFrame,
    block_df: pd.DataFrame,
    alchemy_api_key: str,
    fetch_wallet_balances_func=fetch_wallet_balances_unified_async_wrapper,
    batch_size: int = 80,
    max_concurrency: int = 12,
    rps: float = 15.0,
    max_retries: int = 3,
    retry_backoff: float = 0.5,
    progress_every: int = 10,
    label: str = "balances",
    block_date_col: str = "date"   # <— NEW
) -> pd.DataFrame:
    # Extract [address,date]
    if isinstance(schedule, dict):
        if "balance_query_schedule" not in schedule:
            raise ValueError("schedule dict missing 'balance_query_schedule'.")
        pairs = schedule["balance_query_schedule"].copy()
    else:
        pairs = schedule.copy()

    if pairs.empty:
        return pd.DataFrame(columns=["address","date","block_number","balance_wei"])

    # ----- normalize block_df date -----
    bd = block_df.copy()
    # Try common fallbacks automatically if 'block_date_col' missing
    if block_date_col not in bd.columns:
        for alt in ["block_day", "blockdate", "day", "timestamp", "ts"]:
            if alt in bd.columns:
                block_date_col = alt
                break
        else:
            raise ValueError(
                f"block_df is missing a date-like column. "
                f"Provide block_date_col=... (available: {bd.columns.tolist()})"
            )

    # Build a normalized 'date' column
    if np.issubdtype(bd[block_date_col].dtype, np.datetime64):
        bd["date"] = pd.to_datetime(bd[block_date_col]).dt.normalize()
    else:
        # try to parse strings/ints to datetime
        bd["date"] = pd.to_datetime(bd[block_date_col], errors="coerce").dt.normalize()
    if "block_number" not in bd.columns:
        raise ValueError("block_df must contain 'block_number'.")

    bd = bd.dropna(subset=["date"]).drop_duplicates(subset=["date"])

    # ----- normalize pairs -----
    pairs["date"] = pd.to_datetime(pairs["date"]).dt.normalize()
    pairs["address"] = pairs["address"].astype(str).str.lower().str.strip()

    to_query = (
        pairs.merge(bd[["date","block_number"]], on="date", how="left")
             .dropna(subset=["block_number"])
             .astype({"block_number": "int64"})
             .drop_duplicates(subset=["address","date","block_number"])
             .sort_values(["address","date"])
    )

    if to_query.empty:
        return pd.DataFrame(columns=["address","date","block_number","balance_wei"])

    fetched = fetch_wallet_balances_func(
        to_query_df=to_query[["address","date","block_number"]],
        alchemy_api_key=alchemy_api_key,
        batch_size=batch_size,
        max_concurrency=max_concurrency,
        rps=rps,
        max_retries=max_retries,
        retry_backoff=retry_backoff,
        progress_every=progress_every,
        label=label
    )

    fetched["address"] = fetched["address"].astype(str).str.lower().str.strip()
    fetched["date"] = pd.to_datetime(fetched["date"]).dt.normalize()
    fetched = (fetched
               .drop_duplicates(subset=["address","date","block_number"])
               .sort_values(["address","date"])
               .reset_index(drop=True))
    return fetched


# =============================================================================
# 8. WALLET HIT RATE SCORING
# =============================================================================

def _to_sorted_dates(x):
    return np.array(sorted(pd.to_datetime(pd.Series(x)).dt.normalize().dropna().unique()))

def _hit_counts(trade_dates: np.ndarray, event_dates: np.ndarray, window_days: int) -> tuple[int, int, int, int]:
    """
    Causal hit counting — trades must PRECEDE the event by at most window_days.
    A trade on the event date itself (delta=0) also counts.
    Trades AFTER an event date are never credited (no look-ahead leakage).

    Returns:
      (event_hits, num_events, trade_hits, num_trades)
      where:
        event_hits = # of event dates that have a qualifying trade in the
                     [event_date - window_days, event_date] window
        trade_hits = # of trade dates that have a qualifying event in the
                     [trade_date, trade_date + window_days] window
    """
    if len(event_dates) == 0:
        return 0, 0, 0, len(trade_dates)
    if len(trade_dates) == 0:
        return 0, len(event_dates), 0, 0

    w = np.timedelta64(window_days, 'D')

    # Event coverage: for each event date, was there a trade in the
    # preceding window_days (trade_date in [event_date - w, event_date])?
    eh = 0
    for d in event_dates:
        i = bisect_left(trade_dates, d - w)
        if i < len(trade_dates) and trade_dates[i] <= d:
            eh += 1

    # Trade alignment: for each trade date, did an event occur within
    # the following window_days (event_date in [trade_date, trade_date + w])?
    th = 0
    for d in trade_dates:
        i = bisect_left(event_dates, d)
        if i < len(event_dates) and event_dates[i] <= d + w:
            th += 1

    return eh, len(event_dates), th, len(trade_dates)


def compute_wallet_event_hit_rates(
    inferred_trades: pd.DataFrame,
    *,
    # Provide either (buy_events, sell_events) or a schedule_dates with 'cohort'
    buy_events: pd.DataFrame | None = None,
    sell_events: pd.DataFrame | None = None,
    schedule_dates: pd.DataFrame | None = None,
    date_col_trade: str = "date",
    address_col: str = "address",
    side_col: str = "side",
    size_col: str | None = "size",
    date_col_event: str = "date",
    window_days: int = 1,
    min_abs_size: float = 0.0
) -> pd.DataFrame:
    """
    For each wallet, compute:
      - buy_event_hit_rate   = event_hits / #buy_events
      - buy_trade_hit_rate   = trade_hits / #buy_trades
      - sell_event_hit_rate  = event_hits / #sell_events
      - sell_trade_hit_rate  = trade_hits / #sell_trades

    Notes:
      - Uses the *core* event days (not padded). If you pass schedule_dates, it
        extracts unique dates where cohort=='buy' or 'sell' (treats 'both' as both).
      - Filters trades by |size| >= min_abs_size when size_col is provided.
    """
    if schedule_dates is not None:
        sd = schedule_dates.copy()
        sd[date_col_event] = pd.to_datetime(sd[date_col_event]).dt.normalize()
        buy_ev  = sd.loc[sd["cohort"].isin(["buy","both"]),  date_col_event]
        sell_ev = sd.loc[sd["cohort"].isin(["sell","both"]), date_col_event]
        buy_events_arr  = _to_sorted_dates(buy_ev)
        sell_events_arr = _to_sorted_dates(sell_ev)
    else:
        if buy_events is None or sell_events is None:
            raise ValueError("Provide either schedule_dates with 'cohort' OR both buy_events and sell_events.")
        buy_events_arr  = _to_sorted_dates(buy_events[date_col_event])
        sell_events_arr = _to_sorted_dates(sell_events[date_col_event])

    trades = inferred_trades.copy()
    trades[date_col_trade] = pd.to_datetime(trades[date_col_trade]).dt.normalize()
    trades = trades[trades[side_col].isin(["buy","sell"])]

    if size_col is not None and min_abs_size > 0:
        trades = trades[trades[size_col].abs() >= float(min_abs_size)]

    out_rows = []
    for addr, g in trades.groupby(address_col):
        g_buy  = _to_sorted_dates(g.loc[g[side_col] == "buy",  date_col_trade])
        g_sell = _to_sorted_dates(g.loc[g[side_col] == "sell", date_col_trade])

        b_eh, b_ne, b_th, b_nt = _hit_counts(g_buy,  buy_events_arr,  window_days)
        s_eh, s_ne, s_th, s_nt = _hit_counts(g_sell, sell_events_arr, window_days)

        out_rows.append({
            "address": addr,
            # BUY side
            "buy_events": b_ne,
            "buy_event_hits": b_eh,
            "buy_event_hit_rate": (b_eh / b_ne) if b_ne else np.nan,
            "buy_trades": b_nt,
            "buy_trade_hits": b_th,
            "buy_trade_hit_rate": (b_th / b_nt) if b_nt else np.nan,
            # SELL side
            "sell_events": s_ne,
            "sell_event_hits": s_eh,
            "sell_event_hit_rate": (s_eh / s_ne) if s_ne else np.nan,
            "sell_trades": s_nt,
            "sell_trade_hits": s_th,
            "sell_trade_hit_rate": (s_th / s_nt) if s_nt else np.nan,
            # Simple overall
            "overall_event_hit_rate": (
                (b_eh + s_eh) / (b_ne + s_ne) if (b_ne + s_ne) else np.nan
            ),
            "overall_trade_hit_rate": (
                (b_th + s_th) / (b_nt + s_nt) if (b_nt + s_nt) else np.nan
            ),
        })

    # If some wallets had no trades at all, they won't appear above—add rows if desired:
    if not trades.empty:
        seen = {r["address"] for r in out_rows}
        for addr in trades[address_col].unique():
            if addr not in seen:
                out_rows.append({"address": addr})

    return (pd.DataFrame(out_rows)
              .sort_values(["overall_event_hit_rate","overall_trade_hit_rate"], ascending=False)
              .reset_index(drop=True))


# =============================================================================
# 9. TRUTH CHECK UTILITIES
# =============================================================================

# Manually pulling those 4600 balances
import requests

def get_balance_at_block(address: str, block_number: int, alchemy_api_key: str, timeout: float = 15.0) -> Optional[int]:
    """
    Single-call ETH balance at a given block via Alchemy JSON-RPC.
    Returns int wei, or raises on hard errors.
    """
    url = f"https://eth-mainnet.g.alchemy.com/v2/{alchemy_api_key}"
    payload = {
        "jsonrpc": "2.0",
        "method": "eth_getBalance",
        "params": [address, hex(int(block_number))],
        "id": 1,
    }
    r = requests.post(url, json=payload, timeout=timeout)
    r.raise_for_status()
    data = r.json()
    if "error" in data:
        # Let caller decide how to handle
        raise RuntimeError(data["error"])
    return int(data["result"], 16)


def attach_truth_balances(
    df: pd.DataFrame,
    alchemy_api_key: str,
    *,
    address_col: str = "address",
    block_col: str = "block_number",
    batch_col: str = "balance_wei",
    out_col: str = "balance_wei_truth",
    sleep: float = 0.15,
    retries: int = 3,
    backoff: float = 0.6,
    print_every: int = 20,
    sample_frac: Optional[float] = None,
    random_state: int = 42,
) -> pd.DataFrame:
    """
    Optionally sample a fraction of rows, fetch the ETH balance at (address, block_number)
    sequentially, and attach truth values. Returns only rows with both batch + truth.

    Parameters
    ----------
    df : pd.DataFrame
        Must include address, block_number, and batch balance columns.
    alchemy_api_key : str
        Your Alchemy API key.
    sample_frac : float, optional
        Fraction of rows (0<p<=1) to randomly sample for truth checking. If None,
        all rows are checked.
    random_state : int
        Random seed for reproducible sampling.
    """
    if address_col not in df.columns or block_col not in df.columns:
        raise ValueError(f"DataFrame must include '{address_col}' and '{block_col}'")

    # Step 1: Optionally sample
    work_df = (
        df.sample(frac=sample_frac, random_state=random_state).copy()
        if sample_frac is not None else df.copy()
    )
    work_df[out_col] = pd.NA

    n = len(work_df)
    for i, (idx, row) in enumerate(work_df.iterrows(), start=1):
        addr = str(row[address_col]).strip().lower()
        blk = int(row[block_col])

        bal = None
        for attempt in range(retries + 1):
            try:
                bal = get_balance_at_block(addr, blk, alchemy_api_key)
                break
            except Exception as e:
                if attempt == retries:
                    print(f"\n[warn] Failed {addr} @ {blk}: {e}")
                else:
                    time.sleep(backoff * (2 ** attempt))

        work_df.at[idx, out_col] = bal if bal is not None else pd.NA

        if (i % print_every) == 0 or i == n:
            pct = 100.0 * i / max(1, n)
            print(f"[truth] {i:,}/{n:,} ({pct:5.1f}%)")

        if sleep:
            time.sleep(sleep)

    # Only return rows where we have both batch & truth
    return work_df.dropna(subset=[batch_col, out_col])
