#!/usr/bin/env python3
"""
transform.py
Transforms extracted JSON files into clean tabular datasets.

Reads the latest snapshot metadata (snapshot_*.json) and generates:
- coins.csv / coins.parquet (accumulated metadata)
- market_snapshots_<snapshot_id>.csv / .parquet
- market_history_<snapshot_id>.csv / .parquet
"""

import pandas as pd
import pathlib
import json

# Paths
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
RAW_DIR = BASE_DIR / "data" / "raw"
PROCESSED_DIR = BASE_DIR / "data" / "processed"
PROCESSED_DIR.mkdir(parents=True, exist_ok=True)

def latest_snapshot():
    """Get the most recent snapshot metadata file."""
    snapshots = sorted(RAW_DIR.glob("snapshot_*.json"))
    if not snapshots:
        raise FileNotFoundError("No snapshot metadata found in data/raw/")
    return snapshots[-1]

def transform(snapshot_file):
    # Load snapshot metadata
    with open(snapshot_file, "r", encoding="utf-8") as f:
        meta = json.load(f)

    snapshot_id = meta["snapshot_id"]
    top5 = set(meta["coins"])

    # --- Market snapshots & coins ---
    markets_file = [f for f in meta["files"] if "coins_markets" in f][0]
    markets = pd.read_json(RAW_DIR / markets_file)

    # Keep only top 5 coins
    markets = markets[markets["id"].isin(top5)]

    # Build market_snapshots dataframe
    df_snap = markets.rename(
        columns={
            "id": "coin_id",
            "current_price": "price_eur",
            "market_cap": "market_cap",
            "total_volume": "volume_24h",
            "market_cap_rank": "rank",
            "last_updated": "last_updated",
        }
    )[["coin_id", "price_eur", "market_cap", "volume_24h", "rank", "last_updated"]]
    df_snap.insert(0, "snapshot_id", snapshot_id)

    # Save market_snapshots CSV + Parquet
    snap_csv = PROCESSED_DIR / f"market_snapshots_{snapshot_id}.csv"
    snap_pq = PROCESSED_DIR / f"market_snapshots_{snapshot_id}.parquet"
    df_snap.to_csv(snap_csv, index=False)
    df_snap.to_parquet(snap_pq, index=False)
    print(f"Saved {len(df_snap)} market_snapshots → {snap_csv.name}, {snap_pq.name}")

    # Build/update coins dataframe
    df_coins = markets[["id", "symbol", "name"]].rename(columns={"id": "coin_id"})
    coins_csv = PROCESSED_DIR / "coins.csv"
    coins_pq = PROCESSED_DIR / "coins.parquet"

    if coins_csv.exists():
        existing = pd.read_csv(coins_csv)
        df_coins = pd.concat([existing, df_coins]).drop_duplicates("coin_id", keep="last")

    df_coins.to_csv(coins_csv, index=False)
    df_coins.to_parquet(coins_pq, index=False)
    print(f"Saved {len(df_coins)} coins → {coins_csv.name}, {coins_pq.name}")

    # --- Market history ---
    dfs = []
    for fname in meta["files"]:
        if "market_chart" in fname:
            coin_id = fname.split("_market_chart.json")[0].split("_")[-1]
            df_raw = pd.read_json(RAW_DIR / fname)

            df_hist = pd.DataFrame({
                "ts": [pd.to_datetime(x[0], unit="ms", utc=True).tz_convert(None) for x in df_raw["prices"]],
                "price_eur": [x[1] for x in df_raw["prices"]],
                "market_cap": [x[1] for x in df_raw["market_caps"]],
                "volume_24h": [x[1] for x in df_raw["total_volumes"]],
            })
            df_hist.insert(0, "coin_id", coin_id)
            df_hist["snapshot_id"] = snapshot_id
            dfs.append(df_hist)

    df_history = pd.concat(dfs, ignore_index=True)

    hist_csv = PROCESSED_DIR / f"market_history_{snapshot_id}.csv"
    hist_pq = PROCESSED_DIR / f"market_history_{snapshot_id}.parquet"

    df_history.to_csv(hist_csv, index=False)
    df_history.to_parquet(hist_pq, index=False)
    print(f"Saved {len(df_history)} market_history → {hist_csv.name}, {hist_pq.name}")

def main():
    snapshot_file = latest_snapshot()
    transform(snapshot_file)

if __name__ == "__main__":
    main()
