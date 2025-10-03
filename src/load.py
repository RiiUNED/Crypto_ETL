#!/usr/bin/env python3
"""
load.py
Loads processed datasets (Parquet preferred, CSV fallback) into PostgreSQL.
Assumes data is already clean (timestamps normalized, types correct).
"""

import pandas as pd
import pathlib
from sqlalchemy import create_engine, text

PROC_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "processed"

# PostgreSQL connection config
DB_USER = "ricardo"
DB_PASS = "crypto"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "crypto"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Helpers ---
def read_processed(prefix: str):
    """Read latest dataset: prefer Parquet, fallback to CSV."""
    pq_files = sorted(PROC_DIR.glob(f"{prefix}_*.parquet"))
    csv_files = sorted(PROC_DIR.glob(f"{prefix}_*.csv"))

    if pq_files:
        return pd.read_parquet(pq_files[-1]), pq_files[-1].name
    elif csv_files:
        return pd.read_csv(csv_files[-1]), csv_files[-1].name
    else:
        return None, None

# --- Load functions ---
def load_coins(engine):
    coins_file_pq = PROC_DIR / "coins.parquet"
    coins_file_csv = PROC_DIR / "coins.csv"

    if coins_file_pq.exists():
        df = pd.read_parquet(coins_file_pq)
        src = coins_file_pq.name
    elif coins_file_csv.exists():
        df = pd.read_csv(coins_file_csv)
        src = coins_file_csv.name
    else:
        return

    with engine.begin() as conn:
        for _, row in df.iterrows():
            conn.execute(
                text("""
                    INSERT INTO coins (coin_id, symbol, name)
                    VALUES (:coin_id, :symbol, :name)
                    ON CONFLICT (coin_id) DO NOTHING
                """),
                {"coin_id": row["coin_id"], "symbol": row["symbol"], "name": row["name"]}
            )
    print(f"Inserted/updated {len(df)} coins from {src}")

def load_snapshots(engine):
    df, fname = read_processed("market_snapshots")
    if df is None:
        return

    snapshot_id = df["snapshot_id"].iloc[0]
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM market_snapshots WHERE snapshot_id = :sid LIMIT 1"),
            {"sid": snapshot_id}
        ).fetchone()

        if exists:
            print(f"Snapshot {snapshot_id} already loaded, skipped.")
        else:
            df.to_sql("market_snapshots", engine, if_exists="append", index=False)
            print(f"Inserted {len(df)} rows into market_snapshots ({fname})")

def load_history(engine):
    df, fname = read_processed("market_history")
    if df is None:
        return

    inserted_total = 0
    with engine.begin() as conn:
        for coin_id, group in df.groupby("coin_id"):
            result = conn.execute(
                text("SELECT MAX(ts) FROM market_history WHERE coin_id = :cid"),
                {"cid": coin_id}
            ).fetchone()
            max_ts = result[0]

            if max_ts:
                group = group[group["ts"] > max_ts]

            if not group.empty:
                group.to_sql("market_history", engine, if_exists="append", index=False)
                inserted_total += len(group)

    print(f"Inserted {inserted_total} new rows into market_history ({fname})")

# --- Main ---
def main():
    load_coins(engine)
    load_snapshots(engine)
    load_history(engine)

if __name__ == "__main__":
    main()
