#!/usr/bin/env python3
"""
Carga los CSV de data/processed/ en una base de datos PostgreSQL.
Evita duplicados en market_history: solo inserta días nuevos.
"""

import pandas as pd
import pathlib
from sqlalchemy import create_engine, text

PROC_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "processed"

# Configuración conexión PostgreSQL
DB_USER = "ricardo"
DB_PASS = "crypto"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "crypto"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def load_coins(engine):
    coins_file = PROC_DIR / "coins.csv"
    if coins_file.exists():
        df = pd.read_csv(coins_file)
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
        print(f"Insertadas/actualizadas {len(df)} monedas en coins")

def load_snapshots(engine):
    snaps = sorted(PROC_DIR.glob("market_snapshots_*.csv"))
    if not snaps:
        return
    latest_file = snaps[-1]
    latest = pd.read_csv(latest_file)

    snapshot_id = latest["snapshot_id"].iloc[0]
    with engine.begin() as conn:
        exists = conn.execute(
            text("SELECT 1 FROM market_snapshots WHERE snapshot_id = :sid LIMIT 1"),
            {"sid": snapshot_id}
        ).fetchone()

        if exists:
            print(f"Snapshot {snapshot_id} ya cargado, se omite.")
        else:
            latest.to_sql("market_snapshots", engine, if_exists="append", index=False)
            print(f"Insertados {len(latest)} registros en market_snapshots ({latest_file.name})")

def load_history(engine):
    hists = sorted(PROC_DIR.glob("market_history_*.csv"))
    if not hists:
        return
    latest_file = hists[-1]
    df = pd.read_csv(latest_file)
    df["ts"] = pd.to_datetime(df["ts"], format="ISO8601", errors="coerce")
    df["ts"] = df["ts"].dt.tz_convert("UTC").dt.tz_localize(None)
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

    print(f"Insertados {inserted_total} registros nuevos en market_history ({latest_file.name})")

def main():
    load_coins(engine)
    load_snapshots(engine)
    load_history(engine)

if __name__ == "__main__":
    main()
