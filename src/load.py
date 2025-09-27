#!/usr/bin/env python3
"""
Carga los CSV de data/processed/ en una base de datos SQLite.
"""

import sqlite3
import pandas as pd
import pathlib

PROC_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "processed"
DB_FILE = pathlib.Path(__file__).resolve().parents[1] / "crypto.sqlite"

def create_tables(conn):
    cur = conn.cursor()
    cur.executescript("""
    CREATE TABLE IF NOT EXISTS coins (
        coin_id TEXT PRIMARY KEY,
        symbol TEXT NOT NULL,
        name TEXT NOT NULL
    );

    CREATE TABLE IF NOT EXISTS market_snapshots (
        snapshot_id TEXT,
        coin_id TEXT,
        price_eur REAL,
        market_cap REAL,
        volume_24h REAL,
        rank INTEGER,
        last_updated TEXT,
        PRIMARY KEY (snapshot_id, coin_id),
        FOREIGN KEY (coin_id) REFERENCES coins (coin_id)
    );

    CREATE TABLE IF NOT EXISTS market_history (
        snapshot_id TEXT,
        coin_id TEXT,
        ts TIMESTAMP,
        price_eur REAL,
        market_cap REAL,
        volume_24h REAL,
        PRIMARY KEY (snapshot_id, coin_id, ts),
        FOREIGN KEY (coin_id) REFERENCES coins (coin_id)
    );
    """)
    conn.commit()

def load_csv_to_table(conn, csv_file, table_name):
    df = pd.read_csv(csv_file)
    df.to_sql(table_name, conn, if_exists="append", index=False)
    print(f"Insertados {len(df)} registros en {table_name}")

def main():
    conn = sqlite3.connect(DB_FILE)
    create_tables(conn)

    # Cargar tabla coins
    coins_file = PROC_DIR / "coins.csv"
    if coins_file.exists():
        load_csv_to_table(conn, coins_file, "coins")

    # Cargar el último snapshot
    snaps = sorted(PROC_DIR.glob("market_snapshots_*.csv"))
    hists = sorted(PROC_DIR.glob("market_history_*.csv"))

    if snaps:
        load_csv_to_table(conn, snaps[-1], "market_snapshots")
    if hists:
        load_csv_to_table(conn, hists[-1], "market_history")

    conn.close()

if __name__ == "__main__":
    main()
