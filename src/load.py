#!/usr/bin/env python3
"""
Carga los CSV de data/processed/ en una base de datos SQLite.
Evita duplicados en market_history: solo inserta días nuevos.
"""

import sqlite3
import pandas as pd
import pathlib

PROC_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "processed"
DB_DIR = pathlib.Path(__file__).resolve().parents[1] / "db"
DB_DIR.mkdir(exist_ok=True)
DB_FILE = DB_DIR / "crypto.sqlite"

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
        coin_id TEXT,
        ts TIMESTAMP,
        price_eur REAL,
        market_cap REAL,
        volume_24h REAL,
        snapshot_id TEXT,
        PRIMARY KEY (coin_id, ts),
        FOREIGN KEY (coin_id) REFERENCES coins (coin_id)
    );
    """)
    conn.commit()

def load_coins(conn):
    coins_file = PROC_DIR / "coins.csv"
    if coins_file.exists():
        df = pd.read_csv(coins_file)
        cur = conn.cursor()
        for _, row in df.iterrows():
            cur.execute(
                "INSERT OR IGNORE INTO coins (coin_id, symbol, name) VALUES (?, ?, ?)",
                (row["coin_id"], row["symbol"], row["name"])
            )
        conn.commit()
        print(f"Insertadas/actualizadas {len(df)} monedas en coins")

def load_snapshots(conn):
    snaps = sorted(PROC_DIR.glob("market_snapshots_*.csv"))
    if not snaps:
        return
    latest = pd.read_csv(snaps[-1])
    latest.to_sql("market_snapshots", conn, if_exists="append", index=False)
    print(f"Insertados {len(latest)} registros en market_snapshots ({snaps[-1].name})")

def load_history(conn):
    hists = sorted(PROC_DIR.glob("market_history_*.csv"))
    if not hists:
        return
    latest_file = hists[-1]
    df = pd.read_csv(latest_file)
    inserted_total = 0

    cur = conn.cursor()
    for coin_id, group in df.groupby("coin_id"):
        # Obtener último timestamp ya cargado para la moneda
        cur.execute("SELECT MAX(ts) FROM market_history WHERE coin_id = ?", (coin_id,))
        result = cur.fetchone()
        max_ts = result[0]

        # Filtrar solo filas nuevas
        if max_ts:
            group = group[group["ts"] > max_ts]

        if not group.empty:
            group.to_sql("market_history", conn, if_exists="append", index=False)
            inserted_total += len(group)

    print(f"Insertados {inserted_total} registros nuevos en market_history ({latest_file.name})")

def main():
    conn = sqlite3.connect(DB_FILE)
    create_tables(conn)
    load_coins(conn)
    load_snapshots(conn)
    load_history(conn)
    conn.close()

if __name__ == "__main__":
    main()
