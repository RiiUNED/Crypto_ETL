#!/usr/bin/env python3
"""
Transforma datos crudos de CoinGecko (JSON en data/raw/)
a datos procesados (CSV en data/processed/).
"""

import json
import pathlib
import pandas as pd

RAW_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "raw"
PROC_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "processed"
PROC_DIR.mkdir(parents=True, exist_ok=True)

def transform_snapshot(snapshot_file):
    # Cargar metadatos
    with open(snapshot_file, "r", encoding="utf-8") as f:
        meta = json.load(f)
    snapshot_id = meta["snapshot_id"]
    top5 = set(meta["coins"])  # monedas a conservar

    # === Transformar coins_markets → market_snapshots ===
    markets_file = [f for f in meta["files"] if "coins_markets" in f][0]
    with open(RAW_DIR / markets_file, "r", encoding="utf-8") as f:
        markets = json.load(f)

    # Filtrar solo las monedas del top 5
    markets = [coin for coin in markets if coin["id"] in top5]

    rows = []
    for coin in markets:
        row = {
            "snapshot_id": snapshot_id,
            "coin_id": coin["id"],
            "symbol": coin["symbol"],
            "name": coin["name"],
            "price_eur": coin["current_price"],
            "market_cap": coin["market_cap"],
            "volume_24h": coin["total_volume"],
            "rank": coin["market_cap_rank"],
            "last_updated": coin["last_updated"]
        }
        rows.append(row)

    df_snapshots = pd.DataFrame(rows)

    out_snap = PROC_DIR / f"market_snapshots_{snapshot_id}.csv"
    df_snapshots.to_csv(out_snap, index=False)
    print(f"Guardado: {out_snap}")

    # === Opcional: tabla coins de referencia (solo top 5 acumulados) ===
    coins_file = PROC_DIR / "coins.csv"
    df_coins = df_snapshots[["coin_id", "symbol", "name"]].drop_duplicates()

    if coins_file.exists():
        df_existing = pd.read_csv(coins_file)
        df_coins = pd.concat([df_existing, df_coins]).drop_duplicates()
    df_coins.to_csv(coins_file, index=False)
    print(f"Guardado: {coins_file}")

    # === Transformar históricos → market_history ===
    dfs = []
    for fname in meta["files"]:
        if "market_chart" not in fname:
            continue
        # ejemplo de fname: 20250924T131500Z_20250924T131503Z_bitcoin_market_chart.json
        coin_id = fname.split("_")[2]
        with open(RAW_DIR / fname, "r", encoding="utf-8") as f:
            chart = json.load(f)

        df = pd.DataFrame({
            "ts": [pd.to_datetime(x[0], unit="ms", utc=True) for x in chart["prices"]],
            "price_eur": [x[1] for x in chart["prices"]],
            "market_cap": [x[1] for x in chart["market_caps"]],
            "volume_24h": [x[1] for x in chart["total_volumes"]],
        })
        df["coin_id"] = coin_id
        df["snapshot_id"] = snapshot_id
        dfs.append(df)

    df_history = pd.concat(dfs, ignore_index=True)
    out_hist = PROC_DIR / f"market_history_{snapshot_id}.csv"
    df_history.to_csv(out_hist, index=False)
    print(f"Guardado: {out_hist}")

if __name__ == "__main__":
    # Tomar el snapshot más reciente en data/raw/
    latest_meta = sorted(RAW_DIR.glob("snapshot_*.json"))[-1]
    print(f"Procesando snapshot: {latest_meta}")
    transform_snapshot(latest_meta)
