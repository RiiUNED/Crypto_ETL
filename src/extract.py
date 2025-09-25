#!/usr/bin/env python3
"""
Script de extracción de datos desde CoinGecko API.
Guarda los resultados en data/raw/ con un timestamp.
"""

import requests
import json
import time
import pathlib
from datetime import datetime, timezone

# Carpeta donde se guardarán los JSON crudos
RAW_DIR = pathlib.Path(__file__).resolve().parents[1] / "data" / "raw"
RAW_DIR.mkdir(parents=True, exist_ok=True)

BASE_URL = "https://api.coingecko.com/api/v3"

def robust_get(url, params=None, max_attempts=10, base_wait=30):
    """Hace una petición GET con reintentos en caso de 429 (rate limit)."""
    for attempt in range(max_attempts):
        resp = requests.get(url, params=params, timeout=30)
        if resp.status_code == 429:
            wait = (attempt + 1) * base_wait
            print(f"Rate limit hit. Retrying in {wait}s...")
            time.sleep(wait)
            continue
        resp.raise_for_status()
        return resp.json()
    raise Exception(f"Failed to fetch {url} after {max_attempts} attempts")

def fetch_markets(vs_currency="eur", per_page=10, page=1):
    """Descarga el listado de monedas ordenadas por capitalización."""
    url = f"{BASE_URL}/coins/markets"
    params = {
        "vs_currency": vs_currency,
        "order": "market_cap_desc",
        "per_page": per_page,
        "page": page,
        "price_change_percentage": "24h"
    }
    return robust_get(url, params=params)

def fetch_market_chart(coin_id, days=30, vs_currency="eur"):
    """Descarga histórico de precios, volumen y market cap de una moneda."""
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days}
    return robust_get(url, params=params)

def save_json(obj, name, snapshot_id):
    """Guarda un objeto JSON en disco con snapshot_id + timestamp."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    filename = f"{snapshot_id}_{ts}_{name}.json"
    fp = RAW_DIR / filename
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"Guardado: {fp}")
    return str(fp.name)

def main():
    wait_time = 15  # segundos entre peticiones para respetar rate limit

    # Crear snapshot_id único al inicio
    snapshot_id = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    print(f"=== SNAPSHOT ID: {snapshot_id} ===")

    metadata = {
        "snapshot_id": snapshot_id,
        "ts_start": datetime.now(timezone.utc).isoformat(),
        "coins": [],
        "files": []
    }

    # Descargar snapshot de markets en EUR
    markets = fetch_markets(vs_currency="eur", per_page=10, page=1)
    fname = save_json(markets, "coins_markets", snapshot_id)
    metadata["files"].append(fname)

    # Seleccionar top 5 por capitalización
    top5 = [coin["id"] for coin in markets[:5]]
    metadata["coins"] = top5
    print("Top 5 monedas:", top5)

    # Descargar histórico de esas monedas
    for coin_id in top5:
        chart = fetch_market_chart(coin_id, days=60, vs_currency="eur")
        fname = save_json(chart, f"{coin_id}_market_chart", snapshot_id)
        metadata["files"].append(fname)
        time.sleep(wait_time)  # Respetar rate limit

    # Guardar metadatos del snapshot
    meta_file = RAW_DIR / f"snapshot_{snapshot_id}.json"
    with open(meta_file, "w", encoding="utf-8") as f:
        json.dump(metadata, f, ensure_ascii=False, indent=2)
    print(f"Metadatos guardados en: {meta_file}")

if __name__ == "__main__":
    main()
