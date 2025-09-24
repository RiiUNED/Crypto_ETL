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

def robust_get(url, params=None, max_attempts=5, base_wait=5):
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

def save_json(obj, name):
    """Guarda un objeto JSON en disco con timestamp en el nombre."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fp = RAW_DIR / f"{ts}_{name}.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"Guardado: {fp}")

def main():
    wait_time = 5  # segundos entre peticiones para respetar rate limit

    # Descargar snapshot de markets en EUR
    markets = fetch_markets(vs_currency="eur", per_page=10, page=1)
    save_json(markets, "coins_markets")

    # Seleccionar top 5 por capitalización
    top5 = [coin["id"] for coin in markets[:5]]
    print("Top 5 monedas:", top5)

    # Descargar histórico de esas monedas
    for coin_id in top5:
        chart = fetch_market_chart(coin_id, days=60, vs_currency="eur")
        save_json(chart, f"{coin_id}_market_chart")
        time.sleep(wait_time)  # Respetar rate limit

if __name__ == "__main__":
    main()
