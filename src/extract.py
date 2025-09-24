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
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def fetch_market_chart(coin_id, days=30, vs_currency="usd"):
    """Descarga histórico de precios, volumen y market cap de una moneda."""
    url = f"{BASE_URL}/coins/{coin_id}/market_chart"
    params = {"vs_currency": vs_currency, "days": days}
    resp = requests.get(url, params=params, timeout=30)
    resp.raise_for_status()
    return resp.json()

def save_json(obj, name):
    """Guarda un objeto JSON en disco con timestamp en el nombre."""
    ts = datetime.now(timezone.utc).strftime("%Y%m%dT%H%M%SZ")
    fp = RAW_DIR / f"{ts}_{name}.json"
    with open(fp, "w", encoding="utf-8") as f:
        json.dump(obj, f, ensure_ascii=False, indent=2)
    print(f"Guardado: {fp}")

def main():
    coins = ["bitcoin", "ethereum", "cardano", "solana"]
    
    # Descargar snapshot de markets
    markets = fetch_markets(vs_currency="usd", per_page=50, page=1)
    save_json(markets, "coins_markets")

    # Descargar histórico de algunas monedas
    for coin_id in coins:
        chart = fetch_market_chart(coin_id, days=60, vs_currency="usd")
        save_json(chart, f"{coin_id}_market_chart")
        time.sleep(1)  # Respetar rate limit

if __name__ == "__main__":
    main()
