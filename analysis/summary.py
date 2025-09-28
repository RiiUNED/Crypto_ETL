#!/usr/bin/env python3
"""
Análisis exploratorio básico de la base de datos crypto.sqlite.
Calcula estadísticas descriptivas por moneda (media, varianza, desviación estándar, crecimiento, rango temporal).
Incluye volatilidad relativa (coeficiente de variación).
"""

import sqlite3
import pandas as pd
import pathlib

# Rutas
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DB_FILE = BASE_DIR / "db" / "crypto.sqlite"
OUT_FILE = BASE_DIR / "analysis" / "summary_stats.csv"

def main():
    # Conectar a la base de datos
    conn = sqlite3.connect(DB_FILE)

    # Cargar históricos en pandas
    df = pd.read_sql("SELECT coin_id, ts, price_eur FROM market_history", conn)

    # Calcular métricas por moneda
    stats = df.groupby("coin_id")["price_eur"].agg(
        avg_price="mean",
        variance="var",
        stddev="std",
        min_price="min",
        max_price="max"
    )

    # Crecimiento porcentual
    stats["pct_growth"] = (stats["max_price"] - stats["min_price"]) / stats["min_price"] * 100

    # Volatilidad relativa (coeficiente de variación)
    stats["rel_volatility_pct"] = (stats["stddev"] / stats["avg_price"]) * 100

    # Rango temporal
    ranges = df.groupby("coin_id")["ts"].agg(start_date="min", end_date="max")

    # Unir métricas y rangos
    result = stats.join(ranges)

    # Mostrar en consola
    print("\n=== Estadísticas por moneda ===")
    print(result)

    # Guardar en CSV
    result.to_csv(OUT_FILE, index=True)
    print(f"\nResultados guardados en: {OUT_FILE}")

    conn.close()

if __name__ == "__main__":
    main()
