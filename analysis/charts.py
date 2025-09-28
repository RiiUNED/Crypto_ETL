#!/usr/bin/env python3
"""
charts.py
Genera gráficos a partir de los resultados de summary.py (agregados)
y de la BBDD (series históricas).
"""

import sqlite3
import pandas as pd
import matplotlib
matplotlib.use("Agg")  # backend no interactivo
import matplotlib.pyplot as plt
import pathlib
import sys

# Paths
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
DB_FILE = BASE_DIR / "db" / "crypto.sqlite"
STATS_FILE = BASE_DIR / "analysis" / "summary_stats.csv"
PLOT_DIR = BASE_DIR / "analysis" / "plots"
PLOT_DIR.mkdir(parents=True, exist_ok=True)

# --- Gráfico temporal ---
def plot_price_evolution():
    """Evolución temporal de precios normalizados (100 = valor inicial)"""
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT coin_id, ts, price_eur FROM market_history", conn)
    conn.close()

    # Convertir a datetime
    df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")

    plt.figure(figsize=(10,6))
    for coin_id, group in df.groupby("coin_id"):
        group = group.sort_values("ts")
        group = group.iloc[::6]  # muestreo: 1 de cada 6 puntos (~4 al día)
        norm_price = group["price_eur"] / group["price_eur"].iloc[0] * 100
        plt.plot(group["ts"], norm_price, label=coin_id)

    plt.title("Evolución normalizada de precios (100 = inicio)")
    plt.xlabel("Fecha")
    plt.ylabel("Precio normalizado")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(PLOT_DIR / "price_evolution.png")
    plt.close("all")

# --- Gráfico de Sharpe Ratio ---
def plot_sharpe_ratio():
    """Sharpe Ratio por moneda (basado en retornos diarios)"""
    conn = sqlite3.connect(DB_FILE)
    df = pd.read_sql("SELECT coin_id, ts, price_eur FROM market_history", conn)
    conn.close()

    # Convertir a datetime y ordenar
    df["ts"] = pd.to_datetime(df["ts"], utc=True, format="ISO8601")
    df = df.sort_values(["coin_id", "ts"])

    sharpes = {}
    for coin_id, group in df.groupby("coin_id"):
        group = group.sort_values("ts")
        group["return"] = group["price_eur"].pct_change()
        mean_ret = group["return"].mean()
        std_ret = group["return"].std()
        TRADING_DAYS = 365  # cripto cotiza todos los días

        if std_ret and std_ret > 0:
            daily_sharpe = mean_ret / std_ret
            sharpes[coin_id] = daily_sharpe * (TRADING_DAYS ** 0.5)
        else:
            sharpes[coin_id] = 0.0

    sr_df = pd.DataFrame.from_dict(sharpes, orient="index", columns=["sharpe"])

    sr_df["sharpe"].plot(kind="bar", figsize=(8,6), rot=45)
    plt.title("Sharpe Ratio por moneda (retornos diarios)")
    plt.ylabel("Sharpe Ratio")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / "sharpe_ratio.png")
    plt.close("all")

# --- Gráfico de volatilidad ---
def plot_volatility(stats):
    """Volatilidad relativa (%) por moneda"""
    stats["rel_volatility_pct"].plot(kind="bar", figsize=(8,6), rot=45)
    plt.title("Volatilidad relativa (%)")
    plt.ylabel("% volatilidad")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / "rel_volatility.png")
    plt.close("all")

def main():
    # Cargar estadísticas de summary.py
    stats = pd.read_csv(STATS_FILE, index_col="coin_id")

    # Generar gráficos
    plot_price_evolution()   # BBDD
    plot_sharpe_ratio()      # BBDD
    plot_volatility(stats)   # CSV

    print(f"Gráficos guardados en: {PLOT_DIR}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
