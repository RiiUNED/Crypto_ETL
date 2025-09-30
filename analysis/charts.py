#!/usr/bin/env python3
"""
charts.py
Generates charts from summary.py (aggregated stats)
and from PostgreSQL (historical series).
"""

import pandas as pd
import matplotlib
matplotlib.use("Agg")  # non-interactive backend
import matplotlib.pyplot as plt
import pathlib
import sys
from sqlalchemy import create_engine

# Paths
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
STATS_FILE = BASE_DIR / "analysis" / "summary_stats.csv"
PLOT_DIR = BASE_DIR / "analysis" / "plots"
PLOT_DIR.mkdir(parents=True, exist_ok=True)

# PostgreSQL connection config
DB_USER = "ricardo"
DB_PASS = "crypto"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "crypto"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Time series chart ---
def plot_price_evolution():
    """Normalized price evolution (100 = initial value)"""
    query = "SELECT coin_id, ts, price_eur FROM market_history"
    df = pd.read_sql(query, engine)

    # Convert to datetime
    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])

    plt.figure(figsize=(10,6))
    for coin_id, group in df.groupby("coin_id"):
        group = group.sort_values("ts")
        group = group.iloc[::6]  # sampling: 1 of every 6 points (~4 per day)
        norm_price = group["price_eur"] / group["price_eur"].iloc[0] * 100
        plt.plot(group["ts"], norm_price, label=coin_id)

    plt.title("Normalized Price Evolution (100 = start)")
    plt.xlabel("Date")
    plt.ylabel("Normalized Price")
    plt.legend()
    plt.grid(True, alpha=0.3)
    plt.tight_layout()
    plt.savefig(PLOT_DIR / "price_evolution.png")
    plt.close("all")

# --- Sharpe Ratio chart ---
def plot_sharpe_ratio():
    """Sharpe Ratio per coin (based on daily returns)"""
    query = "SELECT coin_id, ts, price_eur FROM market_history"
    df = pd.read_sql(query, engine)

    df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
    df = df.dropna(subset=["ts"])
    df = df.sort_values(["coin_id", "ts"])

    sharpes = {}
    for coin_id, group in df.groupby("coin_id"):
        group = group.sort_values("ts")
        group["return"] = group["price_eur"].pct_change()
        mean_ret = group["return"].mean()
        std_ret = group["return"].std()
        TRADING_DAYS = 365  # crypto trades every day

        if std_ret and std_ret > 0:
            daily_sharpe = mean_ret / std_ret
            sharpes[coin_id] = daily_sharpe * (TRADING_DAYS ** 0.5)
        else:
            sharpes[coin_id] = 0.0

    sr_df = pd.DataFrame.from_dict(sharpes, orient="index", columns=["sharpe"])

    sr_df["sharpe"].plot(kind="bar", figsize=(8,6), rot=45)
    plt.title("Sharpe Ratio per coin (daily returns)")
    plt.ylabel("Sharpe Ratio")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / "sharpe_ratio.png")
    plt.close("all")

# --- Volatility chart ---
def plot_volatility(stats):
    """Relative volatility (%) per coin"""
    stats["rel_volatility_pct"].plot(kind="bar", figsize=(8,6), rot=45)
    plt.title("Relative Volatility (%)")
    plt.ylabel("% Volatility")
    plt.tight_layout()
    plt.savefig(PLOT_DIR / "rel_volatility.png")
    plt.close("all")

def main():
    # Load summary stats
    stats = pd.read_csv(STATS_FILE, index_col="coin_id")

    # Generate charts
    plot_price_evolution()   # PostgreSQL
    plot_sharpe_ratio()      # PostgreSQL
    plot_volatility(stats)   # CSV

    print(f"Charts saved in: {PLOT_DIR}")
    return 0

if __name__ == "__main__":
    sys.exit(main())
