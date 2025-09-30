#!/usr/bin/env python3
"""
Exploratory analysis of the PostgreSQL crypto database.
Computes descriptive statistics per coin (mean, variance, stddev, growth, time range).
Includes relative volatility (coefficient of variation).
"""

import pandas as pd
import pathlib
from sqlalchemy import create_engine

# Paths
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
OUT_FILE = BASE_DIR / "analysis" / "summary_stats.csv"

# PostgreSQL connection config
DB_USER = "ricardo"
DB_PASS = "crypto"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "crypto"

engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

def main():
    # Load historical data into pandas
    query = "SELECT coin_id, ts, price_eur FROM market_history"
    df = pd.read_sql(query, engine)

    # Calculate metrics per coin
    stats = df.groupby("coin_id")["price_eur"].agg(
        avg_price="mean",
        variance="var",
        stddev="std",
        min_price="min",
        max_price="max"
    )

    # Percentage growth
    stats["pct_growth"] = (stats["max_price"] - stats["min_price"]) / stats["min_price"] * 100

    # Relative volatility (coefficient of variation)
    stats["rel_volatility_pct"] = (stats["stddev"] / stats["avg_price"]) * 100

    # Time range
    ranges = df.groupby("coin_id")["ts"].agg(start_date="min", end_date="max")

    # Merge metrics and ranges
    result = stats.join(ranges)

    # Show in console
    print("\n=== Statistics per coin ===")
    print(result)

    # Save to CSV
    OUT_FILE.parent.mkdir(parents=True, exist_ok=True)
    result.to_csv(OUT_FILE, index=True)
    print(f"\nResults saved in: {OUT_FILE}")

if __name__ == "__main__":
    main()
