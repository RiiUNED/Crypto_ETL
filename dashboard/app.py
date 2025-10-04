import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine
import pathlib

# --- Configuración de la app ---
st.set_page_config(page_title="Crypto Dashboard", layout="wide")
st.title("Crypto-ETL Dashboard")

# Conexión a PostgreSQL
DB_USER = "ricardo"
DB_PASS = "crypto"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "crypto"
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# Paths
BASE_DIR = pathlib.Path(__file__).resolve().parents[1]
STATS_FILE = BASE_DIR / "analysis" / "summary_stats.csv"

# --- Price Evolution ---
st.header("Normalized Price Evolution (100 = start)")

query = "SELECT coin_id, ts, price_eur FROM market_history"
df = pd.read_sql(query, engine)
df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
df = df.dropna(subset=["ts"])

fig, ax = plt.subplots(figsize=(8,4))
for coin_id, group in df.groupby("coin_id"):
    group = group.sort_values("ts").iloc[::6]  # muestreo para aligerar
    norm_price = group["price_eur"] / group["price_eur"].iloc[0] * 100
    ax.plot(group["ts"], norm_price, label=coin_id)

ax.set_title("Normalized Price Evolution (100 = start)")
ax.set_xlabel("Date")
ax.set_ylabel("Normalized Price")
ax.legend()
ax.grid(True, alpha=0.3)

left, center, right = st.columns([1,6,1])
with center:
    st.pyplot(fig, use_container_width=True)

# --- Sharpe Ratio y Volatility en columnas ---
col1, col2 = st.columns(2)

with col1:
    st.subheader("Sharpe Ratio per Coin")
    df = df.sort_values(["coin_id", "ts"])
    sharpes = {}
    for coin_id, group in df.groupby("coin_id"):
        group = group.sort_values("ts")
        group["return"] = group["price_eur"].pct_change()
        mean_ret = group["return"].mean()
        std_ret = group["return"].std()
        TRADING_DAYS = 365
        if std_ret and std_ret > 0:
            daily_sharpe = mean_ret / std_ret
            sharpes[coin_id] = daily_sharpe * (TRADING_DAYS ** 0.5)
        else:
            sharpes[coin_id] = 0.0

    sr_df = pd.DataFrame.from_dict(sharpes, orient="index", columns=["Sharpe"])

    fig, ax = plt.subplots(figsize=(8,4))
    sr_df["Sharpe"].plot(kind="bar", rot=45, ax=ax)
    ax.set_title("Sharpe Ratio (daily returns)")
    ax.set_ylabel("Sharpe Ratio")

    sub_left, sub_center, sub_right = st.columns([1,6,1])
    with sub_center:
        st.pyplot(fig, use_container_width=True)

with col2:
    st.subheader("Relative Volatility (%)")
    stats = pd.read_csv(STATS_FILE, index_col="coin_id")

    fig, ax = plt.subplots(figsize=(8,4))
    stats["rel_volatility_pct"].plot(kind="bar", rot=45, ax=ax)
    ax.set_title("Relative Volatility (%)")
    ax.set_ylabel("% Volatility")

    sub_left, sub_center, sub_right = st.columns([1,6,1])
    with sub_center:
        st.pyplot(fig, use_container_width=True)
