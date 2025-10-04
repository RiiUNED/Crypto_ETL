import streamlit as st
import pandas as pd
import matplotlib.pyplot as plt
from sqlalchemy import create_engine

# --- Config ---
st.set_page_config(page_title="Crypto Dashboard", layout="wide")
st.title("Crypto-ETL Dashboard")

# PostgreSQL connection
DB_USER = "ricardo"
DB_PASS = "crypto"
DB_HOST = "localhost"
DB_PORT = "5432"
DB_NAME = "crypto"
engine = create_engine(f"postgresql+psycopg2://{DB_USER}:{DB_PASS}@{DB_HOST}:{DB_PORT}/{DB_NAME}")

# --- Interactive filters ---
st.sidebar.header("Query Controls")

# Get available coins from DB
coins_query = "SELECT DISTINCT coin_id FROM market_history ORDER BY coin_id"
available_coins = pd.read_sql(coins_query, engine)["coin_id"].tolist()

coin_selected = st.sidebar.selectbox("Select a coin:", available_coins, index=0)
n_records = st.sidebar.slider("Number of records", min_value=50, max_value=1000, step=50, value=200)

# --- Query with filters ---
query = f"""
    SELECT ts, price_eur
    FROM market_history
    WHERE coin_id = '{coin_selected}'
    ORDER BY ts DESC
    LIMIT {n_records}
"""
df = pd.read_sql(query, engine)
df["ts"] = pd.to_datetime(df["ts"], utc=True, errors="coerce")
df = df.sort_values("ts")  # ascending for chart

# --- Display data ---
st.subheader(f"Last {n_records} records for {coin_selected}")
st.dataframe(df)

# --- Plot ---
fig, ax = plt.subplots(figsize=(8,4))
ax.plot(df["ts"], df["price_eur"], marker="o", linestyle="-")
ax.set_title(f"{coin_selected.capitalize()} price (EUR)")
ax.set_xlabel("Timestamp")
ax.set_ylabel("Price (EUR)")
ax.grid(True, alpha=0.3)

left, center, right = st.columns([1,6,1])
with center:
    st.pyplot(fig, use_container_width=True)
