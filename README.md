# Crypto ETL Project

## Project Overview
This project implements a complete **ETL pipeline** for cryptocurrency data using the public **CoinGecko API**.

- **Extract**: fetch market snapshots and historical data for the top 5 cryptocurrencies by market capitalization.  
- **Transform**: clean, normalize, and export data into **CSV** and **Parquet** formats.  
- **Load**: store the processed data in **PostgreSQL** (migrated from the initial prototype in SQLite).  
- **Consume**:  
  - Interactive **Streamlit dashboard** (`dashboard/`) connected live to PostgreSQL.  
  - Classic **Python analysis scripts** (`analysis/`) for exploratory work.  

---

## Repository Structure
```
crypto_etl/
├── src/              # ETL scripts (extract, transform, load)
├── data/
│   ├── raw/          # Raw JSON files from CoinGecko API
│   └── processed/    # Cleaned data (CSV + Parquet)
├── analysis/         # Analysis scripts (summary.py, charts.py)
├── dashboard/        # Streamlit app (app.py + queries page)
├── notebooks/        # Jupyter storytelling notebook
├── requirements.in   # Base dependencies
├── requirements.txt  # Locked dependencies
└── README.md         # Project documentation
```

---

## Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/RiiUNED/crypto_etl.git
cd crypto_etl
```

### 2. Create and activate virtual environment
```bash
python3 -m venv venv
source venv/bin/activate
```

### 3. Install dependencies
```bash
pip install -r requirements.txt
```

### 4. PostgreSQL Setup (WSL)
Start PostgreSQL and create the database:
```bash
sudo service postgresql start
psql -U postgres -c "CREATE DATABASE crypto OWNER ricardo;"
```

---

## Usage

### Extract raw data
```bash
python src/extract.py
```

### Transform raw → processed (CSV & Parquet)
```bash
python src/transform.py
```

### Load into PostgreSQL
```bash
python src/load.py
```

### Run interactive dashboard
```bash
streamlit run dashboard/app.py
```

---

## Data Description

- **Daily snapshot (`coins_markets.json`)**  
  Contains EUR-based stats for the top 5 cryptocurrencies:  
  - `id`, `symbol`, `name`, `current_price`, `market_cap`, `total_volume`, `rank`, etc.  

- **Historical data (`*_market_chart.json`)**  
  Time series of:  
  - `price_eur`, `market_cap`, `volume_24h`  
  - Timestamps in UTC, format `[timestamp, value]`.  

---

## Analysis & Visualization
- Volatility ranking  
- Correlations between assets  
- Rolling averages  
- Sharpe ratios  
- Interactive dashboard with live PostgreSQL queries  

---

## Roadmap
- Extend the dashboard with filters and sliders for live queries.  
- Add a Jupyter storytelling notebook for portfolio presentation.  
- Explore basic ML models (linear regression, ARIMA) for price predictions.  

---

## Additional Notes
- Reference currency: EUR.  
- Top 5 coins selected automatically by market cap.  
- ETL automatically creates necessary folders (`data/raw/`, `data/processed/`).  
- `.gitignore` excludes:  
  - Virtual environment (`venv/`)  
  - Jupyter checkpoints (`.ipynb_checkpoints/`)  
  - Database files (e.g., SQLite prototype).  
