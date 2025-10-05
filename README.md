# Crypto ETL Project

## Project Overview
This project implements a complete **ETL pipeline** for cryptocurrency data using the public **CoinGecko API**.

- **Extract**: fetch market snapshots and historical data for the top 5 cryptocurrencies by market capitalization.  
- **Transform**: clean, normalize, and export data into **CSV** and **Parquet** formats.  
- **Load**: store the processed data in **PostgreSQL** (migrated from the initial prototype in SQLite).  
- **Consume**:  
  - Interactive **Streamlit dashboard** (`dashboard/`) connected live to PostgreSQL.  
  - Classic **Python analysis scripts** (`analysis/`) for exploratory work and visualization.  
  - Automated **backup system** for both database and processed data.  

---

## Repository Structure
```
crypto_etl/
├── src/              # ETL logic: extract, transform, and load data
├── data/
│   ├── raw/          # Raw JSON files from CoinGecko API
│   └── processed/    # Cleaned datasets in CSV and Parquet formats
├── analysis/         # Statistical summaries and plots
│   └── plots/        # Generated visualizations
├── dashboard/        # Streamlit application connected to PostgreSQL
├── notebooks/        # Jupyter storytelling and exploratory notebooks
├── backup/           # Backup system for database and processed data
│   ├── db/           # Compressed database backups (.sql.gz)
│   └── data/         # Compressed data backups (.tar.gz)
├── requirements.in   # Base dependencies
├── requirements.txt  # Locked dependencies
└── README.md         # Project documentation
```

---

### Folder overview

- **`src/`** – Contains all ETL scripts (`extract`, `transform`, `load`) that collect, process, and load cryptocurrency data into the database.  
- **`data/`** – Stores both raw API responses and processed data outputs in CSV and Parquet formats.  
- **`analysis/`** – Contains analysis modules that perform statistical summaries and generate plots, stored in `/plots/`.  
- **`dashboard/`** – Interactive Streamlit web app that visualizes live data from PostgreSQL.  
- **`notebooks/`** – Jupyter notebooks used for storytelling, demonstrations, and step-by-step explanations.  
- **`backup/`** – Scripts and directories for creating compressed backups of the database and processed data.  

---

## Setup & Installation

### 1. Clone the repository
```bash
git clone https://github.com/RiiUNED/Crypto_ETL.git
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

### Run the full ETL pipeline (extract → transform → load)
```bash
./src/run_pipeline.sh
```
This script sequentially executes:
- `extract.py` – downloads raw data from CoinGecko  
- `transform.py` – cleans and structures the data (CSV + Parquet)  
- `load.py` – loads the processed data into PostgreSQL  

---

### Run the Streamlit dashboard
```bash
./dashboard/run_app.sh
```
Launches the interactive dashboard connected to PostgreSQL for live visualization and data exploration.

---

### Run backups
```bash
./backup/run_backup.sh
```
Executes:
- a database dump to `/backup/db/`  
- a compressed copy of `/data/` to `/backup/data/`  

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
- Extend dashboard interactivity with user controls (filters, sliders).  
- Enhance the Jupyter storytelling notebook for demonstrations and presentations.  
- Enhance backup retention and cloud storage integration.  

---

## Additional Notes
- Reference currency: EUR.  
- Top 5 coins selected automatically by market cap.  
- ETL automatically creates necessary folders (`data/raw/`, `data/processed/`).  
- `.gitignore` excludes:  
  - Virtual environment (`venv/`)  
  - Jupyter checkpoints (`.ipynb_checkpoints/`)  
  - Database files and backups (`/backup/db/`, `/backup/data/`).  
