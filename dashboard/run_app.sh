#!/bin/bash

# Script to launch the Streamlit app from the virtual environment
# Usage: ./run_app.sh

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
VENV_STREAMLIT="$PROJECT_DIR/venv/bin/streamlit"

echo "Starting Crypto-ETL Dashboard..."
$VENV_STREAMLIT run "$PROJECT_DIR/dashboard/app.py"
