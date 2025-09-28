#!/bin/bash
# Script maestro para ejecutar análisis y gráficos usando el venv

set -e

PROJECT_DIR=$(dirname "$(dirname "$(realpath "$0")")")
ANALYSIS_DIR="$PROJECT_DIR/analysis"
VENV_PYTHON="$PROJECT_DIR/venv/bin/python"

echo "=== [1/2] Ejecutando summary.py..."
$VENV_PYTHON "$ANALYSIS_DIR/summary.py"

echo "=== [2/2] Ejecutando charts.py..."
$VENV_PYTHON "$ANALYSIS_DIR/charts.py"

echo "=== Análisis completado con éxito ==="
