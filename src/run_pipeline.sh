#!/bin/bash
# Script maestro para ejecutar el pipeline completo usando el venv

set -e

PROJECT_DIR=$(dirname "$(dirname "$(realpath "$0")")")
SRC_DIR="$PROJECT_DIR/src"
VENV_PYTHON="$PROJECT_DIR/venv/bin/python"

echo "=== [1/3] Extrayendo datos..."
$VENV_PYTHON "$SRC_DIR/extract.py"

echo "=== [2/3] Transformando datos..."
$VENV_PYTHON "$SRC_DIR/transform.py"

echo "=== [3/3] Cargando en la BBDD..."
$VENV_PYTHON "$SRC_DIR/load.py"

echo "=== Pipeline completado con éxito ==="
