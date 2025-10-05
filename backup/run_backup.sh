#!/bin/bash

# Master script to run both database and data backups
# Usage: ./run_backup.sh

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"

echo "=== [1/2] Backing up PostgreSQL database ==="
"$PROJECT_DIR/backup_db.sh"

echo ""
echo "=== [2/2] Backing up data directory ==="
"$PROJECT_DIR/backup_data.sh"

echo ""
echo "All backups completed successfully."
