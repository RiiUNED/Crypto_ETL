#!/bin/bash

# Backup script for the /data directory
# Compresses the folder and deletes the original files afterward
# Saves compressed .tar.gz files in backup/data/
# Usage: ./backup_data.sh

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
DATA_DIR="$PROJECT_DIR/data"
BACKUP_DIR="$PROJECT_DIR/backup/data"

mkdir -p "$BACKUP_DIR"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M")

echo "Creating backup of /data directory..."
tar -czf "$BACKUP_DIR/data_${TIMESTAMP}.tar.gz" -C "$PROJECT_DIR" data

if [ $? -eq 0 ]; then
    echo "Data backup completed: $BACKUP_DIR/data_${TIMESTAMP}.tar.gz"
    echo "Deleting original files in /data..."
    rm -rf "$DATA_DIR"/*
    echo "Original data files deleted."
else
    echo "Data backup failed. Original files were NOT deleted."
    exit 1
fi

# Optional: keep only last 10 backups
ls -t "$BACKUP_DIR"/data_*.tar.gz | tail -n +11 | xargs -r rm --
