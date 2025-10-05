#!/bin/bash

# Backup script for PostgreSQL database
# Saves compressed .sql.gz files in backup/db/
# Usage: ./backup_db.sh

PROJECT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
BACKUP_DIR="$PROJECT_DIR/backup/db"
DB_NAME="crypto"
DB_USER="ricardo"

mkdir -p "$BACKUP_DIR"
TIMESTAMP=$(date +"%Y-%m-%d_%H-%M")

echo "Creating database backup for '$DB_NAME'..."
pg_dump -U "$DB_USER" "$DB_NAME" | gzip > "$BACKUP_DIR/${DB_NAME}_${TIMESTAMP}.sql.gz"

if [ $? -eq 0 ]; then
    echo "Database backup completed: $BACKUP_DIR/${DB_NAME}_${TIMESTAMP}.sql.gz"
else
    echo "Database backup failed."
    exit 1
fi

# Optional: keep only last 10 backups
ls -t "$BACKUP_DIR"/crypto_*.sql.gz | tail -n +11 | xargs -r rm --
