#!/bin/bash
# Relationship Engine — Database Backup Script
# Creates timestamped copy of the SQLite database in backups/
#
# Usage:
#   bash backup.sh                # standard backup
#   bash backup.sh --compress     # backup + gzip compression
#
# Recommended: run daily via crontab
#   0 2 * * * cd ~/relationship_engine && bash backup.sh --compress

set -e

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
DB_PATH="$SCRIPT_DIR/private_data/relationship_engine.db"
BACKUP_DIR="$SCRIPT_DIR/backups"
TIMESTAMP=$(date +"%Y%m%d_%H%M%S")
BACKUP_FILE="$BACKUP_DIR/relationship_engine_${TIMESTAMP}.db"

# Create backup directory
mkdir -p "$BACKUP_DIR"

# Verify source DB exists
if [ ! -f "$DB_PATH" ]; then
    echo "ERROR: Database not found at $DB_PATH"
    exit 1
fi

# Get DB size
DB_SIZE=$(du -h "$DB_PATH" | cut -f1)

# Use SQLite backup command for consistency (handles WAL mode)
if command -v sqlite3 &>/dev/null; then
    sqlite3 "$DB_PATH" ".backup '$BACKUP_FILE'"
else
    # Fallback: simple copy
    cp "$DB_PATH" "$BACKUP_FILE"
fi

echo "Backup created: $BACKUP_FILE ($DB_SIZE)"

# Optional compression
if [ "$1" = "--compress" ]; then
    gzip "$BACKUP_FILE"
    COMPRESSED_SIZE=$(du -h "${BACKUP_FILE}.gz" | cut -f1)
    echo "Compressed: ${BACKUP_FILE}.gz ($COMPRESSED_SIZE)"
fi

# Prune old backups (keep last 30)
BACKUP_COUNT=$(ls -1 "$BACKUP_DIR"/relationship_engine_*.db* 2>/dev/null | wc -l)
if [ "$BACKUP_COUNT" -gt 30 ]; then
    REMOVE_COUNT=$((BACKUP_COUNT - 30))
    ls -1t "$BACKUP_DIR"/relationship_engine_*.db* | tail -n "$REMOVE_COUNT" | xargs rm -f
    echo "Pruned $REMOVE_COUNT old backups (keeping 30)."
fi

echo "Done. Total backups: $(ls -1 "$BACKUP_DIR"/relationship_engine_*.db* 2>/dev/null | wc -l)"
