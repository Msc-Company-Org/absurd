#!/bin/sh
# provision.sh — Create and seed the absurd demo database
# Usage: ./provision.sh
#
# Requires: dropdb, createdb, psql (PostgreSQL client tools)
set -euo pipefail

dbname="${DB_NAME:-absurd}"
sql_dir="$(cd "$(dirname "$0")/../../sql" && pwd)"

dropdb "$dbname" 2>/dev/null || true
createdb "$dbname"
psql -d "$dbname" -f "$sql_dir/absurd.sql"

echo "Database '$dbname' provisioned successfully."
