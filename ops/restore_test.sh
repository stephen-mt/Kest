#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
set -a
source .env
set +a

latest_file="$repo_dir/.runtime/backups/LATEST"
test -s "$latest_file" || { echo "No backup found; run make backup first" >&2; exit 2; }
backup_dir="$(head -n 1 "$latest_file")"
test -d "$backup_dir" || { echo "Backup directory is missing: $backup_dir" >&2; exit 2; }
docker compose --env-file .env -f compose.yaml run --rm tooling \
  python /opt/kest/ops/recovery.py verify-checksums "/opt/kest/.runtime/backups/$(basename "$backup_dir")"

backup_id="$(python3 -c "import json; print(json.load(open('$backup_dir/backup.json'))['backup_id'])")"
clickhouse_path="$(python3 -c "import json; print(json.load(open('$backup_dir/backup.json'))['clickhouse_path'])")"
suffix="$(printf '%s' "$backup_id" | tr -d '-' | cut -c1-10)"

for database in business_source platform_control; do
  target="restore_${database}_${suffix}"
  docker compose --env-file .env -f compose.yaml exec -T postgres psql --username postgres --dbname postgres \
    --set=ON_ERROR_STOP=1 --command "DROP DATABASE IF EXISTS ${target} WITH (FORCE)" \
    --command "CREATE DATABASE ${target}"
  docker compose --env-file .env -f compose.yaml exec -T postgres pg_restore \
    --username postgres --dbname "$target" --no-owner --clean --if-exists \
    < "$backup_dir/postgres/$database.dump"
done

source_rows="$(docker compose --env-file .env -f compose.yaml exec -T postgres psql -At --username postgres --dbname business_source --command "SELECT (SELECT count(*) FROM customers)+(SELECT count(*) FROM orders)+(SELECT count(*) FROM order_items)+(SELECT count(*) FROM payments)+(SELECT count(*) FROM refunds)+(SELECT count(*) FROM events)")"
restore_rows="$(docker compose --env-file .env -f compose.yaml exec -T postgres psql -At --username postgres --dbname "restore_business_source_${suffix}" --command "SELECT (SELECT count(*) FROM customers)+(SELECT count(*) FROM orders)+(SELECT count(*) FROM order_items)+(SELECT count(*) FROM payments)+(SELECT count(*) FROM refunds)+(SELECT count(*) FROM events)")"
test "$source_rows" = "$restore_rows" || { echo "PostgreSQL row count mismatch" >&2; exit 1; }

restore_bucket="restore-verify"
docker compose --env-file .env -f compose.yaml run --rm tooling \
  python /opt/kest/ops/recovery.py restore-raw "/opt/kest/.runtime/backups/$(basename "$backup_dir")" "$restore_bucket"

docker compose --env-file .env -f compose.yaml --profile recovery up -d --wait clickhouse-restore
docker compose --env-file .env -f compose.yaml exec -T clickhouse-restore clickhouse-client \
  --user default --password "$CLICKHOUSE_ADMIN_PASSWORD" --multiquery --query \
  "DROP DATABASE IF EXISTS restore_raw; DROP DATABASE IF EXISTS restore_gold; RESTORE DATABASE raw AS restore_raw, DATABASE gold AS restore_gold FROM S3('http://object-store:9000/platform-backups/${clickhouse_path}', '${S3_BACKUP_ACCESS_KEY}', '${S3_BACKUP_SECRET_KEY}')"

for table in mart_daily_revenue mart_conversion_funnel mart_customer_orders; do
  source_sum="$(docker compose --env-file .env -f compose.yaml exec -T clickhouse clickhouse-client --user default --password "$CLICKHOUSE_ADMIN_PASSWORD" --query "SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM gold.${table} FORMAT TSV")"
  restore_sum="$(docker compose --env-file .env -f compose.yaml exec -T clickhouse-restore clickhouse-client --user default --password "$CLICKHOUSE_ADMIN_PASSWORD" --query "SELECT count(), sum(cityHash64(toString(tuple(*)))) FROM restore_gold.${table} FORMAT TSV")"
  test "$source_sum" = "$restore_sum" || { echo "ClickHouse checksum mismatch: $table" >&2; exit 1; }
done

docker compose --env-file .env -f compose.yaml exec -T postgres psql --username postgres --dbname platform_control \
  --set=ON_ERROR_STOP=1 --command "UPDATE backup_manifest SET verified_at=now() WHERE backup_id='${backup_id}'"
echo "Restore verified: backup=$backup_id postgres_rows=$restore_rows bucket=$restore_bucket ClickHouse=dedicated-instance"
