#!/usr/bin/env bash
set -euo pipefail

repo_dir="$(cd "$(dirname "${BASH_SOURCE[0]}")/.." && pwd)"
cd "$repo_dir"
set -a
source .env
set +a

backup_id="$(python3 -c 'import uuid; print(uuid.uuid4())')"
created_at="$(date -u +%Y%m%dT%H%M%SZ)"
backup_dir="$repo_dir/.runtime/backups/${created_at}-${backup_id}"
mkdir -p "$backup_dir/postgres"
chmod 700 "$repo_dir/.runtime/backups" "$backup_dir"

for database in business_source platform_control airflow superset openmetadata; do
  docker compose --env-file .env -f compose.yaml exec -T postgres \
    pg_dump --username postgres --format custom "$database" > "$backup_dir/postgres/$database.dump"
done

docker compose --env-file .env -f compose.yaml run --rm tooling \
  python /opt/kest/ops/recovery.py backup-raw "/opt/kest/.runtime/backups/${created_at}-${backup_id}"

clickhouse_path="${backup_id}/clickhouse"
docker compose --env-file .env -f compose.yaml exec -T clickhouse clickhouse-client \
  --user default --password "$CLICKHOUSE_ADMIN_PASSWORD" --query \
  "BACKUP DATABASE raw, DATABASE gold TO S3('http://object-store:9000/platform-backups/${clickhouse_path}', '${S3_BACKUP_ACCESS_KEY}', '${S3_BACKUP_SECRET_KEY}')"

docker compose --env-file .env -f compose.yaml run --rm tooling \
  python /opt/kest/ops/recovery.py write-checksums "/opt/kest/.runtime/backups/${created_at}-${backup_id}"
artifact_checksum="$(sha256sum "$backup_dir/SHA256SUMS" | cut -d' ' -f1)"
python3 - "$backup_dir/backup.json" "$backup_id" "$created_at" "$clickhouse_path" "$artifact_checksum" <<'PY'
import json
import sys
from pathlib import Path

Path(sys.argv[1]).write_text(json.dumps({
    "backup_id": sys.argv[2],
    "created_at": sys.argv[3],
    "clickhouse_path": sys.argv[4],
    "artifact_checksum": sys.argv[5],
}, indent=2) + "\n")
PY
printf '%s\n' "$backup_dir" > "$repo_dir/.runtime/backups/LATEST"

docker compose --env-file .env -f compose.yaml exec -T postgres psql \
  --username postgres --dbname platform_control --set=ON_ERROR_STOP=1 \
  --command "INSERT INTO backup_manifest(backup_id,path,checksum) VALUES ('$backup_id','$backup_dir','$artifact_checksum')"

echo "Backup complete: $backup_dir"
echo "Copy this directory off-host for real host-loss recovery."
