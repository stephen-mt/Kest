#!/usr/bin/env bash
set -euo pipefail

if ! curl -fsS http://openmetadata-server:8585/healthcheck >/dev/null; then
  echo "OpenMetadata profile is not running; start it with make catalog-up" >&2
  exit 2
fi

/opt/venvs/metadata/bin/metadata ingest -c /opt/kest/governance/openmetadata/clickhouse.yml
/opt/venvs/metadata/bin/metadata ingest -c /opt/kest/governance/openmetadata/dbt.yml
/opt/venvs/ingestion/bin/python /opt/kest/ops/verify_metadata.py
