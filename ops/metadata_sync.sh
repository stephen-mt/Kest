#!/usr/bin/env bash
set -euo pipefail

if ! curl -fsS http://openmetadata-server:8585/healthcheck >/dev/null; then
  echo "OpenMetadata profile is not running; start it with make catalog-up" >&2
  exit 2
fi

export OPENMETADATA_INGESTION_TOKEN="$(/opt/venvs/ingestion/bin/python /opt/kest/ops/openmetadata_login.py)"
export PYTHON_DOTENV_DISABLED=1
export DBT_TARGET_PATH=/tmp/kest-dbt-target
export DBT_LOG_PATH=/tmp/kest-dbt-logs
export OPENMETADATA_DBT_ARTIFACT_DIR=/tmp/kest-dbt-metadata
/opt/venvs/dbt/bin/dbt docs generate --project-dir /opt/kest/transformations --profiles-dir /opt/kest/transformations --target production
/opt/venvs/ingestion/bin/python /opt/kest/ops/prepare_metadata_artifacts.py
/opt/venvs/metadata/bin/metadata ingest -c /opt/kest/governance/openmetadata/clickhouse.yml
/opt/venvs/ingestion/bin/python /opt/kest/ops/bootstrap_governance.py
/opt/venvs/metadata/bin/metadata ingest -c /opt/kest/governance/openmetadata/dbt.yml
AIRFLOW__DATABASE__SQL_ALCHEMY_CONN=sqlite:////tmp/openmetadata-airflow.db \
  /opt/venvs/metadata/bin/metadata ingest -c /opt/kest/governance/openmetadata/airflow.yml
/opt/venvs/metadata/bin/metadata ingest -c /opt/kest/governance/openmetadata/superset.yml
/opt/venvs/ingestion/bin/python /opt/kest/ops/verify_metadata.py
