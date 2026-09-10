#!/usr/bin/env bash
set -euo pipefail

# Regenerate local UI credentials from .env even after container recreation.
python - <<'PY'
import json
import os
from pathlib import Path

path = Path(os.environ['AIRFLOW__CORE__SIMPLE_AUTH_MANAGER_PASSWORDS_FILE'])
path.write_text(json.dumps({os.environ['AIRFLOW_ADMIN_USER']: os.environ['AIRFLOW_ADMIN_PASSWORD']}))
path.chmod(0o600)
PY

exec airflow standalone
