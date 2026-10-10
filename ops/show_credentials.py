#!/usr/bin/env python3
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
values = {}
for line in (ROOT / ".env").read_text().splitlines():
    if line and not line.startswith("#") and "=" in line:
        key, value = line.split("=", 1)
        values[key] = value

print("Airflow:  http://127.0.0.1:8080 (simple-auth users are shown in Airflow init logs)")
print("Superset: http://127.0.0.1:8088")
print(f"  username: {values['SUPERSET_ADMIN_USER']}")
print(f"  password: {values['SUPERSET_ADMIN_PASSWORD']}")
print("OpenMetadata (optional): http://127.0.0.1:8585")
