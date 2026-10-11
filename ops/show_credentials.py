#!/usr/bin/env python3
import argparse
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
values = {}
for line in (ROOT / ".env").read_text().splitlines():
    if line and not line.startswith("#") and "=" in line:
        key, value = line.split("=", 1)
        values[key] = value

parser = argparse.ArgumentParser()
parser.add_argument("--audience", choices=["all", "da"], default="all")
args = parser.parse_args()

if args.audience == "da":
    print("Kest for Data Analysts")
    print("  Superset: http://127.0.0.1:8088")
    print(f"  username: {values['SUPERSET_ADMIN_USER']}")
    print(f"  password: {values['SUPERSET_ADMIN_PASSWORD']}")
    print("  dashboards: Executive Revenue & Orders; Marketing Funnel & Spend")
else:
    print("Airflow:  http://127.0.0.1:8080")
    print(f"  username: {values['AIRFLOW_ADMIN_USER']}")
    print(f"  password: {values['AIRFLOW_ADMIN_PASSWORD']}")
    print("Superset: http://127.0.0.1:8088")
    print(f"  username: {values['SUPERSET_ADMIN_USER']}")
    print(f"  password: {values['SUPERSET_ADMIN_PASSWORD']}")
    print("SILO Console: http://127.0.0.1:9001")
    print(f"  username: {values['S3_ADMIN_ACCESS_KEY']}")
    print(f"  password: {values['S3_ADMIN_SECRET_KEY']}")
    print("OpenMetadata (optional): http://127.0.0.1:8585")
    print("  initial local username: admin@open-metadata.org")
    print("  initial local password: admin (rotate or replace with SSO before production)")
