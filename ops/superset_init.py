#!/usr/bin/env python3
"""Initialize Superset and idempotently provision Kest BI assets."""

from __future__ import annotations

import os
import runpy
import subprocess


def main() -> None:
    subprocess.run(["superset", "db", "upgrade"], check=True)
    if os.getenv("KEST_AUTH_MODE", "oidc") != "oidc":
        subprocess.run(
            [
                "superset", "fab", "create-admin",
                "--username", os.environ["SUPERSET_ADMIN_USER"],
                "--firstname", "Kest", "--lastname", "Admin",
                "--email", "admin@kest.local",
                "--password", os.environ["SUPERSET_ADMIN_PASSWORD"],
            ],
            check=False,
        )
    subprocess.run(["superset", "init"], check=True)
    runpy.run_path("/opt/kest/serving/bootstrap_superset.py", run_name="__main__")


if __name__ == "__main__":
    main()
