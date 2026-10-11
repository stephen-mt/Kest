#!/usr/bin/env python3
from __future__ import annotations

import base64
import os

import httpx


def main() -> None:
    email = os.getenv("OPENMETADATA_ADMIN_EMAIL", "admin@open-metadata.org")
    password = os.getenv("OPENMETADATA_ADMIN_PASSWORD", "admin")
    response = httpx.post(
        "http://openmetadata-server:8585/api/v1/users/login",
        json={"email": email, "password": base64.b64encode(password.encode()).decode()},
        timeout=15,
    )
    response.raise_for_status()
    print(response.json()["accessToken"])


if __name__ == "__main__":
    main()
