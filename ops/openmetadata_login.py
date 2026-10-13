#!/usr/bin/env python3
from __future__ import annotations

import os

import httpx


def main() -> None:
    if os.getenv("KEST_AUTH_MODE", "oidc") == "oidc":
        response = httpx.post(
            "http://keycloak:8080/realms/kest/protocol/openid-connect/token",
            data={
                "grant_type": "client_credentials",
                "client_id": "openmetadata-ingestion",
                "client_secret": os.environ["OPENMETADATA_INGESTION_CLIENT_SECRET"],
            },
            timeout=15,
        )
        response.raise_for_status()
        print(response.json()["access_token"])
        return
    import base64

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
