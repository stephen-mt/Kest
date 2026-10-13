#!/usr/bin/env python3
"""Reconcile versioned Keycloak realm roles and clients without replacing users."""

from __future__ import annotations

import json
import os
import urllib.error
import urllib.parse
import urllib.request
from pathlib import Path
from typing import Any

ROOT = Path(__file__).resolve().parents[1]
BASE = "http://keycloak:8080"


def request(path: str, *, token: str | None = None, method: str = "GET", payload: Any = None) -> Any:
    headers = {"Content-Type": "application/json"}
    if token:
        headers["Authorization"] = f"Bearer {token}"
    body = json.dumps(payload).encode() if payload is not None else None
    call = urllib.request.Request(f"{BASE}{path}", headers=headers, data=body, method=method)
    with urllib.request.urlopen(call, timeout=20) as response:
        data = response.read()
        return json.loads(data) if data else None


def admin_token() -> str:
    body = urllib.parse.urlencode(
        {
            "grant_type": "password",
            "client_id": "admin-cli",
            "username": os.environ["KEYCLOAK_ADMIN_USER"],
            "password": os.environ["KEYCLOAK_ADMIN_PASSWORD"],
        }
    ).encode()
    call = urllib.request.Request(
        f"{BASE}/realms/master/protocol/openid-connect/token",
        headers={"Content-Type": "application/x-www-form-urlencoded"},
        data=body,
    )
    with urllib.request.urlopen(call, timeout=20) as response:
        return json.loads(response.read())["access_token"]


def expand(value: Any) -> Any:
    if isinstance(value, str):
        return os.path.expandvars(value)
    if isinstance(value, list):
        return [expand(item) for item in value]
    if isinstance(value, dict):
        return {key: expand(item) for key, item in value.items()}
    return value


def main() -> None:
    desired = expand(json.loads((ROOT / "infra/identity/kest-realm.json").read_text()))
    token = admin_token()
    realm = desired["realm"]
    for role in desired["roles"]["realm"]:
        try:
            request(f"/admin/realms/{realm}/roles/{role['name']}", token=token)
        except urllib.error.HTTPError as error:
            if error.code != 404:
                raise
            request(f"/admin/realms/{realm}/roles", token=token, method="POST", payload=role)
    for client in desired["clients"]:
        matches = request(
            f"/admin/realms/{realm}/clients?clientId={urllib.parse.quote(client['clientId'])}",
            token=token,
        )
        if matches:
            request(
                f"/admin/realms/{realm}/clients/{matches[0]['id']}",
                token=token,
                method="PUT",
                payload={**matches[0], **client},
            )
        else:
            request(f"/admin/realms/{realm}/clients", token=token, method="POST", payload=client)
        print(f"keycloak client reconciled: {client['clientId']}")


if __name__ == "__main__":
    main()
