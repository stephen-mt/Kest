"""Small authenticated client for the local NiFi REST API."""

from __future__ import annotations

import json
import os
import ssl
import time
import urllib.error
import urllib.parse
import urllib.request
from typing import Any

JsonObject = dict[str, Any]


class NifiClient:
    def __init__(
        self,
        base_url: str = "https://127.0.0.1:8443/nifi-api",
        readiness_attempts: int = 60,
    ) -> None:
        self.base_url = base_url
        self.tls = ssl._create_unverified_context()
        self.readiness_attempts = readiness_attempts
        self.token = self._wait_for_login()

    def _wait_for_login(self) -> str:
        last_error = None
        for _ in range(self.readiness_attempts):
            try:
                return self._login()
            except urllib.error.URLError as error:
                last_error = error
                time.sleep(2)
        timeout = self.readiness_attempts * 2
        raise RuntimeError(
            f"NiFi API did not become ready within {timeout} seconds"
        ) from last_error

    def _login(self) -> str:
        body = urllib.parse.urlencode(
            {
                "username": os.environ["SINGLE_USER_CREDENTIALS_USERNAME"],
                "password": os.environ["SINGLE_USER_CREDENTIALS_PASSWORD"],
            }
        ).encode()
        request = urllib.request.Request(
            self.base_url + "/access/token",
            data=body,
            headers={"Content-Type": "application/x-www-form-urlencoded"},
            method="POST",
        )
        with urllib.request.urlopen(request, context=self.tls, timeout=10) as response:
            return response.read().decode()

    def request(
        self, method: str, path: str, payload: JsonObject | None = None
    ) -> JsonObject | None:
        data = None if payload is None else json.dumps(payload).encode()
        request = urllib.request.Request(
            self.base_url + path,
            data=data,
            headers={
                "Authorization": f"Bearer {self.token}",
                "Content-Type": "application/json",
            },
            method=method,
        )
        try:
            with urllib.request.urlopen(
                request, context=self.tls, timeout=30
            ) as response:
                if response.status == 204:
                    return None
                return json.load(response)
        except urllib.error.HTTPError as error:
            detail = error.read().decode()
            raise RuntimeError(
                f"NiFi {method} {path}: HTTP {error.code}: {detail}"
            ) from error

    def get(self, path: str) -> JsonObject:
        response = self.request("GET", path)
        if response is None:
            raise RuntimeError(f"NiFi GET {path} returned an empty response")
        return response

    def post(self, path: str, payload: JsonObject) -> JsonObject:
        response = self.request("POST", path, payload)
        if response is None:
            raise RuntimeError(f"NiFi POST {path} returned an empty response")
        return response

    def put(self, path: str, payload: JsonObject) -> JsonObject:
        response = self.request("PUT", path, payload)
        if response is None:
            raise RuntimeError(f"NiFi PUT {path} returned an empty response")
        return response

    def delete(self, path: str) -> None:
        self.request("DELETE", path)
