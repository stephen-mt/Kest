"""FAB authentication settings shared by the Airflow API server and CLI."""

import os
from typing import Any

from airflow.providers.fab.auth_manager.security_manager.override import (
    FabAirflowSecurityManagerOverride,
)
from flask_appbuilder.security.manager import AUTH_DB, AUTH_OAUTH

RATELIMIT_STORAGE_URI = f"redis://:{os.environ['REDIS_PASSWORD']}@redis:6379/4"

if os.getenv("KEST_AUTH_MODE", "oidc") == "oidc":
    AUTH_TYPE = AUTH_OAUTH
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Public"
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = {
        "platform-admin": ["Admin"],
        "data-engineer": ["Op"],
        "data-analyst": ["Viewer"],
    }
    OAUTH_PROVIDERS = [
        {
            "name": "keycloak",
            "icon": "fa-key",
            "token_key": "access_token",
            "remote_app": {
                "client_id": "airflow",
                "client_secret": os.environ["AIRFLOW_OIDC_CLIENT_SECRET"],
                "api_base_url": "http://keycloak:8080/realms/kest/",
                "access_token_url": "http://keycloak:8080/realms/kest/protocol/openid-connect/token",
                "authorize_url": "http://127.0.0.1:8180/realms/kest/protocol/openid-connect/auth",
                "request_token_url": None,
                "client_kwargs": {"scope": "openid profile email roles"},
            },
        }
    ]

    class KestAirflowSecurityManager(FabAirflowSecurityManagerOverride):
        def get_oauth_user_info(self, provider: str, resp: Any) -> dict[str, Any]:
            remote = self.appbuilder.sm.oauth_remotes[provider]
            user = remote.get("protocol/openid-connect/userinfo").json()
            return {
                "username": user["preferred_username"],
                "email": user.get("email", ""),
                "first_name": user.get("given_name", ""),
                "last_name": user.get("family_name", ""),
                "role_keys": user.get("realm_access", {}).get("roles", user.get("roles", [])),
            }

    SECURITY_MANAGER_CLASS = KestAirflowSecurityManager
else:
    AUTH_TYPE = AUTH_DB
    AUTH_USER_REGISTRATION = False
