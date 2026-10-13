import os
from typing import Any

from flask_appbuilder.security.manager import AUTH_DB, AUTH_OAUTH
from superset.security import SupersetSecurityManager

SECRET_KEY = os.environ["SUPERSET_SECRET_KEY"]
SQLALCHEMY_DATABASE_URI = os.environ["SUPERSET_DATABASE_URI"]
REDIS_HOST = "redis"
REDIS_PORT = 6379
REDIS_RESULTS_DB = 2
REDIS_CELERY_DB = 1
WTF_CSRF_ENABLED = True
RATELIMIT_STORAGE_URI = f"redis://:{os.environ['REDIS_PASSWORD']}@{REDIS_HOST}:{REDIS_PORT}/3"
TALISMAN_ENABLED = True
TALISMAN_CONFIG = {
    # Compose binds to loopback over HTTP. A production ingress terminates TLS.
    "force_https": False,
    "content_security_policy": {
        "default-src": ["'self'"],
        "img-src": ["'self'", "blob:", "data:"],
        "worker-src": ["'self'", "blob:"],
        "connect-src": ["'self'"],
        "object-src": ["'none'"],
        "style-src": ["'self'", "'unsafe-inline'"],
        "script-src": ["'self'", "'strict-dynamic'"],
    },
    "content_security_policy_nonce_in": ["script-src"],
}
FEATURE_FLAGS = {"ENABLE_TEMPLATE_PROCESSING": False}
ROW_LIMIT = 100_000
SQL_MAX_ROW = 100_000
SUPERSET_WEBSERVER_TIMEOUT = 60

if os.getenv("KEST_AUTH_MODE", "oidc") == "oidc":
    AUTH_TYPE = AUTH_OAUTH
    AUTH_USER_REGISTRATION = True
    AUTH_USER_REGISTRATION_ROLE = "Gamma"
    AUTH_ROLES_SYNC_AT_LOGIN = True
    AUTH_ROLES_MAPPING = {
        "platform-admin": ["Admin"],
        "data-engineer": ["Alpha"],
        "data-analyst": ["Gamma", "sql_lab"],
    }
    OAUTH_PROVIDERS = [
        {
            "name": "keycloak",
            "icon": "fa-key",
            "token_key": "access_token",
            "remote_app": {
                "client_id": "superset",
                "client_secret": os.environ["SUPERSET_OIDC_CLIENT_SECRET"],
                "api_base_url": "http://keycloak:8080/realms/kest/",
                "access_token_url": "http://keycloak:8080/realms/kest/protocol/openid-connect/token",
                "authorize_url": "http://127.0.0.1:8180/realms/kest/protocol/openid-connect/auth",
                "request_token_url": None,
                "client_kwargs": {"scope": "openid profile email roles"},
            },
        }
    ]

    class KestSupersetSecurityManager(SupersetSecurityManager):
        def oauth_user_info(self, provider: str, response: Any = None) -> dict[str, Any]:
            remote = self.appbuilder.sm.oauth_remotes[provider]
            user = remote.get("protocol/openid-connect/userinfo").json()
            return {
                "username": user["preferred_username"],
                "email": user.get("email", ""),
                "first_name": user.get("given_name", ""),
                "last_name": user.get("family_name", ""),
                "role_keys": user.get("realm_access", {}).get("roles", user.get("roles", [])),
            }

    CUSTOM_SECURITY_MANAGER = KestSupersetSecurityManager
else:
    AUTH_TYPE = AUTH_DB

class CeleryConfig:
    broker_url = f"redis://:{os.environ['REDIS_PASSWORD']}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    result_backend = f"redis://:{os.environ['REDIS_PASSWORD']}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    worker_prefetch_multiplier = 1
    task_acks_late = True

CELERY_CONFIG = CeleryConfig
