import os

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

class CeleryConfig:
    broker_url = f"redis://:{os.environ['REDIS_PASSWORD']}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_CELERY_DB}"
    result_backend = f"redis://:{os.environ['REDIS_PASSWORD']}@{REDIS_HOST}:{REDIS_PORT}/{REDIS_RESULTS_DB}"
    worker_prefetch_multiplier = 1
    task_acks_late = True

CELERY_CONFIG = CeleryConfig
