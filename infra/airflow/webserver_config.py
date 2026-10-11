"""FAB authentication settings shared by the Airflow API server and CLI."""

import os

from flask_appbuilder.security.manager import AUTH_DB

AUTH_TYPE = AUTH_DB
AUTH_USER_REGISTRATION = False
RATELIMIT_STORAGE_URI = f"redis://:{os.environ['REDIS_PASSWORD']}@redis:6379/4"
