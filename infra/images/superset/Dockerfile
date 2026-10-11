FROM apache/superset:6.1.0@sha256:16b50bbef6648912a79e3293d418fefa743ce555d36c5af6b85d859119ed7f88

USER root
RUN . /app/.venv/bin/activate \
    && uv pip install "clickhouse-connect==1.10.0" "psycopg2-binary==2.9.11"
COPY infra/superset/superset_config.py /app/pythonpath/superset_config.py
USER superset
