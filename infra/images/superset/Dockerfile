FROM apache/superset:6.1.0

USER root
RUN . /app/.venv/bin/activate \
    && uv pip install "clickhouse-connect==0.9.2" "psycopg2-binary==2.9.10"
COPY infra/superset/superset_config.py /app/pythonpath/superset_config.py
USER superset
