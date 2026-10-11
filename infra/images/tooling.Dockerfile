FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl postgresql-client \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/kest
COPY pyproject.toml /opt/kest/pyproject.toml
COPY ingestion /opt/kest/ingestion
RUN python -m pip install --no-cache-dir "pip==26.2.1" \
    && python -m pip install --no-cache-dir -e '.[test]' \
    && python -m venv /opt/venvs/dbt \
    && /opt/venvs/dbt/bin/pip install --no-cache-dir \
      "dbt-core==1.12.5" "dbt-clickhouse==1.10.3"
ENV PYTHONPATH=/opt/kest/ingestion
