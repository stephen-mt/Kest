FROM python:3.12.7-slim-bookworm

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl postgresql-client \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/kest
COPY pyproject.toml /opt/kest/pyproject.toml
COPY ingestion /opt/kest/ingestion
RUN python -m pip install --no-cache-dir --upgrade pip \
    && python -m pip install --no-cache-dir -e '.[test]' \
    && python -m venv /opt/venvs/dbt \
    && /opt/venvs/dbt/bin/pip install --no-cache-dir \
      "dbt-core==1.12.5" "dbt-clickhouse==1.10.3"
ENV PYTHONPATH=/opt/kest/ingestion
