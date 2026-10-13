FROM python:3.12.12-slim-bookworm@sha256:593bd06efe90efa80dc4eee3948be7c0fde4134606dd40d8dd8dbcade98e669c
ARG KEST_GIT_SHA=unknown
LABEL org.opencontainers.image.revision=$KEST_GIT_SHA

RUN apt-get update \
    && apt-get install -y --no-install-recommends curl postgresql-client \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /opt/kest
COPY pyproject.toml /opt/kest/pyproject.toml
COPY ingestion /opt/kest/ingestion
COPY transformations /opt/kest/transformations
COPY governance /opt/kest/governance
COPY ops /opt/kest/ops
COPY serving /opt/kest/serving
COPY infra/migrations /opt/kest/infra/migrations
RUN python -m pip install --no-cache-dir "pip==26.2.1" \
    && python -m pip install --no-cache-dir -e '.[test]' \
    && python -m venv /opt/venvs/dbt \
    && /opt/venvs/dbt/bin/pip install --no-cache-dir \
      "dbt-core==1.12.5" "dbt-clickhouse==1.10.3"
ENV PYTHONPATH=/opt/kest/ingestion
