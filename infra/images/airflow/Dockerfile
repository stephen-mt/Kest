FROM apache/airflow:3.3.2-python3.12@sha256:9df9c8be4096b9cc626bd7cb1f2b8c712eef66c59c615f8c7e6200871ca10bd1

ARG AIRFLOW_VERSION=3.3.2
ARG PYTHON_VERSION=3.12

USER root
RUN apt-get update \
    && apt-get install -y --no-install-recommends build-essential curl libpq-dev \
    && rm -rf /var/lib/apt/lists/* \
    && mkdir -p /opt/venvs \
    && chown airflow:root /opt/venvs

USER airflow
RUN python -m pip install --no-cache-dir \
      "astronomer-cosmos==1.15.1" \
      "apache-airflow-providers-celery==3.24.0" \
      "apache-airflow-providers-postgres==7.0.2" \
      "apache-airflow-providers-http==6.1.0" \
      "apache-airflow-providers-standard==1.19.0" \
      --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-${AIRFLOW_VERSION}/constraints-${PYTHON_VERSION}.txt"

RUN python -m venv /opt/venvs/ingestion \
    && /opt/venvs/ingestion/bin/pip install --no-cache-dir "pip==26.2.1" \
    && /opt/venvs/ingestion/bin/pip install --no-cache-dir \
      "boto3==1.40.40" \
      "clickhouse-connect==1.10.0" \
      "dlt[filesystem]==1.31.0" \
      "httpx==0.28.1" \
      "psycopg[binary]==3.2.10" \
      "pyarrow==21.0.0" \
      "pydantic==2.11.9" \
      "PyYAML==6.0.2" \
      "tenacity==9.1.2"

RUN python -m venv /opt/venvs/dbt \
    && /opt/venvs/dbt/bin/pip install --no-cache-dir "pip==26.2.1" \
    && /opt/venvs/dbt/bin/pip install --no-cache-dir \
      "dbt-core==1.12.5" \
      "dbt-clickhouse==1.10.3"

RUN python -m venv /opt/venvs/metadata \
    && /opt/venvs/metadata/bin/pip install --no-cache-dir "pip==26.2.1" \
    && /opt/venvs/metadata/bin/pip install --no-cache-dir \
      "openmetadata-ingestion[clickhouse,dbt,airflow,postgres,superset]==2.0.5.0" \
      "psycopg2-binary==2.9.11"

ENV PYTHONPATH=/opt/kest:/opt/kest/ingestion:/opt/kest/orchestration
WORKDIR /opt/kest
