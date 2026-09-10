ARG AIRFLOW_IMAGE=apache/airflow:3.3.0-python3.12
FROM ${AIRFLOW_IMAGE}

USER root
COPY requirements.txt /tmp/kest-requirements.txt
RUN python -m venv /opt/kest/venv \
    && /opt/kest/venv/bin/pip install --no-cache-dir --requirement /tmp/kest-requirements.txt \
    && rm /tmp/kest-requirements.txt

USER airflow
