ARG POSTGRES_SOURCE_IMAGE=postgres:16
FROM ${POSTGRES_SOURCE_IMAGE}

RUN apt-get update \
    && apt-get install --yes --no-install-recommends postgresql-16-wal2json \
    && rm -rf /var/lib/apt/lists/*
