.PHONY: start stop restart logs env lint test platform-up platform-cdc-up platform-down platform-check platform-sql workload-image workload-setup history history-reset cdc generate cdc-drain cdc-test batch batch-check batch-airflow airflow-dag-check workload-check workload-check-history workload-check-history-deep workload-check-cdc

WORKLOAD_COMPOSE := docker compose -f docker-compose.yml -f compose.workload.yml
PLATFORM_COMPOSE := docker compose -f docker-compose.yml -f compose.platform.yml --profile platform
RUFF := UV_CACHE_DIR=.uv-cache UV_TOOL_DIR=.uv-cache/tools uvx --from ruff==0.16.6 ruff

# Required entry point on first start: Lakekeeper's image has no shell and
# requires a one-shot metadata migration before its server can become healthy.
start:
	@test -f .env || (echo 'Create .env from .env.example and replace the placeholders first.'; exit 1)
	docker compose up -d --wait postgres-source postgres-airflow postgres-catalog minio
	docker compose run --rm --no-deps lakekeeper migrate
	docker compose up -d --wait --wait-timeout 300
	python3 docker/lakekeeper/bootstrap.py

stop:
	docker compose stop

restart:
	docker compose stop
	$(MAKE) start

logs:
	docker compose logs -f --tail=100

env:
	uv venv .venv
	uv pip sync requirements.txt --python .venv/bin/python

lint:
	$(RUFF) format --check workload tests docker/lakekeeper/bootstrap.py docker/nifi docker/airflow/dags
	$(RUFF) check workload tests docker/lakekeeper/bootstrap.py docker/nifi docker/airflow/dags

test: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m unittest discover -s tests

# NiFi and Trino are optional. Debezium starts separately so a connector is not
# allowed to retain PostgreSQL WAL before its source configuration is reviewed.
platform-up:
	@test -f .env || (echo 'Create .env from .env.example and replace the placeholders first.'; exit 1)
	$(PLATFORM_COMPOSE) up -d --wait --wait-timeout 300 nifi trino
	$(PLATFORM_COMPOSE) exec -T trino trino --file /etc/trino/bootstrap.sql
	$(PLATFORM_COMPOSE) exec -T nifi python3 /opt/kest/nifi/bootstrap.py

platform-cdc-up: platform-up
	$(PLATFORM_COMPOSE) up -d --wait --wait-timeout 180 debezium-postgres

platform-down:
	$(PLATFORM_COMPOSE) stop debezium-postgres nifi trino

platform-check:
	$(PLATFORM_COMPOSE) exec -T nifi python3 /opt/kest/nifi/check.py
	$(PLATFORM_COMPOSE) exec -T trino trino --execute 'SHOW CATALOGS; SELECT 1 AS ready'

platform-sql:
	$(PLATFORM_COMPOSE) exec trino trino --catalog lakehouse

workload-image:
	$(WORKLOAD_COMPOSE) --profile workload build workload

workload-setup: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.cybermarket.bootstrap

history: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.history

history-reset: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.history --reset

cdc: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.cdc

generate: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.generator

cdc-drain: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.cdc --drain

cdc-test: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.smoke

batch: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.pipelines.batch

batch-check: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.state --phase batch

batch-airflow:
	docker compose exec -T airflow airflow dags trigger cybermarket_batch

airflow-dag-check:
	docker compose exec -T airflow airflow dags list
	docker compose exec -T airflow airflow dags list-import-errors

workload-check: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.state --phase setup

workload-check-history: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.state --phase history

workload-check-history-deep: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.deep_history

workload-check-cdc: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.validation.state --phase cdc
