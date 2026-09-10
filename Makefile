.PHONY: start stop restart logs env lint test
.PHONY: platform-up platform-cdc-up platform-down platform-check platform-sql
.PHONY: workload-image workload-setup history history-reset
.PHONY: cdc generate cdc-drain cdc-test
.PHONY: batch batch-check batch-airflow batch-legacy-clean batch-legacy-purge airflow-dag-check
.PHONY: workload-check workload-check-history workload-check-history-deep
.PHONY: workload-check-cdc
.PHONY: delivery-up delivery-image delivery-setup delivery-seed delivery-source-check
.PHONY: delivery-live delivery-batch
.PHONY: delivery-check delivery-cdc-up delivery-airflow-up delivery-airflow
.PHONY: delivery-cdc-down
.PHONY: delivery-streaming-up delivery-streaming-check delivery-streaming-down
.PHONY: delivery-down

WORKLOAD_COMPOSE := docker compose -f docker-compose.yml -f compose.workload.yml
PLATFORM_COMPOSE := docker compose -f docker-compose.yml -f compose.platform.yml --profile platform
DELIVERY_COMPOSE := docker compose -f docker-compose.yml -f compose.platform.yml -f compose.delivery.yml --profile platform --profile delivery --profile delivery-cdc
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
	$(RUFF) format --check workload scenarios tests docker/lakekeeper/bootstrap.py docker/nifi docker/airflow/dags
	$(RUFF) check workload scenarios tests docker/lakekeeper/bootstrap.py docker/nifi docker/airflow/dags

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

delivery-up:
	@test -f .env || (echo 'Create .env from .env.example and replace the placeholders first.'; exit 1)
	$(DELIVERY_COMPOSE) up -d --wait --wait-timeout 300 postgres-delivery nifi trino
	python3 docker/lakekeeper/bootstrap.py --delivery
	$(DELIVERY_COMPOSE) exec -T trino trino --file /etc/trino/delivery-bootstrap.sql
	$(DELIVERY_COMPOSE) exec -T nifi sh -ec 'MINIO_BUCKET="$$DELIVERY_MINIO_BUCKET" LAKEKEEPER_WAREHOUSE="$$DELIVERY_LAKEKEEPER_WAREHOUSE" exec python3 /opt/kest/nifi/bootstrap.py'

delivery-image:
	$(DELIVERY_COMPOSE) build delivery-job

delivery-setup: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli setup

delivery-seed: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli seed

delivery-source-check: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli source-check

delivery-live: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli emit-live

delivery-batch: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli batch

delivery-check: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli validate

delivery-cdc-up: delivery-up
	$(DELIVERY_COMPOSE) up -d --wait --wait-timeout 180 debezium-delivery

delivery-cdc-down: delivery-image
	$(DELIVERY_COMPOSE) stop debezium-delivery
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli cdc-down

delivery-streaming-up: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli streaming-setup

delivery-streaming-check: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli streaming-check

delivery-streaming-down: delivery-image
	$(DELIVERY_COMPOSE) run --rm delivery-job python -m scenarios.delivery_ops.cli streaming-down

delivery-airflow-up:
	$(DELIVERY_COMPOSE) up -d --wait --wait-timeout 300 airflow

delivery-airflow:
	$(DELIVERY_COMPOSE) exec -T airflow airflow dags trigger delivery_ops_batch

delivery-down: delivery-cdc-down delivery-streaming-down
	$(DELIVERY_COMPOSE) stop postgres-delivery

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

batch-legacy-clean: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.lakehouse.maintenance

batch-legacy-purge: workload-image
	$(WORKLOAD_COMPOSE) run --rm workload python -m workload.lakehouse.maintenance --purge

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
