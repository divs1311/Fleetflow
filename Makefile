# PorterFlow — common commands. `make help` lists them.
SHELL := /bin/bash
COMPOSE := docker compose
DAG := porterflow_daily

.PHONY: help env up down logs status ps dashboard reset backfill rerun full-refresh test test-unit test-integration lint shell dbt-docs

help:  ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## ' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "  \033[36m%-16s\033[0m %s\n", $$1, $$2}'

env:  ## create .env (with your UID on Linux)
	@test -f .env || (cp .env.example .env && \
	  if [ "$$(uname)" = "Linux" ]; then sed -i "s/^AIRFLOW_UID=.*/AIRFLOW_UID=$$(id -u)/" .env; fi && echo "created .env")

up: env  ## build and start everything; the DAG starts backfilling automatically
	$(COMPOSE) up -d --build
	@echo ""
	@echo "Airflow     http://localhost:8080  (airflow / airflow)"
	@echo "Dashboard   http://localhost:8501"
	@echo "Redpanda    http://localhost:8085"
	@echo "MinIO       http://localhost:9001  (porterflow / porterflow-secret)"
	@echo "Master API  http://localhost:8000/docs"

down:  ## stop containers (keeps data)
	$(COMPOSE) down

logs:  ## follow scheduler logs
	$(COMPOSE) logs -f airflow-scheduler

ps:  ## container status
	$(COMPOSE) ps

status:  ## DAG run states
	$(COMPOSE) exec airflow-scheduler airflow dags list-runs -d $(DAG)

backfill:  ## re-process a date range: make backfill START=2026-09-03 END=2026-09-05
	$(COMPOSE) exec airflow-scheduler airflow dags backfill $(DAG) -s $(START) -e $(END) --reset-dagruns -y

rerun:  ## clear and re-run one day: make rerun DAY=2026-09-05
	$(COMPOSE) exec airflow-scheduler airflow tasks clear $(DAG) -s $(DAY) -e $(DAY) -y

full-refresh:  ## rebuild every incremental dbt model from the raw lake for DAY
	$(COMPOSE) exec airflow-scheduler airflow dags trigger $(DAG) -e $(DAY)T12:00:00+00:00 -c '{"full_refresh": true}'

shell:  ## shell inside the Airflow image (dbt, spark-submit, duckdb available)
	$(COMPOSE) exec airflow-scheduler bash

reset:  ## DANGER: stop everything and delete all data, topics and metadata
	$(COMPOSE) down -v
	rm -rf data/lake data/warehouse data/serving data/state data/quality

test: test-unit  ## run unit tests (no Docker needed)

test-unit:
	PYTHONDONTWRITEBYTECODE=1 python -m pytest tests/unit -q

test-integration:  ## end-to-end test: simulator -> lake -> Spark -> dbt -> serving (needs dbt + Java)
	PYTHONDONTWRITEBYTECODE=1 python -m pytest tests/integration -q

dbt-docs:  ## generate dbt docs (lineage graph) into data/state/dbt/target
	$(COMPOSE) exec airflow-scheduler bash -c '$$DBT_EXECUTABLE docs generate --project-dir $$DBT_PROJECT_DIR'
