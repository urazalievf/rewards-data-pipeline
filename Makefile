# Card rewards medallion pipeline
# `make help` lists everything.

SHELL := /bin/bash
PY ?= python3
VENV := .venv
BIN := $(VENV)/bin
DATE ?= $(shell date +%F)
DAYS ?= 14
COMPOSE := docker compose

# Spark 3.5 runs on JDK 17 or 21 only, and many machines default to a newer
# JDK. scripts/find-jdk.sh locates a usable one and JAVA_HOME is pointed at it
# for the duration of the make run. Nothing is installed; your shell is
# untouched. Set JAVA_HOME yourself to override.
SUPPORTED_JAVA := $(shell ./scripts/find-jdk.sh 2>/dev/null)
ifneq ($(strip $(SUPPORTED_JAVA)),)
JAVA_HOME := $(SUPPORTED_JAVA)
export JAVA_HOME
endif

.DEFAULT_GOAL := help

## --- setup ---------------------------------------------------------------

help: ## show this help
	@grep -E '^[a-zA-Z_-]+:.*?## .*$$' $(MAKEFILE_LIST) \
		| awk 'BEGIN {FS = ":.*?## "}; {printf "\033[36m%-18s\033[0m %s\n", $$1, $$2}'

venv: ## create the virtualenv and install dev dependencies
	$(PY) -m venv $(VENV)
	$(BIN)/pip install --upgrade pip
	$(BIN)/pip install -r requirements-dev.txt
	$(BIN)/pip install -e .

doctor: ## check that the local toolchain can run Spark
	@echo "python    : $$($(PY) -V 2>&1)"
	@echo "JAVA_HOME : $${JAVA_HOME:-(unset)}"
	@echo "java      : $$($${JAVA_HOME:+$$JAVA_HOME/bin/}java -version 2>&1 | head -1)"
	@JV=$$($${JAVA_HOME:+$$JAVA_HOME/bin/}java -version 2>&1 | head -1 | sed -E 's/.*"([0-9]+).*/\1/'); \
	if [ "$$JV" != "17" ] && [ "$$JV" != "21" ]; then \
		echo ""; \
		echo "  !! Spark 3.5 supports Java 17 or 21; found $$JV."; \
		echo "  !! Either install a supported JDK and export JAVA_HOME,"; \
		echo "  !! or use the Docker path: make docker-run"; \
	else \
		echo "java version OK for Spark 3.5"; \
	fi

## --- pipeline ------------------------------------------------------------

seed: ## regenerate the synthetic landing zone
	$(BIN)/rewards seed

bronze: ## run the bronze stage for DATE (default: today)
	$(BIN)/rewards bronze --date $(DATE)

silver: ## run the silver stage for DATE
	$(BIN)/rewards silver --date $(DATE)

gold: ## run the gold stage for DATE
	$(BIN)/rewards gold --date $(DATE)

run: ## seed + bronze + silver + gold, end to end
	$(BIN)/rewards run-all --seed --date $(DATE)

backfill: ## seed + ingest DAYS of history (default 14) + rebuild silver and gold
	$(BIN)/rewards backfill --days $(DAYS) --seed

preview: ## print the gold marts
	$(BIN)/rewards preview --limit 15

quality: ## show the latest data-quality report
	@find data/warehouse/_quality -name '*.json' -print -exec \
		$(PY) -c 'import json,sys; d=json.load(open(sys.argv[1])); \
		print("  passed" if d["passed"] else "  FAILED"); \
		[print("   ",r["check"],r["column"] or "",r["detail"]) for r in d["results"]]' {} \; \
		2>/dev/null || echo "no quality reports yet - run the pipeline first"

## --- quality gates -------------------------------------------------------

test: ## run the full test suite
	$(BIN)/pytest

test-fast: ## run only the tests that do not need a JVM
	$(BIN)/pytest -m "not slow"

lint: ## ruff + mypy
	$(BIN)/ruff check src tests dags
	$(BIN)/mypy src

format: ## apply ruff formatting and import ordering
	$(BIN)/ruff format src tests dags
	$(BIN)/ruff check --fix src tests dags

## --- docker --------------------------------------------------------------

docker-build: ## build the pipeline image
	$(COMPOSE) build pipeline

docker-run: ## run the whole pipeline inside the container
	$(COMPOSE) run --rm pipeline run-all --seed --date $(DATE)

docker-backfill: ## backfill DAYS of history inside the container
	$(COMPOSE) run --rm pipeline backfill --days $(DAYS) --seed

docker-preview: ## print the gold marts from inside the container
	$(COMPOSE) run --rm pipeline preview --limit 15

docker-test: ## run the test suite inside the container
	$(COMPOSE) run --rm --entrypoint pytest pipeline

docker-shell: ## open a shell in the pipeline container
	$(COMPOSE) run --rm --entrypoint bash pipeline

cluster-up: ## start a standalone Spark master + worker
	$(COMPOSE) --profile cluster up -d

airflow-up: ## start Airflow (the DAG is created paused)
	$(COMPOSE) --profile airflow up -d
	@echo "Airflow UI: http://localhost:8080  (admin / admin)"
	@echo "The rewards_medallion DAG is paused - unpause it to schedule runs."

down: ## stop every container and remove volumes
	$(COMPOSE) --profile cluster --profile airflow down -v

## --- housekeeping --------------------------------------------------------

clean: ## remove generated data and caches
	rm -rf data/landing/* data/warehouse/* spark-warehouse metastore_db derby.log
	find . -name __pycache__ -type d -prune -exec rm -rf {} +
	rm -rf .pytest_cache .ruff_cache .mypy_cache
	@touch data/landing/.gitkeep data/warehouse/.gitkeep

.PHONY: help venv doctor seed bronze silver gold run backfill preview quality test test-fast \
	lint format docker-build docker-run docker-backfill docker-preview docker-test docker-shell \
	cluster-up airflow-up down clean
