CONTAINER_RUNTIME ?= docker
IMAGE_NAME ?= ae-challenge-ex02-local
WORKDIR ?= /workspace
BIND_MOUNT_SUFFIX ?= :Z
MODE ?= full-refresh
PROCESS_DATE ?=
PROJECT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PROJECT_MOUNT := $(PROJECT_DIR):$(WORKDIR)$(BIND_MOUNT_SUFFIX)
MODE_ARGS = --mode $(MODE)
PROCESS_DATE_ARGS = $(if $(strip $(PROCESS_DATE)),--process-date $(PROCESS_DATE),)

CONTAINER_RUN = $(CONTAINER_RUNTIME) run --rm 	-v "$(PROJECT_MOUNT)" 	-w $(WORKDIR) 	-e PYTHONPATH=$(WORKDIR)/src 	-e SPARK_LOCAL_IP=127.0.0.1 	$(IMAGE_NAME)

CONTAINER_RUN_TTY = $(CONTAINER_RUNTIME) run --rm -it 	-v "$(PROJECT_MOUNT)" 	-w $(WORKDIR) 	-e PYTHONPATH=$(WORKDIR)/src 	-e SPARK_LOCAL_IP=127.0.0.1 	$(IMAGE_NAME)

.PHONY: help image compile test ingest-bronze build-silver build-gold-purchase-snapshot build-gold-gmv-daily-by-subsidiary run-data-quality shell pyspark smoke

help:
	@printf '%s\n' 'Available targets:'
	@printf '%s\n' '  make image           Build the local container image'
	@printf '%s\n' '  make compile         Run Python compile checks inside the container'
	@printf '%s\n' '  make test            Run the PySpark unit, integration, and data-quality tests inside the container'
	@printf '%s\n' '  make ingest-bronze   Load the sample txt files into local bronze Iceberg tables'
	@printf '%s\n' '  make build-silver    Materialize the silver CDC tables from bronze'
	@printf '%s\n' '  make build-gold-purchase-snapshot  Materialize the gold purchase snapshot'
	@printf '%s\n' '  make build-gold-gmv-daily-by-subsidiary  Materialize the final GMV aggregate'
	@printf '%s\n' '  make run-data-quality  Rebuild the pipeline, publish OPS audit tables, and fail on blocking quality checks'
	@printf '%s\n' '  make pyspark         Open an interactive Spark console already pointed at ae_challenge'
	@printf '%s\n' '  make shell           Open an interactive shell inside the container'
	@printf '%s\n' '  Variables: MODE=full-refresh|incremental PROCESS_DATE=YYYY-MM-DD'
	@printf '%s\n' '  Incremental mode always processes D-1 relative to PROCESS_DATE (or to the current date when omitted)'
	@printf '%s\n' '  make ingest-bronze BIND_MOUNT_SUFFIX=    Disable SELinux relabel when needed'

image:
	$(CONTAINER_RUNTIME) build -t $(IMAGE_NAME) .

compile:
	$(CONTAINER_RUN) python -m compileall src scripts tests

test:
	$(CONTAINER_RUN) pytest -q

ingest-bronze:
	$(CONTAINER_RUN) python scripts/ingest_bronze_local.py $(MODE_ARGS) $(PROCESS_DATE_ARGS)

build-silver: ingest-bronze
	$(CONTAINER_RUN) python scripts/build_silver_local.py $(MODE_ARGS) $(PROCESS_DATE_ARGS)

build-gold-purchase-snapshot: build-silver
	$(CONTAINER_RUN) python scripts/build_gold_purchase_snapshot_local.py $(MODE_ARGS) $(PROCESS_DATE_ARGS)

build-gold-gmv-daily-by-subsidiary: build-gold-purchase-snapshot
	$(CONTAINER_RUN) python scripts/build_gold_gmv_daily_by_subsidiary_local.py $(MODE_ARGS) $(PROCESS_DATE_ARGS)

run-data-quality: build-gold-gmv-daily-by-subsidiary
	$(CONTAINER_RUN) python scripts/run_data_quality_local.py --reset

smoke: ingest-bronze

pyspark:
	$(CONTAINER_RUN_TTY) python scripts/open_bronze_console.py

shell:
	$(CONTAINER_RUN_TTY) bash
