CONTAINER_RUNTIME ?= docker
IMAGE_NAME ?= ae-challenge-ex02-local
WORKDIR ?= /workspace
BIND_MOUNT_SUFFIX ?= :Z
PROJECT_DIR := $(abspath $(dir $(lastword $(MAKEFILE_LIST))))
PROJECT_MOUNT := $(PROJECT_DIR):$(WORKDIR)$(BIND_MOUNT_SUFFIX)

CONTAINER_RUN = $(CONTAINER_RUNTIME) run --rm 	-v "$(PROJECT_MOUNT)" 	-w $(WORKDIR) 	-e PYTHONPATH=$(WORKDIR)/src 	-e SPARK_LOCAL_IP=127.0.0.1 	$(IMAGE_NAME)

CONTAINER_RUN_TTY = $(CONTAINER_RUNTIME) run --rm -it 	-v "$(PROJECT_MOUNT)" 	-w $(WORKDIR) 	-e PYTHONPATH=$(WORKDIR)/src 	-e SPARK_LOCAL_IP=127.0.0.1 	$(IMAGE_NAME)

.PHONY: help image compile ingest-bronze build-silver build-gold-purchase-snapshot shell pyspark smoke

help:
	@printf '%s\n' 'Available targets:'
	@printf '%s\n' '  make image           Build the local container image'
	@printf '%s\n' '  make compile         Run Python compile checks inside the container'
	@printf '%s\n' '  make ingest-bronze   Load the sample txt files into local bronze Iceberg tables'
	@printf '%s\n' '  make build-silver    Rebuild the local bronze sample and materialize the silver CDC tables'
	@printf '%s\n' '  make build-gold-purchase-snapshot  Rebuild bronze and silver, then materialize the gold purchase snapshot'
	@printf '%s\n' '  make pyspark         Open an interactive Spark console already pointed at ae_challenge'
	@printf '%s\n' '  make shell           Open an interactive shell inside the container'
	@printf '%s\n' '  make ingest-bronze BIND_MOUNT_SUFFIX=    Disable SELinux relabel when needed'

image:
	$(CONTAINER_RUNTIME) build -t $(IMAGE_NAME) .

compile:
	$(CONTAINER_RUN) python -m compileall src scripts

ingest-bronze:
	$(CONTAINER_RUN) python scripts/ingest_bronze_local.py --reset

build-silver: ingest-bronze
	$(CONTAINER_RUN) python scripts/build_silver_local.py --reset

build-gold-purchase-snapshot: build-silver
	$(CONTAINER_RUN) python scripts/build_gold_purchase_snapshot_local.py --reset

smoke: ingest-bronze

pyspark:
	$(CONTAINER_RUN_TTY) python scripts/open_bronze_console.py

shell:
	$(CONTAINER_RUN_TTY) bash
