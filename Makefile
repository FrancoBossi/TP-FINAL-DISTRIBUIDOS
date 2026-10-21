PYTHON := .venv/bin/python

DATASET ?= datasets/wikipedia_sample.parquet
REFERENCE_OUTPUT ?= output/reference
DISTRIBUTED_OUTPUT ?= output/distributed


.PHONY: setup test reference compare client gateway lint lint-fix check


setup:
	test -x $(PYTHON) || uv venv .venv
	uv pip install --python $(PYTHON) -r requirements-dev.txt
	$(PYTHON) -m nltk.downloader stopwords


test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s test -v

lint:
	$(PYTHON) -m ruff check src test
	$(PYTHON) -m ruff format --check src test

lint-fix:
	$(PYTHON) -m ruff check --fix src test
	$(PYTHON) -m ruff format src test

check: lint


reference:
	PYTHONPATH=src $(PYTHON) -m reference.run $(DATASET) $(REFERENCE_OUTPUT)


compare:
	PYTHONPATH=src $(PYTHON) -m reference.compare $(REFERENCE_OUTPUT) $(DISTRIBUTED_OUTPUT)

client:
	PYTHONPATH=src INPUT_FILE=$(DATASET) OUTPUT_DIR=$(DISTRIBUTED_OUTPUT) $(PYTHON) -m client.main

gateway:
	PYTHONPATH=src $(PYTHON) -m gateway.main

up:
	COMPOSE_HTTP_TIMEOUT=300 docker compose -f docker-compose.yaml up --build --remove-orphans --detach
	docker compose -f docker-compose.yaml logs --follow
.PHONY: up

down:
	docker compose -f docker-compose.yaml stop -t 5
	docker compose -f docker-compose.yaml down
.PHONY: down

logs:
	docker compose -f docker-compose.yaml logs
.PHONY: logs
