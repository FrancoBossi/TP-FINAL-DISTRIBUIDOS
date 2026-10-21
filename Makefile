PYTHON := .venv/bin/python

DATASET ?= datasets/wikipedia_sample.parquet
REFERENCE_OUTPUT ?= output/reference
DISTRIBUTED_OUTPUT ?= output/distributed


.PHONY: setup test reference compare lint lint-fix check


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
