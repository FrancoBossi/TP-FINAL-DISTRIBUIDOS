PYTHON := .venv/bin/python

DATASET ?= datasets/wikipedia_sample.parquet
REFERENCE_OUTPUT ?= output/reference
DISTRIBUTED_OUTPUT ?= output/distributed


.PHONY: setup test reference compare


setup:
	test -x $(PYTHON) || uv venv .venv
	uv pip install --python $(PYTHON) -r requirements.txt
	$(PYTHON) -m nltk.downloader stopwords


test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s test -v


reference:
	PYTHONPATH=src $(PYTHON) -m reference.run $(DATASET) $(REFERENCE_OUTPUT)


compare:
	PYTHONPATH=src $(PYTHON) -m reference.compare $(REFERENCE_OUTPUT) $(DISTRIBUTED_OUTPUT)
