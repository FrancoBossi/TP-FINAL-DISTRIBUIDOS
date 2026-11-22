PYTHON := .venv/bin/python

DATASET ?= datasets/wikipedia_sample.parquet
REFERENCE_OUTPUT ?= output/reference
DISTRIBUTED_OUTPUT ?= output/distributed
RUNS ?= 3
BENCHMARK_RESULTS ?= benchmarks/results
BENCHMARK_PROJECT ?= wikipedia-benchmark
BENCHMARK_SAMPLE_SECONDS ?= 5
SMOKE_DATASET ?= datasets/wikipedia_sample.parquet
BASELINE_DATASET ?= datasets/wikipedia_benchmark_12.parquet
BASELINE_SHARDS ?= 12


.PHONY: setup test reference compare client gateway lint lint-fix check \
	benchmark-smoke-dataset benchmark-smoke benchmark-baseline-dataset \
	benchmark-baseline


setup:
	test -x $(PYTHON) || uv venv .venv
	uv pip install --python $(PYTHON) -r requirements-dev.txt
	$(PYTHON) -m nltk.downloader stopwords


test:
	PYTHONPATH=src $(PYTHON) -m unittest discover -s test -v

lint:
	$(PYTHON) -m ruff check src test scripts/run_benchmark.py scripts/chaos_monkey.py scripts/monitor_chaos.py scripts/compare_benchmarks.py
	$(PYTHON) -m ruff format --check src test scripts/run_benchmark.py scripts/chaos_monkey.py scripts/monitor_chaos.py scripts/compare_benchmarks.py

lint-fix:
	$(PYTHON) -m ruff check --fix src test scripts/run_benchmark.py scripts/chaos_monkey.py scripts/monitor_chaos.py scripts/compare_benchmarks.py
	$(PYTHON) -m ruff format src test scripts/run_benchmark.py scripts/chaos_monkey.py scripts/monitor_chaos.py scripts/compare_benchmarks.py

check: lint


reference:
	PYTHONPATH=src $(PYTHON) -m reference.run $(DATASET) $(REFERENCE_OUTPUT)


compare:
	PYTHONPATH=src $(PYTHON) -m reference.compare $(REFERENCE_OUTPUT) $(DISTRIBUTED_OUTPUT)

client:
	PYTHONPATH=src INPUT_FILE=$(DATASET) OUTPUT_DIR=$(DISTRIBUTED_OUTPUT) $(PYTHON) -m client.main

gateway:
	PYTHONPATH=src $(PYTHON) -m gateway.main

benchmark-smoke-dataset:
	test -f $(SMOKE_DATASET) || uv run scripts/download_wikipedia_sample.py --files-per-language 1 --output $(SMOKE_DATASET)

benchmark-smoke: benchmark-smoke-dataset
	$(PYTHON) scripts/run_benchmark.py \
		--dataset $(SMOKE_DATASET) \
		--runs 1 \
		--results-root $(BENCHMARK_RESULTS) \
		--expected-output $(REFERENCE_OUTPUT) \
		--project-name $(BENCHMARK_PROJECT)-smoke \
		--sample-seconds $(BENCHMARK_SAMPLE_SECONDS)

benchmark-baseline-dataset:
	test -f $(BASELINE_DATASET) || uv run scripts/download_wikipedia_sample.py --files-per-language $(BASELINE_SHARDS) --output $(BASELINE_DATASET)

benchmark-baseline: benchmark-baseline-dataset
	$(PYTHON) scripts/run_benchmark.py \
		--dataset $(BASELINE_DATASET) \
		--runs $(RUNS) \
		--results-root $(BENCHMARK_RESULTS) \
		--project-name $(BENCHMARK_PROJECT) \
		--sample-seconds $(BENCHMARK_SAMPLE_SECONDS)

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
