TSBS_SCALE := 10000
# If GNU date is available, use it; otherwise, fall back to the standard date command
# User can install GNU date on macOS via `brew install coreutils`
DATE_CMD := $(shell which gdate 2>/dev/null || echo date)
TSBS_START := $(shell $(DATE_CMD) -u -d "1 day ago 00:00:00" +"%Y-%m-%dT%H:%M:%SZ")
TSBS_END   := $(shell $(DATE_CMD) -u -d "00:00:00" +"%Y-%m-%dT%H:%M:%SZ")
TSBS_STEP := 80s
TSBS_QUERIES := 10000
TSBS_WORKERS := 4
TSBS_DATA_FILE := ./benchmarks/tsbs-data-$(TSBS_SCALE)-$(TSBS_START)-$(TSBS_END)-$(TSBS_STEP).tsbs
TSBS_QUERY_FILE := ./benchmarks/tsbs-queries-$(TSBS_SCALE)-$(TSBS_START)-$(TSBS_END)-$(TSBS_QUERIES).gz

TSBS_READ_URLS := http://localhost:9090

generate: tsbs-generate-data tsbs-generate-queries

# Generate sample time series data
tsbs-generate-data:
	test -f $(TSBS_DATA_FILE) || tsbs_generate_data \
		--format=prometheus \
		--use-case=cpu-only  \
		--seed=4318 \
		--scale=$(TSBS_SCALE) \
		--timestamp-start=$(TSBS_START) \
		--timestamp-end=$(TSBS_END) \
		--log-interval=$(TSBS_STEP) \
		> $(TSBS_DATA_FILE)

tsbs-generate-queries:
	test -f $(TSBS_QUERY_FILE) || tsbs_generate_queries \
		--format=victoriametrics \
		--use-case=cpu-only \
		--seed=8428 \
		--scale=$(TSBS_SCALE) \
		--timestamp-start=$(TSBS_START) \
		--timestamp-end=$(TSBS_END) \
		--query-type=cpu-max-all-8 \
		--queries=$(TSBS_QUERIES) \
		| gzip > $(TSBS_QUERY_FILE)

tsbs-load-data:
	tsbs_load load prometheus --config=./benchmarks/config.yml

tsbs-query-data:
	cat $(TSBS_QUERY_FILE) | gunzip | tsbs_run_queries_victoriametrics --workers=$(TSBS_WORKERS) --urls=$(TSBS_READ_URLS)

start:
	./bsi --web.enable-remote-write-receiver

cpu:
	go tool pprof -http=:8080 http://localhost:9090/debug/pprof/profile
heap:
	go tool pprof -http=:8080 http://localhost:9090/debug/pprof/heap