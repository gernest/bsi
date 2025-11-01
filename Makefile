TSBS_SCALE := 1000
# If GNU date is available, use it; otherwise, fall back to the standard date command
# User can install GNU date on macOS via `brew install coreutils`
DATE_CMD := $(shell which gdate 2>/dev/null || echo date)
TSBS_START := $(shell $(DATE_CMD) -u -d "1 day ago 00:00:00" +"%Y-%m-%dT%H:%M:%SZ")
TSBS_END   := $(shell $(DATE_CMD) -u -d "00:00:00" +"%Y-%m-%dT%H:%M:%SZ")
TSBS_STEP := 80s
TSBS_DATA_FILE := ./benchmarks/$(TSBS_SCALE)-$(TSBS_START)-$(TSBS_END)-$(TSBS_STEP).tsbs

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

tsbs-load-data:
	tsbs_load load prometheus --config=./benchmarks/config.yml

start:
	./bsi --web.enable-remote-write-receiver