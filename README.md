# BSI
Fast and cost effective prometheus

- **Low memory footprint** : series data is kept on disc
- **Low storage footprint**: samples are stored as Compressed Roaring Bitmaps
- **Fast and efficient**: there is no decompression step once data is ingested.
- **Permanent storage**: you don't need remote storage
-- **Fully prometheus**: we only replace storage, the rest is the same code as prometheus.
-- **High cardinality**: we support high cardinality time series data at scale.

## deprecations
- `prometheus_tsdb_head_min_time_seconds` is removed
- `prometheus_tsdb_head_max_time_seconds` is renamed to `prometheus_tsdb_max_time_seconds`