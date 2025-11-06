# BSI - disc based storage engine for Prometheus
BSI is a new storage engine for Prometheus, it replaces existing `tsdb` used by 
upstream prometheus with a compressed roaring bitmaps based storage that stores has
the following core features.

- **Low memory footprint** 
- **Low disc storage footprint**
- **fast and efficient**
- **permanent storage**
- **Fully prometheus**
- **High cardinality**

With BSI there is no need for third party remote storage, a single prometheus instance 
can handle up to 1 million active timeseries without being OOM killed.