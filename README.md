# BSI - columnar storage engine for Prometheus
BSI is a new storage engine for Prometheus, it replaces existing `tsdb` used by 
upstream prometheus with a compressed roaring bitmaps based storage that has
the following core features.

- **Low memory footprint** 
- **Low disc storage footprint**
- **Fast and efficient**
- **Permanent storage**
- **Fully promql compliant**
- **High cardinality**

With BSI there is no need for third party remote storage, a single prometheus instance 
can handle up to 1 million active timeseries without being OOM killed.