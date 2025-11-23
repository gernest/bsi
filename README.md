# BSI - columnar storage engine for Prometheus
BSI is a new storage engine for Prometheus, it replaces existing `tsdb` used by 
upstream prometheus with a compressed roaring bitmaps based storage that has
the following core features.

- **Low memory footprint** 
- **Low disc storage footprint**
- **Fast and efficient**
- **Permanent storage**
- **Fully promql compliant**
- **Designed for high cardinality**

With BSI there is no need for third party remote storage, a single prometheus instance 
can handle up to 1 million active timeseries without being OOM killed.

Below figure shows amount of ram after ingesting `108000000` rows with `10000` active timeseries.

<p align="center">
  <picture>
    <source media="(prefers-color-scheme: dark)" srcset="./benchmarks/ram-dark.svg">
    <source media="(prefers-color-scheme: light)" srcset="./benchmarks/ram-light.svg">
    <img alt="Bar chart with ram benchmark results" src="./benchmarks/ram-light.svg">
  </picture>
</p>

**Please run your own benchmarks with real data before reaching any conclusions**

With **BSI** there is no big difference between a million active timeseries and  one active
timeseries in term of memory consumption.