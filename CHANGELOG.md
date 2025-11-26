# tip
- FEATURE: support building  `386`, `arm`, `mips` , `mipsle` and `ppc` binaries. See <https://github.com/gernest/bsi/pull/1>
- FEATURE: override maximum samples database size. By default we use 2GB for 64 bit arch and 1 GB for 32 bit arch .
With this flag we can set any value up to 256TB for 64 bit arch and 2GB for 32 bit arch. See <https://github.com/gernest/bsi/pull/3>
- BUGFIX: align snapshot destination to match tsdb. See <https://github.com/gernest/bsi/pull/2>