# Architecture Decision Records

Each record explains one decision: the context, the options considered, what was chosen and why, and when to reverse it.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-overall-data-architecture.md) | Overall data architecture: Kappa, the bronze files are the log until streaming | accepted |
| [0002](0002-bronze-snapshots-and-changes.md) | Bronze storage: full snapshots for 7 days, then hourly keyframes + changes | superseded by 0006 |
| [0003](0003-bronze-storage-plain-files-fsspec.md) | Where bronze lives: plain files on the VM disk, written through fsspec | accepted |
| [0004](0004-poller-retries-and-failure-records.md) | Poller retries, timeouts and failure records | accepted |
| [0005](0005-silver-engine-duckdb.md) | Silver processing engine: DuckDB | accepted |
| [0006](0006-bronze-hourly-zstd-archives.md) | Long-term bronze: hourly zstd archives, verified before raw is deleted | accepted |
| [0007](0007-silver-grain-last-state-per-call.md) | Silver grain: the last known state of each stop call, built in two steps (hourly, daily) | accepted |
