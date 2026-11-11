# Architecture Decision Records

Each record explains one decision: the context, the options considered, what was chosen and why, and when to reverse it.

| ADR | Decision | Status |
|---|---|---|
| [0001](0001-overall-data-architecture.md) | Overall data architecture: Kappa, the bronze files are the log until streaming | accepted |
| [0002](0002-bronze-snapshots-and-changes.md) | Bronze storage: full snapshots for 7 days, then hourly keyframes + changes | accepted |
| [0003](0003-bronze-storage-plain-files-fsspec.md) | Where bronze lives: plain files on the VM disk, written through fsspec | accepted |
| [0004](0004-poller-retries-and-failure-records.md) | Poller retries, timeouts and failure records | accepted |
