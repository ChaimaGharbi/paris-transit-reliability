# ADR-0005: Silver processing engine: DuckDB

- **Status:** accepted
- **Date:** 2026-11-17

## Context

Silver turns raw bronze snapshots into clean rows: **one row per (snapshot, vehicle journey, stop call)**. The engine must read the nested SIRI JSON, flatten it, deduplicate it and write Parquet.

The scale, measured on my own data:
- one snapshot (Nov 14, 18:52 UTC): **12,186 vehicle journeys, ~100,000 stop calls**, 877 lines; 3.5 MB gzipped, **40.5 MB of JSON** (ratio 11.5 : 1)
- busiest hour (Nov 16, 15 UTC): ~23,000 journeys per snapshot, **2.31 GiB of JSON per hour**
- 720 snapshots a day → about **72 million stop-call rows a day** before deduplication
- it runs on the VM: **2 OCPU, 12 GB RAM**, shared with the poller and later Projects A and C

The snapshot is **one JSON object** with 4 nested lists on the path to a stop call (see Exploring Nested JSON):

```
Siri                              dict
 └ ServiceDelivery                dict
    └ EstimatedTimetableDelivery          list (1 item)
       └ EstimatedJourneyVersionFrame     list (1 item)
          └ EstimatedVehicleJourney       list (~12,000 to 23,000 items)
             └ EstimatedCalls             dict
                └ EstimatedCall           list (stops of one journey)
```

## Decision drivers

- Must handle **more data than RAM** on one small VM
- Must read **nested JSON** and **gzip** directly, and write **Parquet**
- One developer: the least code for flattening and aggregating
- Skills for interviews: data engineering interviews test **SQL** heavily; gold metrics ("median delay per line per hour") are naturally SQL-shaped
- Free and open source

## Options considered

| Option | What it is | For this project |
|---|---|---|
| **Plain Python** (`json` + loops) | what `timing_stats.py` does | fine for one file; slow and verbose for 72M rows a day; every group-by written by hand |
| **pandas** | DataFrames in memory | `json_normalize` handles nesting, but everything lives in RAM: one day of rows does not fit comfortably in 12 GB |
| **Polars** | fast DataFrame library, streams data bigger than RAM | **real alternative**: very fast, reads JSON and Parquet; Python API instead of SQL |
| **DuckDB** | SQL database inside Python, no server | SQL directly on `.json.gz` and Parquet; `UNNEST` for nested lists; reads `date=`/`hour=` folders as columns; streams data bigger than RAM |
| **Spark** | distributed engine for clusters | built for many machines; on one small VM mostly overhead (JVM, memory). Overkill |

## Decision

Chosen option: **DuckDB**, because it reads the gzipped nested JSON and Parquet directly with SQL, handles more data than RAM on one VM, and lets me practise the SQL that data engineering interviews test. Polars would also work; the deciding point is SQL.

## Consequences

- Positive: no server; silver and gold queries are plain SQL; Hive partitions (`date=`, `hour=`) become columns and enable **partition pruning** (a query on one hour opens one folder)
- Negative: nested JSON needs care: DuckDB's default `maximum_object_size` (16 MB) is too small for one snapshot. Set **`maximum_object_size=200000000`** (200 MB, ~2× the biggest snapshot seen: 7.5 MB gzipped × 11.5 ≈ 86 MB of JSON)
- Negative: archives from [ADR-0006](0006-bronze-hourly-zstd-archives.md) (`.tar.zst`) are not readable by DuckDB: silver must read raw hours within the 7-day window
- Risks: one big JSON object per file means DuckDB parses a whole snapshot at once (mitigation: one file at a time, measure memory on the VM)

## Reversal trigger

- If processing one day of snapshots takes **more than 2 hours** on the VM, or uses **more than 8 GB of RAM**, compare with Polars on the same day of data

## After measurement

Fill in when silver runs: time and peak memory per day, rows per day before and after deduplication.

## Links

Related: Previous: [ADR-0004](0004-poller-retries-and-failure-records.md) · Next: [ADR-0006](0006-bronze-hourly-zstd-archives.md)
Sources: [DuckDB: UNNEST](https://duckdb.org/docs/current/sql/query_syntax/unnest) · [DuckDB: Loading JSON](https://duckdb.org/docs/current/data/json/loading_json)
