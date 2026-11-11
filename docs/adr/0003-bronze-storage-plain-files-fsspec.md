# ADR-0003: Where bronze lives (object storage)

- **Status:** accepted
- **Date:** 2026-10-15

## Context

Bronze is the replayable log of [ADR-0001](0001-overall-data-architecture.md) (Kappa), stored as defined in [ADR-0002](0002-bronze-snapshots-and-changes.md) (raw gzipped snapshots for 7 days, then keyframes + changes). The project runs on **one free server**: Oracle Cloud Always Free, Arm, 2 OCPU, 12 GB RAM, **191 GB free disk** (see Oracle VM Setup). Budget: **zero**.

In Level 1 there is **one machine, one writer (the poller), one reader (me, then the Level 2 job on the same machine)**.

The cahier planned **MinIO**. On 2026-10-15 I found that the open-source MinIO repository is **no longer maintained**; MinIO now points to **AIStor Free** (proprietary, single node) and paid tiers.

## Decision drivers

- **Solve today's need only:** no service that Level 1 does not need
- **Zero budget, one small VM:** every extra service costs RAM and monitoring
- **Easy to switch later:** moving to S3-compatible storage must not mean a rewrite
- **Long-lived portfolio:** others should be able to clone and run it for years

## Options considered

### Option 1: Plain files on the VM disk, written through fsspec
Layout: `bronze/<source>/date=YYYY-MM-DD/hour=HH/<timestamp>.json.gz` + metadata.
- Pros: **nothing extra to run**; 191 GB free; fastest to build; `fsspec` lets the same code write to `file://` now and `s3://` later
- Cons: no S3 API to practise yet; tied to this one machine

### Option 2: AIStor Free (MinIO's free edition)
- Pros: real S3 API, the most common S3 server in industry; free; single node is all I have
- Cons: **proprietary** (no modification or redistribution); MinIO can change it "at its sole discretion" and already dropped its open-source edition once; one more service on the VM

### Option 3: Open-source S3 servers: Garage, SeaweedFS, RustFS
- Pros: S3 API, open source, no vendor risk (Garage is built for small self-hosted setups)
- Cons: one more service to run and debug; no Level 1 need for it

(A Chainguard-maintained fork of MinIO also exists; it mainly helps projects **already** depending on MinIO, which this one does not.)


### Option 4: Oracle Object Storage (Always Free)
- Pros: managed, nothing to run
- Cons: **only 20 GB** and 50,000 requests a month; bronze is ~110 GB a month raw

## Decision

Chosen option: **Option 1, plain files on the VM disk, written through `fsspec`**, because Level 1 has one machine, one writer and one reader: an S3 server would solve no current problem and add a service to run. Writing through `fsspec` keeps the switch to S3 a **configuration change** (`file://` → `s3://`), not a rewrite.

## Consequences

- Positive: the poller ships today; no extra service; the storage path is one setting.
- Negative: no S3 practice in Level 1; bronze exists on one disk only (no redundancy).
- Risks: disk loss = bronze loss (mitigation: backups, a later task); hard-coded local paths sneaking into code (mitigation: every read and write goes through one storage helper using `fsspec`).

## Reversal trigger

Add an S3-compatible server (then choose between Garage, SeaweedFS, RustFS and AIStor Free, with numbers) **when any of these becomes true**:
- a **second machine or service** must read or write bronze (e.g. Level 3 serving, Level 5 broker on another host)
- the local disk passes **70% full** even after compaction
- a tool I need can only read from S3

## After measurement

Filled in later: disk used per day, time to switch a path to `s3://` when it happens.

## Links

Related: [ADR-0002](0002-bronze-snapshots-and-changes.md)
Sources: [MinIO repository (archived)](https://github.com/minio/minio) · [AIStor Free agreement](https://www.min.io/legal/aistor-free-agreement) · [AIStor tiers](https://www.min.io/blog/introducing-new-subscription-tiers-for-minio-aistor-free-enterprise-lite-and-enterprise) · [Oracle Always Free resources](https://docs.oracle.com/en-us/iaas/Content/FreeTier/freetier_topic-Always_Free_Resources.htm) · [fsspec](https://filesystem-spec.readthedocs.io/)
