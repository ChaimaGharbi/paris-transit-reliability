"""
- measure how big the CHANGES between two consecutive snapshots are, compared to a full snapshot (ADR-0002)
- a change = a vehicle journey that appeared, disappeared, or has any different field
- journeys are matched by DatedVehicleJourneyRef
- two versions of the measure:
    all fields       -> everything counts as a change
    without RecordedAtTime -> ignores the "last updated" timestamp, which can move even when nothing else did
- for each pair: changes file size (gzipped JSON of the changed journeys) / full snapshot size (gzipped)
- usage: python scripts/measure_changes.py 2026-11-16            (one UTC day, all hours)
         python scripts/measure_changes.py 2026-11-16 05 15      (only these hours)
"""


import sys
import os
import json
import gzip
import statistics
import time
import fsspec
from datetime import datetime
from dotenv import load_dotenv


load_dotenv()

BRONZE_ROOT = os.getenv("BRONZE_ROOT")
fs, root = fsspec.core.url_to_fs(BRONZE_ROOT)

VOLATILE = ["RecordedAtTime"]  # fields that may change on every snapshot without meaning anything


def load_journeys(path):
    with fs.open(path, "rb") as f:
        snapshot = json.load(gzip.open(f))
    journeys = {}
    for delivery in snapshot["Siri"]["ServiceDelivery"]["EstimatedTimetableDelivery"]:
        for frame in delivery["EstimatedJourneyVersionFrame"]:
            for journey in frame["EstimatedVehicleJourney"]:
                key = journey.get("DatedVehicleJourneyRef", {}).get("value")
                journeys[key] = journey
    return journeys


def changed(old, new, ignore=()):
    # keys that appeared, disappeared, or whose content differs
    added = [k for k in new if k not in old]
    removed = [k for k in old if k not in new]
    modified = []
    for k in new:
        if k in old:
            a = {f: v for f, v in old[k].items() if f not in ignore}
            b = {f: v for f, v in new[k].items() if f not in ignore}
            if a != b:
                modified.append(k)
    return added, removed, modified


def change_size(new, added, removed, modified):
    # what a change file would hold: full new/modified journeys + the keys of removed ones, gzipped
    record = {"upsert": [new[k] for k in added + modified], "removed": removed}
    return len(gzip.compress(json.dumps(record, separators=(",", ":")).encode()))


def seconds_between(path_a, path_b):
    # filenames are like 20261116T051401Z.json.gz
    stamp = lambda p: datetime.strptime(p.split("/")[-1].split(".")[0], "%Y%m%dT%H%M%SZ")
    return (stamp(path_b) - stamp(path_a)).total_seconds()


def main():
    date = sys.argv[1]
    hours = sys.argv[2:]
    files = sorted(fs.glob(f"{root}/prim_global/date={date}/hour=*/*.json.gz"))
    if hours:
        files = [p for p in files if p.split("/")[-2].split("=")[-1] in hours]
    print(f"{len(files)} snapshots on {date}" + (f" hours {hours}" if hours else ""), flush=True)

    rows = []
    previous, previous_path = None, None
    for path in files:
        t0 = time.time()
        current = load_journeys(path)
        # only compare snapshots taken 2 min apart: a gap (failed run, skipped hour) is not a real pair
        if previous is not None and seconds_between(previous_path, path) <= 150:
            full = fs.size(path)
            a, r, m = changed(previous, current)
            a2, r2, m2 = changed(previous, current, VOLATILE)
            row = {
                "hour": path.split("/")[-2].split("=")[-1],
                "file": path.split("/")[-1],
                "journeys": len(current),
                "full_bytes": full,
                "added": len(a), "removed": len(r), "modified": len(m), "modified_wo_volatile": len(m2),
                "ratio_all": change_size(current, a, r, m) / full,
                "ratio_wo_volatile": change_size(current, a2, r2, m2) / full,
                "identical": len(a2) == len(r2) == len(m2) == 0,
            }
            rows.append(row)
            print(f"{row['file']} journeys={row['journeys']} +{row['added']} -{row['removed']} ~{row['modified']} "
                  f"(~{row['modified_wo_volatile']} without volatile) ratio={row['ratio_all']:.1%} "
                  f"({row['ratio_wo_volatile']:.1%} without volatile) {time.time() - t0:.1f}s", flush=True)
        previous, previous_path = current, path

    if not rows:
        print("no consecutive pairs found")
        return

    print("\n=== summary ===")
    for name in ["ratio_all", "ratio_wo_volatile"]:
        values = [row[name] for row in rows]
        print(f"{name}: median {statistics.median(values):.1%}, mean {statistics.mean(values):.1%}, max {max(values):.1%}")
    full_total = sum(row["full_bytes"] for row in rows)
    change_total = sum(row["ratio_wo_volatile"] * row["full_bytes"] for row in rows)
    print(f"bytes: full snapshots {full_total / 1e9:.2f} GB, changes only {change_total / 1e9:.2f} GB "
          f"({change_total / full_total:.1%}), plus 1 keyframe per hour on top")
    print(f"identical consecutive snapshots (no change at all): {sum(row['identical'] for row in rows)} of {len(rows)}")
    print("\nper hour (median ratio without volatile):")
    by_hour = {}
    for row in rows:
        by_hour.setdefault(row["hour"], []).append(row["ratio_wo_volatile"])
    for hour in sorted(by_hour):
        print(f"  {hour}: {statistics.median(by_hour[hour]):.1%}  ({len(by_hour[hour])} pairs)")


if __name__ == "__main__":
    main()
