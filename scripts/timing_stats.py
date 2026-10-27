import json
import fsspec
import statistics
from dotenv import load_dotenv
import os
import datetime


load_dotenv()

# read all meta_data
def load_meta(fs, root) -> list[dict]:
    meta_data_files = fs.glob(f"{root}/prim_global/date=*/hour=*/*.meta.json")
    results = []
    for path in meta_data_files:
        with fs.open(path, "r") as f:
            metadata = json.load(f)
            hour = None if metadata.get("request_time") is None else datetime.datetime.fromisoformat(metadata.get("request_time")).hour
            status = metadata.get("status", "ok")
            results.append({"path": path, "hour": hour, "time_to_headers": metadata.get("duration_time_to_headers"), "stream_duration" :metadata.get("stream_duration"), "status": status})
    return results


def print_stats(name, values):
    values_not_none = [x for x in values if x is not None]
    count = len(values_not_none)
    if count == 0:
        print("name: ", name, " No Data!")
        return
    elif count == 1:
        median = p95 = p99 = maximum = values_not_none[0]
    else:
        median = statistics.median(values_not_none)
        cuts = statistics.quantiles(values_not_none, n=100)
        p95, p99 = cuts[94], cuts[98]
        maximum = max(values_not_none)
    
    print("Name: ", name, "\n***********" , "\ncount: ", count, f"({len(values)-count} None removed)","\nmedian: ", f"{median:.3f}", "\np95: ", f"{p95:.3f}", "\np99: ", f"{p99:.3f}", "\nmaximum: ", f"{maximum:.3f}")

def main():
    BRONZE_ROOT = os.getenv("BRONZE_ROOT")
    fs, root = fsspec.core.url_to_fs(BRONZE_ROOT)
    extracted_metadata = load_meta(fs, root)
    time_to_headers = []
    stream_duration = []
    total_time = []
    per_hour_time_to_headers = {}
    for meta in extracted_metadata:
        time_to_headers.append(meta["time_to_headers"])
        stream_duration.append(meta["stream_duration"])
        total_time.append(None if meta["time_to_headers"] is None or meta["stream_duration"] is None else meta["time_to_headers"]+meta["stream_duration"])
        if meta["hour"] is not None:
            per_hour_time_to_headers.setdefault(meta["hour"], []).append(meta["time_to_headers"])
    print_stats("time_to_headers", time_to_headers)
    print_stats("stream_duration", stream_duration)
    print_stats("total_time", total_time)
    per_hour_time_to_headers_sorted = dict(sorted(per_hour_time_to_headers.items()))
    for hour in per_hour_time_to_headers_sorted.keys():
        print_stats(hour, per_hour_time_to_headers_sorted[hour])

if __name__ == "__main__":
    main()
        