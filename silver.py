"""
- silver = one row per stop call (journey, stop, aimed time) with its LAST known state (ADR-0007)
- step 1, hourly: raw hour -> last observation of each call inside that hour -> silver_hourly/date={date}/hour={hour}/calls.parquet
- step 2, daily: hourly files of the day + the 3 hours after midnight -> last observation over the whole day -> silver/calls/date={date}/calls.parquet
- rules (exercises 3 and 4): delay_s = expected - aimed departure, NULL for no_aimed, no_report, cancelled, implausible
- implausible = journey median delay > 60 min or < -10 min (ADR-0007)
- every run finds the work left to do by itself (hours without hourly file, finished days without daily file): run it every hour
- silver_hourly is kept 7 days like raw, then removed once the daily files that need it exist
- usage: python silver.py            (everything that is ready)
         python silver.py hour 2026-11-14 18
         python silver.py day 2026-11-14
"""


from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os
import sys
import pytz
import fsspec
import duckdb


load_dotenv()

BRONZE_ROOT = os.getenv("BRONZE_ROOT")
fs, root = fsspec.core.url_to_fs(BRONZE_ROOT)

# silver next to bronze: data/bronze -> data/silver
SILVER_ROOT = os.getenv("SILVER_ROOT", BRONZE_ROOT.rstrip("/").rsplit("/", 1)[0] + "/silver")
fs_silver, silver_root = fsspec.core.url_to_fs(SILVER_ROOT)

MAX_OBJECT_SIZE = 200_000_000  # one snapshot is one JSON object, up to ~86 MB seen (ADR-0005)
LOOKAHEAD_HOURS = 3     # calls of 23:xx are still seen after midnight
LATE_LIMIT_S = 60 * 60    # journey median delay above -> implausible (exercise 4: p99 = 17.8 min)
EARLY_LIMIT_S = -10 * 60   # journey median delay below -> implausible (p1 = -3.9 min)
HOUR_DONE_AFTER = 15 * 60  # an hour is finished 15 min after it ends (last poller run + retries)
HOURLY_KEEP_DAYS = 7    # silver_hourly kept like raw: 7 days, then only the daily file is needed

# only the fields silver uses, with their types: a field missing from a snapshot becomes NULL instead of an error,
# and duckdb does not have to guess the schema of 30 big files
CALL = ("STRUCT(StopPointRef STRUCT(value VARCHAR), AimedDepartureTime TIMESTAMPTZ, ExpectedDepartureTime TIMESTAMPTZ, "
        "AimedArrivalTime TIMESTAMPTZ, ExpectedArrivalTime TIMESTAMPTZ, DepartureStatus VARCHAR, ArrivalStatus VARCHAR)")
JOURNEY = f"STRUCT(LineRef STRUCT(value VARCHAR), DatedVehicleJourneyRef STRUCT(value VARCHAR), EstimatedCalls STRUCT(EstimatedCall {CALL}[]))"
SIRI = f"STRUCT(ServiceDelivery STRUCT(EstimatedTimetableDelivery STRUCT(EstimatedJourneyVersionFrame STRUCT(EstimatedVehicleJourney {JOURNEY}[])[])[]))"


def connect():
    con = duckdb.connect()
    con.sql("SET TimeZone = 'UTC'")
    con.sql("SET threads = 2")   # 2 OCPU, the poller and the archive job run too
    con.sql("SET memory_limit = '6GB'")
    return con


def now():
    return datetime.now(pytz.utc)


def hour_paths(date, hour):
    raw_dir = f"{root}/prim_global/date={date}/hour={hour:02d}"
    hourly = f"{silver_root}/silver_hourly/date={date}/hour={hour:02d}/calls.parquet"
    return raw_dir, hourly


def day_path(date):
    return f"{silver_root}/calls/date={date}/calls.parquet"


def write_parquet(con, query, path):
    # duckdb writes to a temp name, then one move: the parquet appears complete or not at all
    fs_silver.makedirs(path.rsplit("/", 1)[0], exist_ok=True)
    tmp = path + ".tmp"
    con.sql(f"COPY ({query}) TO '{tmp}' (FORMAT parquet)")
    fs_silver.mv(tmp, path)


def silver_hour(con, date, hour):
    start = time.time()
    raw_dir, hourly = hour_paths(date, hour)
    files = sorted(fs.glob(f"{raw_dir}/*.json.gz"))
    if not files:
        print(f"{now().isoformat(timespec='seconds')} silver hour {date}/{hour:02d} no raw files, skipped", flush=True)
        return "skipped"
    file_list = "[" + ", ".join(f"'{p}'" for p in files) + "]"

    query = f"""
    WITH journeys AS (
        SELECT filename, unnest(Siri.ServiceDelivery.EstimatedTimetableDelivery[1].EstimatedJourneyVersionFrame[1].EstimatedVehicleJourney) AS j
        FROM read_json({file_list}, maximum_object_size={MAX_OBJECT_SIZE}, filename=true, columns={{'Siri': '{SIRI}'}})
    ),
    calls AS (
        SELECT filename, j, unnest(j.EstimatedCalls.EstimatedCall) AS k
        FROM journeys
    ),
    observations AS (
        -- time of the snapshot from its name (20261114T185201Z.json.gz)
        SELECT strptime(split_part(split_part(filename, '/', -1), '.', 1), '%Y%m%dT%H%M%SZ')::TIMESTAMPTZ as snapshot_time,
               j.LineRef.value as line_ref, j.DatedVehicleJourneyRef.value as journey_ref, k.StopPointRef.value as stop_ref,
               coalesce(k.AimedDepartureTime, k.AimedArrivalTime) as aimed_time,
               k.AimedDepartureTime as aimed_departure, k.ExpectedDepartureTime as expected_departure,
               k.AimedArrivalTime as aimed_arrival, k.ExpectedArrivalTime as expected_arrival,
               k.DepartureStatus as departure_status, k.ArrivalStatus as arrival_status
        FROM calls
    )
    -- one call = (journey, stop, aimed time): keep its last observation of the hour
    SELECT * FROM observations
    QUALIFY row_number() OVER (PARTITION BY journey_ref, stop_ref, aimed_time ORDER BY snapshot_time DESC) = 1
    """
    try:
        write_parquet(con, query, hourly)
    except Exception as e:
        # a broken hour must not stop the other hours: log it, try again next run
        print(f"{now().isoformat(timespec='seconds')} silver hour {date}/{hour:02d} status=failed error={type(e).__name__}: {str(e)[:300]}", flush=True)
        return "failed"
    rows = con.sql(f"SELECT COUNT(*) FROM read_parquet('{hourly}')").fetchall()[0][0]
    print(f"{now().isoformat(timespec='seconds')} silver hour {date}/{hour:02d} status=ok snapshots={len(files)} calls={rows} total={time.time() - start:.1f}s", flush=True)
    return "ok"


def silver_day(con, date):
    start = time.time()
    day = datetime.strptime(str(date), "%Y-%m-%d")
    # the day's hours + the first hours of the next day, only the ones that exist
    wanted = [(day + timedelta(hours=h)) for h in range(24 + LOOKAHEAD_HOURS)]
    hourly_files = [hour_paths(t.date(), t.hour)[1] for t in wanted]
    hourly_files = [p for p in hourly_files if fs_silver.exists(p)]
    if not hourly_files:
        print(f"{now().isoformat(timespec='seconds')} silver day {date} no hourly files, skipped", flush=True)
        return "skipped"
    file_list = "[" + ", ".join(f"'{p}'" for p in hourly_files) + "]"

    query = f"""
    WITH observations AS (
        SELECT * FROM read_parquet({file_list}, union_by_name=true)
    ),
    last_state AS (
        SELECT * FROM observations
        QUALIFY row_number() OVER (PARTITION BY journey_ref, stop_ref, aimed_time ORDER BY snapshot_time DESC) = 1
    ),
    of_the_day AS (
        -- a call belongs to the day of its planned time (expected if there is no planned time)
        SELECT * FROM last_state
        WHERE CAST(coalesce(aimed_time, expected_departure, expected_arrival) AS DATE) = DATE '{date}'
    ),
    with_delay AS (
        SELECT *,
               CASE WHEN departure_status IN ('NO_REPORT', 'CANCELLED') THEN NULL
                    ELSE date_diff('second', aimed_departure, expected_departure) END as raw_delay_s
        FROM of_the_day
    ),
    journeys AS (
        SELECT journey_ref, median(raw_delay_s) as journey_median_s
        FROM with_delay WHERE raw_delay_s IS NOT NULL GROUP BY journey_ref
    )
    SELECT w.snapshot_time as last_seen, w.line_ref, w.journey_ref, w.stop_ref, w.aimed_time,
           w.aimed_departure, w.expected_departure, w.aimed_arrival, w.expected_arrival, w.departure_status, w.arrival_status,
           -- one category per row, checked in this order (exercise 3)
           CASE WHEN w.departure_status = 'CANCELLED' THEN 'cancelled'
                WHEN w.departure_status = 'NO_REPORT' THEN 'no_report'
                WHEN w.aimed_departure IS NULL THEN 'no_aimed'
                WHEN w.expected_departure IS NULL THEN 'no_expected'
                WHEN jr.journey_median_s > {LATE_LIMIT_S} OR jr.journey_median_s < {EARLY_LIMIT_S} THEN 'implausible'
                ELSE 'measured' END as category,
           CASE WHEN jr.journey_median_s > {LATE_LIMIT_S} OR jr.journey_median_s < {EARLY_LIMIT_S} THEN NULL
                ELSE w.raw_delay_s END as delay_s
    FROM with_delay w LEFT JOIN journeys jr USING (journey_ref)
    """
    path = day_path(date)
    try:
        write_parquet(con, query, path)
    except Exception as e:
        print(f"{now().isoformat(timespec='seconds')} silver day {date} status=failed error={type(e).__name__}: {str(e)[:300]}", flush=True)
        return "failed"
    counts = dict(con.sql(f"SELECT category, COUNT(*) FROM read_parquet('{path}') GROUP BY category").fetchall())
    print(f"{now().isoformat(timespec='seconds')} silver day {date} status=ok hourly_files={len(hourly_files)} calls={sum(counts.values())} "
          f"categories={counts} total={time.time() - start:.1f}s", flush=True)
    return "ok"


def todo():
    # hours: raw exists, finished, no hourly file yet
    hours = []
    for raw_dir in sorted(fs.glob(f"{root}/prim_global/date=*/hour=*")):
        date, hour = raw_dir.split("/")[-2].split("=")[-1], int(raw_dir.split("/")[-1].split("=")[-1])
        end = pytz.utc.localize(datetime.strptime(date, "%Y-%m-%d")) + timedelta(hours=hour + 1)
        if (now() - end).total_seconds() > HOUR_DONE_AFTER and not fs_silver.exists(hour_paths(date, hour)[1]):
            hours.append((date, hour))
    # days: the lookahead hours are over, no daily file yet
    days = []
    dates = sorted({p.split("/")[-1].split("=")[-1] for p in fs_silver.glob(f"{silver_root}/silver_hourly/date=*")})
    for date in dates:
        ready = pytz.utc.localize(datetime.strptime(date, "%Y-%m-%d")) + timedelta(hours=24 + LOOKAHEAD_HOURS)
        if (now() - ready).total_seconds() > HOUR_DONE_AFTER and not fs_silver.exists(day_path(date)):
            days.append(date)
    return hours, days


def clean_hourly():
    # hourly files of a date are only read to build that date (and the day before, for the 3 hours after midnight)
    # so once both daily files exist and the date is older than 7 days, they can go: they can be rebuilt from raw or archives
    cutoff = (now() - timedelta(days=HOURLY_KEEP_DAYS)).date()
    removed = []
    for hourly_dir in sorted(fs_silver.glob(f"{silver_root}/silver_hourly/date=*")):
        date = datetime.strptime(hourly_dir.split("/")[-1].split("=")[-1], "%Y-%m-%d").date()
        day_before = date - timedelta(days=1)
        day_before_needs_it = fs_silver.exists(f"{silver_root}/silver_hourly/date={day_before}") or fs_silver.exists(day_path(day_before))
        if date < cutoff and fs_silver.exists(day_path(date)) and (fs_silver.exists(day_path(day_before)) or not day_before_needs_it):
            fs_silver.rm(hourly_dir, recursive=True)
            removed.append(str(date))
    if removed:
        print(f"{now().isoformat(timespec='seconds')} silver hourly files removed for {len(removed)} days: {removed[0]} to {removed[-1]}", flush=True)
    return removed


def run():
    con = connect()
    hours, _ = todo()
    print(f"{now().isoformat(timespec='seconds')} silver: {len(hours)} hours to do", flush=True)
    results = [silver_hour(con, date, hour) for date, hour in hours]
    # days after hours: a day needs its hourly files
    _, days = todo()
    print(f"{now().isoformat(timespec='seconds')} silver: {len(days)} days to do", flush=True)
    results += [silver_day(con, date) for date in days]
    clean_hourly()
    print(f"done: {results.count('ok')} ok, {results.count('failed')} failed, {results.count('skipped')} skipped", flush=True)
    return results


if __name__ == "__main__":
    if len(sys.argv) >= 4 and sys.argv[1] == "hour":
        silver_hour(connect(), sys.argv[2], int(sys.argv[3]))
    elif len(sys.argv) >= 3 and sys.argv[1] == "day":
        silver_day(connect(), sys.argv[2])
    else:
        run()
