"""
- One run -> One request to PRIM
- api key sent through apiKey header in the request
- we can compress download for faster download and then we need to save it gzipped so
I will look for the way to do this automatically
- output files bronze/prim_global/date={date}/hour={hour}/UTC.json.gz and
bronze/prim_global/date={date}/hour={hour}/UTC.meta.json
- use fsspec to write to the VM
- metadata contains: request time (UTC), HTTP status, duration, size in bytes,
sha256 of the payload, request URL and parameters
- every run writes a meta.json with status ok/failed, even when an exception happens
- max 2 retries, wait 5s then 10s (+ a bit of random), no new attempt after 90s, one attempt max 25s
- retry only on dropped connection, timeout, 5xx, 429
- max 230 retries a day, counter in a file per UTC day so it resets at midnight UTC like PRIM's quota
"""


from datetime import datetime
import time
from dotenv import load_dotenv
import os
import requests
from urllib.parse import urljoin
import pytz
from hashlib import sha256
import json
import fsspec
from tempfile import NamedTemporaryFile
from urllib3.exceptions import ProtocolError, ReadTimeoutError
import random


load_dotenv()


BASE_URL = os.getenv("BASE_URL", None)
BASE_PATH = os.getenv("BASE_PATH", None)
API_KEY = os.getenv("PRIM_API_KEY", None)

url = urljoin(BASE_URL, BASE_PATH+"/estimated-timetable")

BRONZE_ROOT = os.getenv("BRONZE_ROOT")
fs, root = fsspec.core.url_to_fs(BRONZE_ROOT)

params={"LineRef":"ALL"}

ERROR_BODY_LIMIT = 1_000_000  # max bytes kept from an error body

# retries (ADR-0004):
MAX_ATTEMPTS = 3
BACKOFF = [5, 10] # wait before attempt 2, before attempt 3
JITTER = 1 # up to 1s added at random to the wait
ATTEMPT_LIMIT = 25 # one attempt max 25s (headers + body)
DEADLINE = 90 # no new attempt after 90s from the start of the run
MIN_ATTEMPT = 5 # not worth starting an attempt with less than 5s left
RETRY_BUDGET = 230 # 1000 quota - 720 runs - margin


class AttemptTooLong(Exception):
    pass


# network or server failures: we record them instead of crashing
NETWORK_ERRORS = (requests.exceptions.ConnectionError, requests.exceptions.Timeout, ProtocolError, ReadTimeoutError, AttemptTooLong)


# retries counter, one file per UTC day
def load_retries_used(state_path):
    if not fs.exists(state_path):
        return 0
    with fs.open(state_path, "r") as st:
        return json.load(st)["retries_used"]


def save_retries_used(state_path, retries_used):
    with fs.transaction:
        with fs.open(state_path, "w") as st:
            print(json.dumps({"retries_used": retries_used}), file=st)



def run():
    start = time.time()
    utc = datetime.now(pytz.utc)
    zulu = utc.isoformat(timespec="seconds").replace("+00:00", "Z")
    zulu_sep_removed = "".join([i for i in zulu if i.isalnum()])
    date, hour = utc.date(), f"{utc.hour:02d}"

    #bronze_dir = os.path.join("bronze", "prim_global", f"date={date}", f"hour={hour}")
    bronze_dir = f"{root}/prim_global/date={date}/hour={hour}"
    fs.makedirs(bronze_dir, exist_ok=True)

    state_dir = f"{root}/_state"
    fs.makedirs(state_dir, exist_ok=True)
    state_path = f"{state_dir}/retries_{date}.json"
    retries_used = load_retries_used(state_path)

    # everything the meta needs exists before the request, so the meta can always be written
    r, end, js = None, None, None
    sha, start_stream, end_stream, size_in_bytes = None, None, None, None
    status, error_type, error_message = "failed", None, None
    attempts, stopped_because, attempt_start = 0, None, start


    for attempt in range(1, MAX_ATTEMPTS + 1):
        attempt_limit = ATTEMPT_LIMIT

        if attempt > 1:
            if retries_used >= RETRY_BUDGET:
                stopped_because = "retry budget used up"
                break

            # wait more each time + a bit of random
            wait = BACKOFF[attempt - 2] + random.uniform(0, JITTER)
            # 429: if the server says how long to wait, we wait at least that
            if r is not None and r.status_code == 429 and str(r.headers.get("Retry-After", "")).isdigit():
                wait = max(wait, int(r.headers.get("Retry-After")))

            elapsed = time.time() - start
            attempt_limit = min(ATTEMPT_LIMIT, DEADLINE - elapsed - wait)
            if attempt_limit < MIN_ATTEMPT:
                stopped_because = "deadline"
                break

            time.sleep(wait)
            retries_used += 1
            save_retries_used(state_path, retries_used)


        # fresh values for each attempt, otherwise attempt 1 leaks into attempt 2
        r, end, js = None, None, None
        sha, start_stream, end_stream, size_in_bytes = None, None, None, None
        error_type, error_message = None, None
        attempts = attempt
        attempt_start = time.time()
        retry = False

        try:
            r = requests.get(url, params=params , headers={"apiKey": API_KEY, "Accept": "application/json"}, stream=True, timeout=(5, 15))
            end = time.time()

            r.raw.decode_content = False

            if r.status_code == 200 and r.headers.get("Content-Encoding") == "gzip":
                #filename = os.path.join(bronze_dir, f"{zulu_sep_removed}.json.gz")
                filename = f"{bronze_dir}/{zulu_sep_removed}.json.gz"
                #tmpfile = NamedTemporaryFile('w+b', dir=bronze_dir).name

                with fs.transaction:
                    with fs.open(filename, 'wb') as f:
                        sha_running = sha256()  # own name: if the download drops, sha stays None, not a hash object
                        start_stream = time.time()
                        for data in r.raw.stream(decode_content=False):
                            # the read timeout only counts silence between chunks, so we check the total here
                            if time.time() - attempt_start > attempt_limit:
                                raise AttemptTooLong(f"still downloading after {attempt_limit:.0f}s")
                            f.write(data)
                            sha_running.update(data)
                        end_stream = time.time()
                        sha = sha_running.hexdigest()
                        f.flush()
                        #os.fsync(f.fileno())
                #os.replace(tmpfile, filename)
                size_in_bytes = fs.size(filename)  # after the transaction: the file is committed
                status = "ok"
            else:
                # keep the body for now, written only if the whole run fails
                js = r.raw.read(ERROR_BODY_LIMIT, decode_content = True)
                error_type, error_message = "HTTPStatus", f"status {r.status_code}, Content-Encoding {r.headers.get('Content-Encoding')}"
                retry = r.status_code == 429 or 500 <= r.status_code <= 599
        except NETWORK_ERRORS as e:
            # the transaction already threw away the half-written .json.gz
            error_type, error_message = type(e).__name__, str(e)[:500]
            retry = True

        if status == "ok" or not retry:
            break


    if status == "failed" and js:
        filename = f"{bronze_dir}/{zulu_sep_removed}.error"
        with fs.open(filename, 'wb') as er:
            er.write(js)

    #metadata_filename = os.path.join(bronze_dir, f"{zulu_sep_removed}.meta.json")
    metadata_filename = f"{bronze_dir}/{zulu_sep_removed}.meta.json"

    with fs.open(metadata_filename, 'w') as md:
        metadata = {
            "request_time" : str(utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")),
            "status": status,
            "status_code": r.status_code if r is not None else None,
            # last attempt
            "duration_time_to_headers": end-attempt_start if end is not None else None,
            "stream_duration": end_stream - start_stream if end_stream is not None and start_stream is not None else None,
            "request_url": url,
            "params": params,
            "sha256": sha,
            #"size_in_bytes": os.stat(filename).st_size
            "size_in_bytes": size_in_bytes,
            "content_type": r.headers.get("Content-Type") if r is not None else None,
            "ray_id": r.headers.get("cf-ray") if r is not None else None,
            "error_type": error_type,
            "error_message": error_message,
            "attempts": attempts,
            "stopped_because": stopped_because,
            "total_duration": time.time() - start,
            "retries_used_today": retries_used,
        }
        json_dumps_str = json.dumps(metadata, indent=4)
        print(json_dumps_str, file=md)

    # one line per run in poller.log, with the time so we know when things failed
    print(f"{zulu} status={status} attempts={attempts} http={metadata['status_code']} error={error_type} total={metadata['total_duration']:.1f}s retries_today={retries_used}", flush=True)



if __name__ == "__main__":
    run()
