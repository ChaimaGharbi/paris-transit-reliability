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


load_dotenv()


BASE_URL = os.getenv("BASE_URL", None)
BASE_PATH = os.getenv("BASE_PATH", None)
API_KEY = os.getenv("PRIM_API_KEY", None)

url = urljoin(BASE_URL, BASE_PATH+"/estimated-timetable")

BRONZE_ROOT = os.getenv("BRONZE_ROOT")
fs, root = fsspec.core.url_to_fs(BRONZE_ROOT)

params={"LineRef":"ALL"}
start = time.time()
utc = datetime.now(pytz.utc)
zulu = utc.isoformat(timespec="seconds").replace("+00:00", "Z")
zulu_sep_removed = "".join([i for i in zulu if i.isalnum()])
r = requests.get(url, params=params , headers={"apiKey": API_KEY, "Accept": "application/json"}, stream=True)
end = time.time()

r.encoding

r.raw.decode_content = False

date, hour = utc.date(), f"{utc.hour:02d}"




#bronze_dir = os.path.join("bronze", "prim_global", f"date={date}", f"hour={hour}")
bronze_dir = f"{root}/prim_global/date={date}/hour={hour}"

fs.makedirs(bronze_dir, exist_ok=True)


#filename = os.path.join(bronze_dir, f"{zulu_sep_removed}.json.gz")
filename = f"{bronze_dir}/{zulu_sep_removed}.json.gz"
#tmpfile = NamedTemporaryFile('w+b', dir=bronze_dir).name

#metadata_filename = os.path.join(bronze_dir, f"{zulu_sep_removed}.meta.json")
metadata_filename = f"{bronze_dir}/{zulu_sep_removed}.meta.json"
with fs.transaction:
    with fs.open(filename, 'wb') as f:
        sha = sha256()
        start_stream = time.time()
        for data in r.raw.stream(decode_content=False):
            f.write(data)
            sha.update(data)
        end_stream = time.time()
        sha = sha.hexdigest()
        f.flush()
        #os.fsync(f.fileno())
#os.replace(tmpfile, filename)
        
with fs.open(metadata_filename, 'w') as md:
    metadata = {
        "request_time" : str(utc.isoformat(timespec="milliseconds").replace("+00:00", "Z")),
        "status_code": r.status_code,
        "duration_time_to_headers": end-start,
        "stream_duration": end_stream - start_stream,
        "request_url": url,
        "params": params,
        "sha256": sha,
        #"size_in_bytes": os.stat(filename).st_size
        "size_in_bytes": fs.size(filename)
    }
    json_dumps_str = json.dumps(metadata, indent=4)
    print(json_dumps_str, file=md)
    
    
    
    

