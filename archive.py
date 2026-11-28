"""
- once a day: hours older than 7 days -> one archive per hour (ADR-0006)
- archive = tar of the decompressed snapshots (.json) + their .meta.json + .error files, compressed with zstd (long window)
- output files bronze_archive/prim_global/date={date}/hour={hour}.tar.zst and
bronze_archive/prim_global/date={date}/hour={hour}.manifest.json
- manifest: one line per file in the archive: name, size, sha256 of the content
- check before delete: read the archive back, every file must match the manifest
- raw hour deleted only if the check passed AND ARCHIVE_DELETE_RAW=true (week 1: false, archive only)
- open an archive by hand: zstd -d --long=31 hour=15.tar.zst -c | tar -t
"""


from datetime import datetime, timedelta
import time
from dotenv import load_dotenv
import os
import pytz
from hashlib import sha256
import json
import fsspec
import gzip
import tarfile
import io
import zstandard


load_dotenv()

BRONZE_ROOT = os.getenv("BRONZE_ROOT")
fs, root = fsspec.core.url_to_fs(BRONZE_ROOT)

# archives next to bronze, not inside it: bronze readers glob prim_global/ and must never see .tar.zst
ARCHIVE_ROOT = os.getenv("ARCHIVE_ROOT", BRONZE_ROOT.rstrip("/") + "_archive")
fs_archive, archive_root = fsspec.core.url_to_fs(ARCHIVE_ROOT)

DELETE_RAW = os.getenv("ARCHIVE_DELETE_RAW", "false") == "true" # week 1: false

RAW_DAYS = 7   # hot window: raw kept 7 days
ZSTD_LEVEL = 15     # same as the test on Nov 18 (zstd --long=31 -15)
ZSTD_WINDOW_LOG = 31  # 2**31 = 2 GB window, like --long=31: sees one whole hour
ZSTD_THREADS = 2  # one per OCPU


def files_of_hour(hour_dir):
    # everything the poller wrote for this hour
    return sorted(fs.glob(f"{hour_dir}/*.json.gz") + fs.glob(f"{hour_dir}/*.meta.json") + fs.glob(f"{hour_dir}/*.error"))


def member(path):
    # name and content inside the archive: .json.gz goes in decompressed, the rest as it is
    name = path.split("/")[-1]
    with fs.open(path, "rb") as f:
        data = f.read()
    if name.endswith(".json.gz"):
        return name[:-len(".gz")], gzip.decompress(data)
    return name, data


def write_archive(archive_path, raw_files):
    manifest = []
    params = zstandard.ZstdCompressionParameters.from_level(ZSTD_LEVEL, window_log=ZSTD_WINDOW_LOG, enable_ldm=True, threads=ZSTD_THREADS)
    with fs_archive.transaction:
        with fs_archive.open(archive_path, "wb") as f:
            writer = zstandard.ZstdCompressor(compression_params=params).stream_writer(f, closefd=False)
            with tarfile.open(fileobj=writer, mode="w|") as tar:
                for path in raw_files:
                    name, data = member(path)
                    info = tarfile.TarInfo(name)
                    info.size = len(data)
                    # time from the filename (20261116T150201Z), so the archive does not depend on when the job ran
                    info.mtime = int(datetime.strptime(name.split(".")[0], "%Y%m%dT%H%M%SZ").replace(tzinfo=pytz.utc).timestamp())
                    tar.addfile(info, io.BytesIO(data))
                    manifest.append({"name": name, "size": len(data), "sha256": sha256(data).hexdigest()})
            writer.close()
    return manifest


def read_archive(archive_path):
    # what is really inside the archive, file by file
    found = []
    with fs_archive.open(archive_path, "rb") as f:
        reader = zstandard.ZstdDecompressor(max_window_size=2 ** ZSTD_WINDOW_LOG).stream_reader(f)
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            for info in tar:
                data = tar.extractfile(info).read()
                found.append({"name": info.name, "size": len(data), "sha256": sha256(data).hexdigest()})
    return found


def check(manifest, found, raw_files):
    # same files, same order, same content, and nothing from the raw hour missing
    if len(manifest) != len(raw_files):
        return f"manifest has {len(manifest)} files, raw hour has {len(raw_files)}"
    if found != manifest:
        wrong = [m["name"] for m, x in zip(manifest, found) if m != x]
        return f"archive does not match manifest: {len(found)} files read, {len(manifest)} expected, first differences {wrong[:3]}"
    return None


def archive_hour(hour_dir):
    start = time.time()
    date, hour = hour_dir.split("/")[-2], hour_dir.split("/")[-1]
    archive_dir = f"{archive_root}/prim_global/{date}"
    archive_path = f"{archive_dir}/{hour}.tar.zst"
    manifest_path = f"{archive_dir}/{hour}.manifest.json"
    fs_archive.makedirs(archive_dir, exist_ok=True)

    raw_files = files_of_hour(hour_dir)
    raw_bytes = sum(fs.size(p) for p in raw_files)
    status, error, deleted = "failed", None, False

    try:
        if fs_archive.exists(manifest_path) and not DELETE_RAW:
            # already archived on an earlier day, nothing to delete yet: skip, don't read it again every day
            print(f"archive {date}/{hour} already archived, raw kept (delete off)", flush=True)
            return "ok"
        if fs_archive.exists(manifest_path):
            # archived on an earlier day (deletion was off): check again before deleting
            with fs_archive.open(manifest_path, "r") as mf:
                manifest = json.load(mf)["files"]
        else:
            manifest = write_archive(archive_path, raw_files)
            with fs_archive.transaction:
                with fs_archive.open(manifest_path, "w") as mf:
                    print(json.dumps({"hour": f"{date}/{hour}", "created": datetime.now(pytz.utc).isoformat(timespec="seconds").replace("+00:00", "Z"),
                                      "zstd": {"level": ZSTD_LEVEL, "window_log": ZSTD_WINDOW_LOG}, "files": manifest}, indent=4), file=mf)

        error = check(manifest, read_archive(archive_path), raw_files)
        if error is None:
            status = "ok"
            if DELETE_RAW:
                fs.rm(hour_dir, recursive=True)
                deleted = True
                # last hour of the day gone: the date folder is empty, remove it too (ls stays honest)
                date_dir = hour_dir.rsplit("/", 1)[0]
                if not fs.ls(date_dir):
                    fs.rmdir(date_dir)
    except Exception as e:
        # keep the raw hour, log it, go on with the next hour
        error = f"{type(e).__name__}: {str(e)[:300]}"

    archive_bytes = fs_archive.size(archive_path) if fs_archive.exists(archive_path) else None
    ratio = f"{archive_bytes / raw_bytes:.1%}" if archive_bytes and raw_bytes else None
    print(f"{datetime.now(pytz.utc).isoformat(timespec='seconds').replace('+00:00', 'Z')} archive {date}/{hour} status={status} files={len(raw_files)} "
          f"raw={raw_bytes} archive={archive_bytes} ratio={ratio} deleted={deleted} error={error} total={time.time() - start:.1f}s", flush=True)
    return status


def run():
    # whole days only: a date is archived when it is older than RAW_DAYS
    cutoff = (datetime.now(pytz.utc) - timedelta(days=RAW_DAYS)).date()
    hour_dirs = sorted(fs.glob(f"{root}/prim_global/date=*/hour=*"))
    old = [h for h in hour_dirs if datetime.strptime(h.split("/")[-2].split("=")[-1], "%Y-%m-%d").date() < cutoff]
    print(f"{len(old)} hours older than {cutoff} to archive, delete raw = {DELETE_RAW}", flush=True)

    results = [archive_hour(h) for h in old]
    print(f"done: {results.count('ok')} ok, {results.count('failed')} failed", flush=True)
    return results


if __name__ == "__main__":
    run()
