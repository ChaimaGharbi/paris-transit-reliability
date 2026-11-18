"""Tests for archive.py on a fake bronze folder. Run: python -m unittest -v tests/test_archive.py"""
import gzip, io, json, os, sys, tarfile, tempfile, unittest, random
from datetime import datetime, timedelta, timezone
from pathlib import Path
from unittest import mock

tmp = tempfile.mkdtemp()
os.environ["BRONZE_ROOT"] = f"file://{tmp}/bronze"
os.environ["ARCHIVE_ROOT"] = f"file://{tmp}/bronze_archive"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import archive  # noqa: E402
import zstandard  # noqa: E402

archive.ZSTD_WINDOW_LOG = 22 # small window for tests (the VM uses 31)
archive.ZSTD_LEVEL = 3

BRONZE = Path(tmp) / "bronze" / "prim_global"
ARCH = Path(tmp) / "bronze_archive" / "prim_global"
OLD = (datetime.now(timezone.utc) - timedelta(days=10)).date()
NEW = (datetime.now(timezone.utc) - timedelta(days=2)).date()


def snapshot(i):
    rnd = random.Random(i)
    journeys = [{"ref": f"j{k}", "expected": f"18:{rnd.randint(0, 59):02d}"} for k in range(300)]
    return json.dumps({"Siri": {"journeys": journeys}}).encode()


def make_hour(date, hour, n=5, error=False):
    d = BRONZE / f"date={date}" / f"hour={hour:02d}"
    d.mkdir(parents=True, exist_ok=True)
    originals = {}
    for i in range(n):
        stamp = f"{date:%Y%m%d}T{hour:02d}{i * 2:02d}01Z"
        data = snapshot(hour * 100 + i)
        (d / f"{stamp}.json.gz").write_bytes(gzip.compress(data))
        meta = json.dumps({"status": "ok", "request_time": stamp}).encode()
        (d / f"{stamp}.meta.json").write_bytes(meta)
        originals[f"{stamp}.json"], originals[f"{stamp}.meta.json"] = data, meta
    if error:
        stamp = f"{date:%Y%m%d}T{hour:02d}5801Z"
        (d / f"{stamp}.error").write_bytes(b"<html>blocked</html>")
        (d / f"{stamp}.meta.json").write_bytes(b'{"status": "failed"}')
        originals[f"{stamp}.error"], originals[f"{stamp}.meta.json"] = b"<html>blocked</html>", b'{"status": "failed"}'
    return d, originals


def unpack(path):
    out = {}
    with open(path, "rb") as f:
        reader = zstandard.ZstdDecompressor(max_window_size=2 ** 31).stream_reader(f)
        with tarfile.open(fileobj=reader, mode="r|") as tar:
            for info in tar:
                out[info.name] = tar.extractfile(info).read()
    return out


class ArchiveTests(unittest.TestCase):
    def setUp(self):
        import shutil
        shutil.rmtree(Path(tmp) / "bronze", ignore_errors=True)
        shutil.rmtree(Path(tmp) / "bronze_archive", ignore_errors=True)
        self.d05, self.o05 = make_hour(OLD, 5)
        self.d15, self.o15 = make_hour(OLD, 15, error=True)
        self.dnew, _ = make_hour(NEW, 15)

    def test_week1_archive_only_raw_kept(self):
        with mock.patch.object(archive, "DELETE_RAW", False):
            results = archive.run()
        self.assertEqual(results, ["ok", "ok"])  # only the 2 old hours
        self.assertTrue(self.d05.exists() and self.d15.exists())   # raw kept
        self.assertEqual(unpack(ARCH / f"date={OLD}" / "hour=15.tar.zst"), self.o15) # exact content back, .error included
        manifest = json.loads((ARCH / f"date={OLD}" / "hour=15.manifest.json").read_text())
        self.assertEqual(len(manifest["files"]), len(self.o15))
        self.assertFalse((ARCH / f"date={NEW}").exists())    # recent hour untouched

    def test_delete_on_removes_raw_after_check(self):
        with mock.patch.object(archive, "DELETE_RAW", True):
            archive.run()
        self.assertFalse(self.d05.exists() or self.d15.exists())
        self.assertTrue(self.dnew.exists())
        self.assertEqual(unpack(ARCH / f"date={OLD}" / "hour=05.tar.zst"), self.o05)

    def test_second_run_skips_archived_hours(self):
        with mock.patch.object(archive, "DELETE_RAW", False):
            archive.run()
            before = (ARCH / f"date={OLD}" / "hour=05.tar.zst").stat().st_mtime_ns
            archive.run()
        self.assertEqual((ARCH / f"date={OLD}" / "hour=05.tar.zst").stat().st_mtime_ns, before)

    def test_turning_delete_on_later_checks_old_archives(self):
        with mock.patch.object(archive, "DELETE_RAW", False):
            archive.run()
        with mock.patch.object(archive, "DELETE_RAW", True):
            results = archive.run()
        self.assertEqual(results, ["ok", "ok"])
        self.assertFalse(self.d05.exists())

    def test_corrupted_archive_keeps_raw(self):
        with mock.patch.object(archive, "DELETE_RAW", False):
            archive.run()
        path = ARCH / f"date={OLD}" / "hour=05.tar.zst"
        data = bytearray(path.read_bytes()); data[len(data) // 2] ^= 0xFF; path.write_bytes(bytes(data))
        with mock.patch.object(archive, "DELETE_RAW", True):
            results = archive.run()
        self.assertEqual(results[0], "failed")
        self.assertTrue(self.d05.exists()) # raw kept
        self.assertFalse(self.d15.exists())  # the healthy hour still went through

    def test_missing_file_in_archive_keeps_raw(self):
        with mock.patch.object(archive, "DELETE_RAW", False):
            archive.run()
        # a new file appeared in the raw hour after archiving: counts differ, never delete
        (self.d05 / "20991231T000000Z.meta.json").write_text("{}")
        with mock.patch.object(archive, "DELETE_RAW", True):
            archive.run()
        self.assertTrue(self.d05.exists())


if __name__ == "__main__":
    unittest.main()
