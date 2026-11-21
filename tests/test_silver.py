"""Tests for silver.py on fake SIRI snapshots. Run: python -m unittest -v tests/test_silver.py"""
import gzip, json, os, sys, tempfile, unittest, shutil
from datetime import datetime, timedelta
from pathlib import Path
from unittest import mock

tmp = tempfile.mkdtemp()
os.environ["BRONZE_ROOT"] = f"file://{tmp}/bronze"
os.environ["SILVER_ROOT"] = f"file://{tmp}/silver"
sys.path.insert(0, str(Path(__file__).resolve().parents[1]))
import silver  # noqa: E402
import pytz  # noqa: E402

BRONZE = Path(tmp) / "bronze" / "prim_global"


def call(stop, aimed_dep=None, expected_dep=None, status="ON_TIME", aimed_arr=None, expected_arr=None):
    c = {"StopPointRef": {"value": stop}, "DepartureStatus": status}
    if aimed_dep: c["AimedDepartureTime"] = aimed_dep
    if expected_dep: c["ExpectedDepartureTime"] = expected_dep
    if aimed_arr: c["AimedArrivalTime"] = aimed_arr
    if expected_arr: c["ExpectedArrivalTime"] = expected_arr
    return c


def journey(ref, line, calls):
    return {"LineRef": {"value": line}, "DatedVehicleJourneyRef": {"value": ref}, "EstimatedCalls": {"EstimatedCall": calls}}


def snapshot(stamp, journeys):
    # same nesting as PRIM: 4 lists on the way to a stop call
    d = BRONZE / f"date={stamp[:4]}-{stamp[4:6]}-{stamp[6:8]}" / f"hour={stamp[9:11]}"
    d.mkdir(parents=True, exist_ok=True)
    doc = {"Siri": {"ServiceDelivery": {"EstimatedTimetableDelivery": [{"EstimatedJourneyVersionFrame": [{"EstimatedVehicleJourney": journeys}]}]}}}
    (d / f"{stamp}.json.gz").write_bytes(gzip.compress(json.dumps(doc).encode()))


def z(t):
    return f"2026-11-14T{t}:00.000Z" if len(t) == 5 else t


def build():
    shutil.rmtree(Path(tmp) / "bronze", ignore_errors=True)
    shutil.rmtree(Path(tmp) / "silver", ignore_errors=True)
    other = [
        journey("B", "L2", [call("S1", z("19:00"), z("19:00"), "CANCELLED")]),
        journey("C", "L3", [call("S1", z("19:05"), z("19:05"), "NO_REPORT")]),
        journey("D", "L4", [call("S9", aimed_arr=z("19:30"), expected_arr=z("19:31"))]),  # last stop: arrival only
        journey("E", "L5", [call(f"S{i}", z(f"10:0{i}"), z(f"18:3{i}")) for i in range(3)]),  # 8.5 h late everywhere
        journey("F", "L6", [call(f"S{i}", z(f"19:3{i}"), z(f"19:1{i}")) for i in range(3)]),  # 20 min early everywhere
    ]
    snapshot("20261114T180001Z", [journey("A", "L1", [call("S1", z("19:24"), z("19:24"))])] + other)
    snapshot("20261114T183001Z", [journey("A", "L1", [call("S1", z("19:24"), z("19:24"))])] + other)
    snapshot("20261114T185801Z", [journey("A", "L1", [call("S1", z("19:24"), "2026-11-14T19:25:09.000Z")])] + other)
    snapshot("20261114T191001Z", [journey("A", "L1", [call("S1", z("19:24"), z("19:27"))])])   # final state is in hour 19
    # around midnight
    snapshot("20261114T235001Z", [journey("G", "L7", [call("S1", z("23:55"), z("23:56")), call("S2", "2026-11-15T00:10:00.000Z", "2026-11-15T00:12:00.000Z")])])
    snapshot("20261115T000501Z", [journey("G", "L7", [call("S2", "2026-11-15T00:10:00.000Z", "2026-11-15T00:14:00.000Z")]),
                                  journey("H", "L8", [call("S1", z("23:58"), "2026-11-15T00:03:00.000Z")])])  # a Nov 14 call seen after midnight


class SilverTests(unittest.TestCase):
    def setUp(self):
        build()
        self.con = silver.connect()
        late = pytz.utc.localize(datetime(2026, 11, 16, 12, 0))
        self.p = mock.patch.object(silver, "now", lambda: late)
        self.p.start(); self.addCleanup(self.p.stop)

    def day(self, date):
        return {r[0]: r for r in self.con.sql(
            f"SELECT journey_ref || '/' || stop_ref, category, delay_s, last_seen FROM read_parquet('{silver.day_path(date)}')").fetchall()}

    def test_run_does_hours_then_days(self):
        results = silver.run()
        self.assertEqual(results.count("ok"), 4 + 2)        # 4 raw hours, 2 days
        self.assertEqual(silver.run(), [])                  # second run: nothing left to do

    def test_final_state_comes_from_the_next_hour(self):
        silver.run()
        a = self.day("2026-11-14")["A/S1"]
        self.assertEqual((a[1], a[2]), ("measured", 180))   # +3 min from 19:10, not +69 s from 18:58

    def test_rules(self):
        silver.run()
        d = self.day("2026-11-14")
        self.assertEqual(d["B/S1"][1:3], ("cancelled", None))
        self.assertEqual(d["C/S1"][1:3], ("no_report", None))
        self.assertEqual(d["D/S9"][1:3], ("no_aimed", None))
        self.assertEqual(d["E/S0"][1:3], ("implausible", None))
        self.assertEqual(d["F/S1"][1:3], ("implausible", None))

    def test_midnight(self):
        silver.run()
        d14, d15 = self.day("2026-11-14"), self.day("2026-11-15")
        self.assertIn("G/S1", d14)
        self.assertNotIn("G/S2", d14)                         # planned 00:10 Nov 15: belongs to Nov 15
        self.assertEqual(d15["G/S2"][2], 240)                 # its last state from 00:05
        self.assertEqual(d14["H/S1"][2], 300)                 # planned 23:58, seen only after midnight: still Nov 14

    def test_hour_not_finished_is_left_for_later(self):
        early = pytz.utc.localize(datetime(2026, 11, 14, 19, 5))   # hour 18 ended 5 min ago
        with mock.patch.object(silver, "now", lambda: early):
            hours, days = silver.todo()
        self.assertNotIn(("2026-11-14", 18), hours)
        self.assertEqual(days, [])


if __name__ == "__main__":
    unittest.main()
