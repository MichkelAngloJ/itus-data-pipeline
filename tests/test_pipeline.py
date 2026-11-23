"""End-to-end tests. Run with:  python -m unittest -v"""
import shutil
from contextlib import closing
import sqlite3
import sys
import tempfile
import unittest
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

import queries  # noqa: E402
from run_pipeline import run  # noqa: E402


def snapshot(conn):
    tables = ["dim_company", "company_identifier", "stg_operations", "quarantine",
              "fact_operations", "data_quality_log"]
    return {t: conn.execute(f"SELECT * FROM {t} ORDER BY 1, 2").fetchall() for t in tables}


class PipelineTest(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.tmp = Path(tempfile.mkdtemp())
        cls.db = cls.tmp / "test.db"
        cls.results = {r["name"]: r for r in run(cls.db, report_dir=None, verbose=False)}
        cls.conn = sqlite3.connect(cls.db)

    @classmethod
    def tearDownClass(cls):
        cls.conn.close()
        shutil.rmtree(cls.tmp, ignore_errors=True)

    def one(self, sql, *args):
        return self.conn.execute(sql, args).fetchone()[0]

    # ---- idempotency -------------------------------------------------------------
    def test_rerun_is_idempotent(self):
        before = snapshot(self.conn)
        raw_before = self.one("SELECT COUNT(*) FROM raw_operations_2024_01")
        run(self.db, report_dir=None, verbose=False)
        with closing(sqlite3.connect(self.db)) as c:
            self.assertEqual(snapshot(c), before)
            self.assertEqual(c.execute("SELECT COUNT(*) FROM raw_operations_2024_01").fetchone()[0], raw_before)
            # second run must not reload any raw file
            self.assertEqual(c.execute(
                "SELECT files_loaded FROM pipeline_runs ORDER BY run_id DESC LIMIT 1").fetchone()[0], 0)

    # ---- raw ---------------------------------------------------------------------
    def test_raw_is_untouched(self):
        # the float-formatted employees and the empty ISIN survive as-is in raw
        self.assertEqual(self.one("SELECT employees FROM raw_operations_2025_05 WHERE vendor_id='V001'"), "1600.0")
        self.assertEqual(self.one("SELECT isin FROM raw_operations_2025_06 WHERE vendor_id='V151'"), "")
        self.assertEqual(self.one("SELECT COUNT(*) FROM raw_operations_2024_04"), 201)

    # ---- SCD2 / time travel ------------------------------------------------------
    def test_revision_kept_and_point_in_time(self):
        hist = queries.version_history(self.conn, "C074", "2024-09")
        self.assertEqual([h[3] for h in hist], [31119, 31529])
        before = dict((r[0], r[2]) for r in queries.as_of_view(self.conn, "C074", "2025-01-02"))
        after = dict((r[0], r[2]) for r in queries.as_of_view(self.conn, "C074", "2025-01-03"))
        self.assertEqual(before["2024-09"], 31119)
        self.assertEqual(after["2024-09"], 31529)

    def test_nothing_visible_before_it_was_received(self):
        self.assertEqual(queries.as_of_view(self.conn, "C001", "2024-02-12 14:50:59"), [])
        self.assertEqual(len(queries.as_of_view(self.conn, "C001", "2024-02-12 14:51:00")), 1)

    def test_late_supplement(self):
        early = {r[0] for r in queries.as_of_view(self.conn, "C106", "2025-05-19")}
        late = {r[0] for r in queries.as_of_view(self.conn, "C106", "2025-05-21")}
        self.assertNotIn("2025-02", early)
        self.assertIn("2025-02", late)

    def test_identical_redelivery_creates_no_version(self):
        # the v2 file re-sent 199 unchanged rows; only C074 got a new version
        self.assertEqual(self.one("SELECT COUNT(*) FROM fact_operations WHERE period='2024-09' AND version > 1"), 1)
        self.assertEqual(self.one(
            "SELECT times_delivered FROM fact_operations WHERE company_id='C001' AND period='2024-09'"), 2)

    # ---- identity ----------------------------------------------------------------
    def test_vendor_id_change_keeps_one_company(self):
        periods = [r[0] for r in queries.current_view(self.conn, "C009")]
        self.assertIn("2025-07", periods)
        self.assertIn("2025-09", periods)  # delivered under V909
        self.assertEqual(self.one("SELECT COUNT(*) FROM stg_operations WHERE vendor_id='V909' AND company_id!='C009'"), 0)

    def test_name_change_and_null_isin_resolve(self):
        self.assertEqual(self.one(
            "SELECT COUNT(DISTINCT company_id) FROM stg_operations WHERE company_name LIKE 'Aster Mo%'"), 1)
        self.assertEqual(self.one(
            "SELECT company_id FROM stg_operations WHERE source_file='operations_2025_06.csv' AND vendor_id='V151'"),
            "C151")

    def test_duplicate_names_not_merged(self):
        self.assertEqual(self.one("SELECT COUNT(*) FROM fact_operations WHERE company_id='C003'"),
                         self.one("SELECT COUNT(*) FROM fact_operations WHERE company_id='C151'"))

    def test_merger(self):
        self.assertEqual(self.one("SELECT status FROM dim_company WHERE company_id='C199'"), "merged")
        self.assertEqual(self.one("SELECT MIN(period) FROM fact_operations WHERE company_id='C201'"), "2025-11")
        self.assertEqual(self.one("SELECT MAX(period) FROM fact_operations WHERE company_id IN ('C199','C200')"), "2025-10")

    # ---- quarantine --------------------------------------------------------------
    def test_bad_rows_kept_out_of_fact(self):
        for cid, period in [("C044", "2024-06"), ("C122", "2024-10"), ("C063", "2025-01"),
                            ("C063", "2025-02"), ("C063", "2025-03")]:
            self.assertEqual(self.one(
                "SELECT COUNT(*) FROM fact_operations WHERE company_id=? AND period=?", cid, period), 0,
                f"{cid} {period} should not be trusted")
        self.assertEqual(self.one("SELECT COUNT(*) FROM quarantine WHERE source_file='operations_2025_08.csv'"), 200)
        self.assertEqual(self.one("SELECT COUNT(*) FROM fact_operations WHERE source_file='operations_2025_08.csv'"), 0)

    def test_exact_duplicate_loaded_once(self):
        self.assertEqual(self.one(
            "SELECT COUNT(*) FROM fact_operations WHERE company_id='C037' AND period='2024-04'"), 1)

    def test_schema_drift_mapped(self):
        self.assertEqual(self.one(
            "SELECT production_units FROM fact_operations WHERE company_id='C001' AND period='2025-04'"), 12667)

    def test_missing_employees_loaded_as_null(self):
        self.assertIsNone(self.one(
            "SELECT employees FROM fact_operations WHERE company_id='C055' AND period='2025-05'"))

    # ---- checks ------------------------------------------------------------------
    def test_checks_catch_known_problems(self):
        expected = {
            "no_conflicting_duplicates": 1, "no_exact_duplicate_rows": 1,
            "production_within_capacity": 4, "capacity_scale_stable": 2,
            "file_period_matches_content": 1, "no_duplicate_file_content": 1,
            "no_missing_months": 1, "active_company_has_row": 1, "schema_matches_contract": 1,
            "isin_present": 2, "employees_present": 1, "name_matches_identity": 1,
            "feed_ids_in_master": 1, "merger_successor_in_master": 1, "master_names_unique": 1,
            "published_after_period_end": 1, "first_delivery_on_time": 1, "restatements": 1,
        }
        for name, n in expected.items():
            self.assertEqual(self.results[name]["actual"], n, name)

    def test_clean_layer_integrity(self):
        for r in self.results.values():
            if r["category"] == "integrity":
                self.assertEqual(r["status"], "PASS", r["name"])


class FutureFileTest(unittest.TestCase):
    """A made-up Jan-2026 delivery with fresh problems must be caught, not loaded."""

    def test_new_file_problems_are_caught(self):
        tmp = Path(tempfile.mkdtemp())
        try:
            raw = tmp / "raw"
            shutil.copytree(ROOT / "raw", raw)
            header = "vendor_id,company_name,isin,period,capacity_units,production_units,employees,published_at\n"
            rows = [
                "V001,Aster Power Systems Ltd,INX000000001,2026-01,14375,-5,1600,2026-02-12 09:00:00",
                "V002,Aster Auto Components Ltd,INX000000999,2026-01,49500,37000,abc,2026-02-12 09:00:00",
                "V999,Unknown Co Ltd,INX000009999,2026-01,1000,900,10,2026-02-12 09:00:00",
                "V004,Aster Industrial Systems Ltd,INX000000004,2026-01,26000,,531,2026-02-12 09:00:00",
                "V005,Aster Precision Engineering Ltd,INX000000005,2026-13,8500,5000,1141,2026-02-12 09:00:00",
            ]
            (raw / "operations_2026_01.csv").write_text(header + "\n".join(rows) + "\n")
            with open(raw / "file_manifest.csv", "a") as fh:
                fh.write("operations_2026_01.csv,2026-02-12 14:00:00\n")

            db = tmp / "t.db"
            res = {r["name"]: r for r in run(db, raw_dir=raw, report_dir=None, verbose=False)}
            with closing(sqlite3.connect(db)) as c:
                reasons = dict(c.execute(
                    "SELECT row_number, reason_codes FROM quarantine WHERE source_file='operations_2026_01.csv'"))
                fact = c.execute(
                    "SELECT COUNT(*) FROM fact_operations WHERE source_file='operations_2026_01.csv'").fetchone()[0]

            self.assertIn("NEGATIVE_VALUE", reasons[1])
            self.assertNotIn(2, reasons)            # unknown ISIN but vendor_id resolves -> loaded with warning
            self.assertIn("UNRESOLVED_ENTITY", reasons[3])
            self.assertIn("MISSING_REQUIRED", reasons[4])
            self.assertIn("INVALID_PERIOD", reasons[5])
            self.assertEqual(fact, 1)
            self.assertGreaterEqual(res["no_negative_metrics"]["actual"], 1)
            self.assertGreaterEqual(res["required_fields_present"]["actual"], 1)
            self.assertGreaterEqual(res["feed_ids_in_master"]["actual"], 2)
            self.assertGreaterEqual(res["active_company_has_row"]["actual"], 190)  # most companies missing
        finally:
            shutil.rmtree(tmp, ignore_errors=True)


if __name__ == "__main__":
    unittest.main()
