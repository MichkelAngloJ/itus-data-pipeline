"""Automated data-quality checks.

Every check is plain SQL returning one number (the count of offending
rows/keys). A check passes when that number is <= its threshold.

Source checks run against staging (i.e. what the vendor sent, before
quarantine), so on this dataset many of them FAIL on purpose: each FAIL is a
real problem in the source files, and the same SQL will catch that class of
problem in future deliveries. The integrity checks at the bottom run on the
clean layer and must always pass.

    python checks.py            # run against itus_pipeline.db and print a table
"""
import sqlite3
from contextlib import closing
import sys
from datetime import datetime, timezone

from itus import config

LATEST = """
    latest AS (
        SELECT * FROM (
            SELECT s.*, ROW_NUMBER() OVER (PARTITION BY company_id, period
                                           ORDER BY received_at DESC) AS rn
            FROM stg_operations s
            WHERE company_id IS NOT NULL AND period IS NOT NULL
              AND source_file IN (SELECT file_name FROM stg_file WHERE file_period = content_periods)
        ) WHERE rn = 1
    ),
    lagged AS (
        SELECT company_id, period, capacity_units, production_units, employees,
               LAG(capacity_units)   OVER w AS prev_cap,
               LAG(production_units) OVER w AS prev_prod,
               LAG(employees)        OVER w AS prev_emp
        FROM latest WINDOW w AS (PARTITION BY company_id ORDER BY period)
    )"""

CHECKS = [
    # ---------------------------------------------------------------- completeness
    dict(name="required_fields_present", category="completeness", severity="error", threshold=0,
         description="vendor_id, period, capacity_units and production_units must all be populated",
         sql="""SELECT COUNT(*) FROM stg_operations
                WHERE vendor_id IS NULL OR period IS NULL
                   OR capacity_units IS NULL OR production_units IS NULL"""),
    dict(name="employees_present", category="completeness", severity="warn", threshold=0,
         description="employees should be populated (row is still loaded with NULL)",
         sql="SELECT COUNT(*) FROM stg_operations WHERE employees_raw IS NULL"),
    dict(name="isin_present", category="completeness", severity="warn", threshold=0,
         description="ISIN should be populated; without it we rely on vendor_id alone",
         sql="SELECT COUNT(*) FROM stg_operations WHERE isin IS NULL"),
    dict(name="active_company_has_row", category="completeness", severity="error", threshold=0,
         description="every company active in a delivered month has a row for it (merged companies "
                     "are only expected up to their end date)",
         sql="""WITH periods AS (SELECT DISTINCT period FROM stg_operations
                                 WHERE period IS NOT NULL
                                   AND source_file IN (SELECT file_name FROM stg_file
                                                       WHERE file_period = content_periods)),
                expected AS (SELECT d.company_id, p.period FROM dim_company d CROSS JOIN periods p
                             WHERE p.period || '-01' BETWEEN d.active_from AND d.active_to),
                got AS (SELECT DISTINCT company_id, period FROM stg_operations
                        WHERE company_id IS NOT NULL)
                SELECT COUNT(*) FROM expected e
                LEFT JOIN got g ON g.company_id = e.company_id AND g.period = e.period
                WHERE g.company_id IS NULL"""),
    dict(name="trusted_value_exists", category="completeness", severity="warn", threshold=0,
         description="company-months that were delivered but have no trusted value because every "
                     "copy was quarantined",
         sql="""SELECT COUNT(*) FROM (
                    SELECT company_id, period FROM stg_operations
                    WHERE company_id IS NOT NULL
                      AND source_file IN (SELECT file_name FROM stg_file
                                          WHERE file_period = content_periods)
                    GROUP BY company_id, period
                    HAVING SUM(status != 'quarantined') = 0)"""),

    # ---------------------------------------------------------------- uniqueness
    dict(name="no_exact_duplicate_rows", category="uniqueness", severity="warn", threshold=0,
         description="the same vendor/period row repeated with identical values inside one file",
         sql="""SELECT COALESCE(SUM(n - 1), 0) FROM (
                    SELECT COUNT(*) AS n FROM stg_operations
                    GROUP BY source_file, vendor_id, period, capacity_units, production_units,
                             IFNULL(employees, -1)
                    HAVING COUNT(*) > 1)"""),
    dict(name="no_conflicting_duplicates", category="uniqueness", severity="error", threshold=0,
         description="the same vendor/period appears more than once in a file with different values",
         sql="""SELECT COUNT(*) FROM (
                    SELECT source_file, vendor_id, period FROM stg_operations
                    GROUP BY source_file, vendor_id, period
                    HAVING COUNT(DISTINCT capacity_units || '|' || production_units || '|'
                                          || IFNULL(employees, '')) > 1)"""),
    dict(name="master_ids_unique", category="uniqueness", severity="error", threshold=0,
         description="entity_id, vendor_id and isin are each unique in company_master",
         sql="""SELECT (SELECT COUNT(*) - COUNT(DISTINCT entity_id) FROM raw_company_master)
                     + (SELECT COUNT(*) - COUNT(DISTINCT vendor_id) FROM raw_company_master)
                     + (SELECT COUNT(*) - COUNT(DISTINCT isin) FROM raw_company_master)"""),
    dict(name="master_names_unique", category="uniqueness", severity="warn", threshold=0,
         description="company names in company_master that belong to more than one entity "
                     "(name can't be used as a key)",
         sql="""SELECT COUNT(*) FROM (SELECT company_name FROM raw_company_master
                                      GROUP BY company_name HAVING COUNT(*) > 1)"""),

    # ---------------------------------------------------------------- validity
    dict(name="no_negative_metrics", category="validity", severity="error", threshold=0,
         description="capacity, production and employees are never negative",
         sql="""SELECT COUNT(*) FROM stg_operations
                WHERE capacity_units < 0 OR production_units < 0 OR employees < 0"""),
    dict(name="production_within_capacity", category="validity", severity="error", threshold=0,
         description="production_units can't exceed capacity_units",
         sql="SELECT COUNT(*) FROM stg_operations WHERE production_units > capacity_units"),
    dict(name="capacity_scale_stable", category="validity", severity="error", threshold=0,
         description=f"capacity doesn't move more than {config.CAPACITY_SCALE_RATIO:g}x month on month "
                     "(catches thousands/units mix-ups)",
         sql=f"""WITH {LATEST}
                SELECT COUNT(*) FROM lagged
                WHERE prev_cap > 0 AND capacity_units IS NOT NULL
                  AND (1.0 * capacity_units / prev_cap > {config.CAPACITY_SCALE_RATIO}
                       OR 1.0 * capacity_units / prev_cap < {1 / config.CAPACITY_SCALE_RATIO})"""),
    dict(name="capacity_step_changes", category="validity", severity="warn", threshold=0,
         description=f"capacity steps of {config.CAPACITY_STEP_PCT:.0%}+ (often real expansions, "
                     "but worth a human look)",
         sql=f"""WITH {LATEST}
                SELECT COUNT(*) FROM lagged
                WHERE prev_cap > 0 AND ABS(1.0 * capacity_units / prev_cap - 1) >= {config.CAPACITY_STEP_PCT}"""),
    dict(name="production_jump", category="validity", severity="warn", threshold=0,
         description=f"production moving more than {config.PRODUCTION_JUMP_PCT:.0%} month on month",
         sql=f"""WITH {LATEST}
                SELECT COUNT(*) FROM lagged
                WHERE prev_prod > 0 AND ABS(1.0 * production_units / prev_prod - 1) > {config.PRODUCTION_JUMP_PCT}"""),
    dict(name="employees_jump", category="validity", severity="warn", threshold=0,
         description=f"headcount moving more than {config.EMPLOYEES_JUMP_PCT:.0%} month on month",
         sql=f"""WITH {LATEST}
                SELECT COUNT(*) FROM lagged
                WHERE prev_emp > 0 AND ABS(1.0 * employees / prev_emp - 1) > {config.EMPLOYEES_JUMP_PCT}"""),
    dict(name="employees_integer_format", category="validity", severity="warn", threshold=0,
         description="employees sent as whole numbers, not '1600.0'",
         sql="SELECT COUNT(*) FROM stg_operations WHERE employees_raw LIKE '%.%'"),
    dict(name="period_format_valid", category="validity", severity="error", threshold=0,
         description="period is YYYY-MM with a real month",
         sql="""SELECT COUNT(*) FROM stg_operations
                WHERE period IS NULL AND issue_codes LIKE '%INVALID_PERIOD%'"""),
    dict(name="published_after_period_end", category="validity", severity="warn", threshold=0,
         description="published_at can't be before the reported month is over",
         sql="""SELECT COUNT(*) FROM stg_operations
                WHERE published_at IS NOT NULL AND period_start IS NOT NULL
                  AND published_at < date(period_start, '+1 month')"""),
    dict(name="published_before_receipt", category="validity", severity="warn", threshold=0,
         description="published_at can't be later than our received_at",
         sql="SELECT COUNT(*) FROM stg_operations WHERE published_at > received_at"),
    dict(name="no_future_periods", category="validity", severity="error", threshold=0,
         description="a file can't report a month that hasn't started when we receive it",
         sql="SELECT COUNT(*) FROM stg_operations WHERE period_start > substr(received_at, 1, 10)"),

    # ---------------------------------------------------------------- consistency
    dict(name="schema_matches_contract", category="consistency", severity="warn", threshold=0,
         description="files whose header differs from the expected column list",
         sql=f"""SELECT COUNT(*) FROM stg_file
                 WHERE raw_columns != '{str(config.OPS_COLUMNS).replace("'", '"')}'"""),
    dict(name="file_period_matches_content", category="consistency", severity="error", threshold=0,
         description="the period in the file name matches the periods inside the file",
         sql="SELECT COUNT(*) FROM stg_file WHERE content_periods IS NOT file_period"),
    dict(name="no_duplicate_file_content", category="consistency", severity="error", threshold=0,
         description="two different files carrying identical data rows (a re-sent file)",
         sql="""SELECT COUNT(*) FROM (SELECT content_hash FROM stg_file
                                      GROUP BY content_hash HAVING COUNT(*) > 1)"""),
    dict(name="name_matches_identity", category="consistency", severity="warn", threshold=0,
         description="feed company_name equals the company's known name for that month "
                     "(after applying name-change events)",
         sql="""SELECT COUNT(*) FROM stg_operations s
                WHERE s.company_id IS NOT NULL AND s.company_name IS NOT NULL
                  AND NOT EXISTS (SELECT 1 FROM company_identifier i
                                  WHERE i.company_id = s.company_id AND i.id_type = 'name'
                                    AND i.id_value = s.company_name
                                    AND s.period_start BETWEEN i.valid_from AND i.valid_to)"""),
    dict(name="feed_ids_in_master", category="consistency", severity="error", threshold=0,
         description="vendor_ids in the feed that are neither in company_master nor introduced by a "
                     "company event (master is stale)",
         sql="""SELECT COUNT(DISTINCT vendor_id) FROM stg_operations
                WHERE vendor_id IS NOT NULL
                  AND vendor_id NOT IN (SELECT vendor_id FROM raw_company_master)
                  AND vendor_id NOT IN (SELECT new_value FROM raw_company_events
                                        WHERE event_type = 'vendor_id_change')"""),
    dict(name="merger_successor_in_master", category="consistency", severity="warn", threshold=0,
         description="merger events pointing at an entity that isn't in company_master",
         sql="""SELECT COUNT(DISTINCT new_value) FROM raw_company_events
                WHERE event_type = 'merger'
                  AND new_value NOT IN (SELECT entity_id FROM raw_company_master)"""),
    dict(name="vendor_and_isin_agree", category="consistency", severity="error", threshold=0,
         description="vendor_id and ISIN on the same row point to the same company",
         sql="""SELECT COUNT(*) FROM stg_operations s
                JOIN company_identifier v ON v.id_type = 'vendor_id' AND v.id_value = s.vendor_id
                     AND s.period_start BETWEEN v.valid_from AND v.valid_to
                JOIN company_identifier i ON i.id_type = 'isin' AND i.id_value = s.isin
                     AND s.period_start BETWEEN i.valid_from AND i.valid_to
                WHERE v.company_id != i.company_id"""),
    dict(name="identifier_used_in_window", category="consistency", severity="warn", threshold=0,
         description="a known vendor_id used outside the dates it was valid (e.g. old id after a change)",
         sql="""SELECT COUNT(*) FROM stg_operations s
                WHERE s.vendor_id IN (SELECT id_value FROM company_identifier WHERE id_type = 'vendor_id')
                  AND NOT EXISTS (SELECT 1 FROM company_identifier i
                                  WHERE i.id_type = 'vendor_id' AND i.id_value = s.vendor_id
                                    AND s.period_start BETWEEN i.valid_from AND i.valid_to)"""),
    dict(name="no_activity_outside_lifetime", category="consistency", severity="error", threshold=0,
         description="no rows for a company before it exists or after it merged away",
         sql="""SELECT COUNT(*) FROM stg_operations s JOIN dim_company d USING (company_id)
                WHERE s.period_start NOT BETWEEN d.active_from AND d.active_to"""),
    dict(name="restatements", category="consistency", severity="info", threshold=None,
         description="company-months whose values changed between deliveries (kept as SCD2 versions)",
         sql="""SELECT COUNT(*) FROM (SELECT company_id, period FROM fact_operations
                                      GROUP BY company_id, period HAVING COUNT(*) > 1)"""),

    # ---------------------------------------------------------------- timeliness
    dict(name="no_missing_months", category="timeliness", severity="error", threshold=0,
         description="every month between the first and last file has data",
         sql="""WITH RECURSIVE months(m) AS (
                    SELECT MIN(file_period) || '-01' FROM stg_file
                    UNION ALL
                    SELECT date(m, '+1 month') FROM months
                    WHERE date(m, '+1 month') <= (SELECT MAX(file_period) || '-01' FROM stg_file))
                SELECT COUNT(*) FROM months
                WHERE substr(m, 1, 7) NOT IN (SELECT DISTINCT period FROM stg_operations
                                              WHERE period IS NOT NULL)"""),
    dict(name="first_delivery_on_time", category="timeliness", severity="warn", threshold=0,
         description=f"company-months first delivered more than {config.LATE_AFTER_DAYS} days after "
                     "month end",
         sql=f"""SELECT COUNT(*) FROM (
                    SELECT company_id, period, MIN(received_at) AS first_seen FROM stg_operations
                    WHERE company_id IS NOT NULL AND status != 'quarantined'
                    GROUP BY company_id, period)
                 WHERE substr(first_seen, 1, 10) > date(period || '-01', '+1 month', '+{config.LATE_AFTER_DAYS - 1} days')"""),
    dict(name="manifest_matches_files", category="timeliness", severity="error", threshold=0,
         description="every operations file on disk is in file_manifest and vice versa",
         sql="""SELECT (SELECT COUNT(*) FROM raw_file_log
                        WHERE file_name LIKE 'operations%'
                          AND file_name NOT IN (SELECT filename FROM raw_file_manifest))
                     + (SELECT COUNT(*) FROM raw_file_manifest
                        WHERE filename NOT IN (SELECT file_name FROM raw_file_log))"""),
    dict(name="reissued_files", category="timeliness", severity="info", threshold=None,
         description="files that re-deliver an older month (v2 / supplement)",
         sql="SELECT COUNT(*) FROM stg_file WHERE file_kind != 'regular'"),

    # ---------------------------------------------------------------- clean-layer integrity
    dict(name="one_current_version", category="integrity", severity="error", threshold=0,
         description="exactly one is_current row per company-month",
         sql="""SELECT COUNT(*) FROM (SELECT company_id, period FROM fact_operations
                                      GROUP BY company_id, period HAVING SUM(is_current) != 1)"""),
    dict(name="no_overlapping_versions", category="integrity", severity="error", threshold=0,
         description="SCD2 windows for a company-month never overlap",
         sql="""SELECT COUNT(*) FROM fact_operations a JOIN fact_operations b
                  ON a.company_id = b.company_id AND a.period = b.period AND a.fact_id < b.fact_id
                 AND a.valid_from < b.valid_to AND b.valid_from < a.valid_to"""),
    dict(name="no_gaps_between_versions", category="integrity", severity="error", threshold=0,
         description="each closed version ends exactly when the next one starts",
         sql="""SELECT COUNT(*) FROM fact_operations a
                LEFT JOIN fact_operations b ON b.company_id = a.company_id AND b.period = a.period
                     AND b.version = a.version + 1
                WHERE a.is_current = 0 AND (b.fact_id IS NULL OR b.valid_from != a.valid_to)"""),
    dict(name="fact_company_exists", category="integrity", severity="error", threshold=0,
         description="every fact row joins to dim_company",
         sql="""SELECT COUNT(*) FROM fact_operations
                WHERE company_id NOT IN (SELECT company_id FROM dim_company)"""),
    dict(name="no_quarantined_rows_in_fact", category="integrity", severity="error", threshold=0,
         description="nothing that was quarantined made it into the trusted table",
         sql="""SELECT COUNT(*) FROM fact_operations f JOIN quarantine q
                  ON q.source_file = f.source_file AND q.row_number = f.source_row_number"""),
    dict(name="as_of_matches_manifest", category="integrity", severity="error", threshold=0,
         description="as_of_date always equals the manifest receipt time of the source file",
         sql="""SELECT COUNT(*) FROM fact_operations f JOIN raw_file_manifest m
                  ON m.filename = f.source_file WHERE f.as_of_date != m.received_at"""),
]


def run_checks(conn):
    ran_at = datetime.now(timezone.utc).strftime("%Y-%m-%d %H:%M:%S")
    results = []
    for c in CHECKS:
        actual = conn.execute(c["sql"]).fetchone()[0]
        if c["threshold"] is None:
            status = "INFO"
        elif actual <= c["threshold"]:
            status = "PASS"
        else:
            status = "FAIL" if c["severity"] == "error" else "WARN"
        results.append(dict(c, actual=actual, status=status))

    with conn:
        conn.execute("DROP TABLE IF EXISTS dq_check_results")
        conn.execute(
            """CREATE TABLE dq_check_results (
                check_name TEXT PRIMARY KEY, category TEXT, severity TEXT, description TEXT,
                sql_text TEXT, threshold INTEGER, actual INTEGER, status TEXT, ran_at TEXT)"""
        )
        conn.executemany(
            "INSERT INTO dq_check_results VALUES (?,?,?,?,?,?,?,?,?)",
            [(r["name"], r["category"], r["severity"], r["description"], " ".join(r["sql"].split()),
              r["threshold"], r["actual"], r["status"], ran_at) for r in results],
        )
    return results


def print_results(results):
    print(f"{'check':34} {'category':13} {'thr':>4} {'actual':>7}  status")
    print("-" * 70)
    for r in results:
        thr = "-" if r["threshold"] is None else r["threshold"]
        print(f"{r['name']:34} {r['category']:13} {thr:>4} {r['actual']:>7}  {r['status']}")
    counts = {s: sum(r["status"] == s for r in results) for s in ("PASS", "WARN", "FAIL", "INFO")}
    print("-" * 70)
    print("  ".join(f"{k}={v}" for k, v in counts.items()))


if __name__ == "__main__":
    db = sys.argv[1] if len(sys.argv) > 1 else config.DB_PATH
    with closing(sqlite3.connect(db)) as conn:
        print_results(run_checks(conn))
