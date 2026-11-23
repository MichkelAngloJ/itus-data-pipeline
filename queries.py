"""Research queries against the clean layer.

    python queries.py                          # run the worked examples
    python queries.py current C074             # current best view
    python queries.py asof C074 2024-12-31     # what we knew at end of that day
    python queries.py history C074 2024-09     # every version of one month
    python queries.py find "Aster Electronics" # look a company up by name / vendor id / ISIN

As-of convention
----------------
"As of D" means: everything whose file had been *received* (file_manifest
received_at) at or before D. A bare date means end of that day, so
"as of 2025-01-03" includes a file that landed at 10:15 that morning.
The vendor's published_at is not used, because data isn't usable to us
until it's in our system. Windows are half-open: a version is visible when
valid_from <= D < valid_to.
"""
import sqlite3
from contextlib import closing
import sys

from itus import config

CURRENT_SQL = """
SELECT f.period, f.capacity_units, f.production_units, f.employees,
       f.version, f.as_of_date, f.source_file
FROM fact_operations f
WHERE f.company_id = :company_id AND f.is_current = 1
ORDER BY f.period
"""

AS_OF_SQL = """
SELECT f.period, f.capacity_units, f.production_units, f.employees,
       f.version, f.as_of_date, f.source_file
FROM fact_operations f
WHERE f.company_id = :company_id
  AND f.as_of_date <= :as_of
  AND f.valid_from <= :as_of AND :as_of < f.valid_to
ORDER BY f.period
"""

HISTORY_SQL = """
SELECT f.version, f.change_type, f.capacity_units, f.production_units, f.employees,
       f.valid_from, f.valid_to, f.is_current, f.source_file, f.times_delivered
FROM fact_operations f
WHERE f.company_id = :company_id AND f.period = :period
ORDER BY f.version
"""

FIND_SQL = """
SELECT DISTINCT d.company_id, d.company_name, d.status, i.id_type, i.id_value, i.valid_from, i.valid_to
FROM company_identifier i JOIN dim_company d USING (company_id)
WHERE i.id_value LIKE :q OR d.company_id = :exact
ORDER BY d.company_id, i.id_type, i.valid_from
"""


def normalise_as_of(value):
    """'2024-06-15' -> '2024-06-15 23:59:59' (end of day); timestamps pass through."""
    return value + " 23:59:59" if len(value) == 10 else value


def current_view(conn, company_id):
    return conn.execute(CURRENT_SQL, {"company_id": company_id}).fetchall()


def as_of_view(conn, company_id, as_of):
    return conn.execute(AS_OF_SQL, {"company_id": company_id, "as_of": normalise_as_of(as_of)}).fetchall()


def version_history(conn, company_id, period):
    return conn.execute(HISTORY_SQL, {"company_id": company_id, "period": period}).fetchall()


def find_company(conn, text):
    return conn.execute(FIND_SQL, {"q": f"%{text}%", "exact": text}).fetchall()


def show(title, cols, rows, limit=None):
    print(f"\n{title}")
    if not rows:
        print("  (no rows)")
        return
    shown = rows if limit is None else rows[:limit]
    widths = [max(len(str(c)), *(len(str(r[i])) for r in shown)) for i, c in enumerate(cols)]
    print("  " + "  ".join(str(c).ljust(w) for c, w in zip(cols, widths)))
    for r in shown:
        print("  " + "  ".join(str(v).ljust(w) for v, w in zip(r, widths)))
    if limit is not None and len(rows) > limit:
        print(f"  ... {len(rows) - limit} more")


VIEW_COLS = ["period", "capacity", "production", "employees", "ver", "as_of_date", "source_file"]
HIST_COLS = ["ver", "type", "capacity", "production", "employees", "valid_from", "valid_to",
             "current", "source_file", "deliveries"]


def _between(rows, first, last):
    return [r for r in rows if first <= r[0] <= last]


def examples(conn):
    print("=" * 78)
    print("1) A restatement: Helios Industrial Systems (C074), Sep-2024 production")
    print("   operations_2024_09.csv said 31,119; operations_2024_09_v2.csv (received")
    print("   2025-01-03 10:15) says 31,529. Both versions are kept.")
    show("version history C074 2024-09", HIST_COLS, version_history(conn, "C074", "2024-09"))
    show("current best view, C074 Aug-Oct 2024", VIEW_COLS,
         _between(current_view(conn, "C074"), "2024-08", "2024-10"))
    show("as of 2024-12-31 (before the v2 file) -> 31119", VIEW_COLS,
         _between(as_of_view(conn, "C074", "2024-12-31"), "2024-08", "2024-10"))
    show("as of 2025-01-03 (v2 landed that morning) -> 31529", VIEW_COLS,
         _between(as_of_view(conn, "C074", "2025-01-03"), "2024-08", "2024-10"))

    print("\n" + "=" * 78)
    print("2) Late data: Keystone Cooling Systems (C106) was left out of the Feb-2025")
    print("   file and only arrived in operations_2025_02_supplement.csv on 2025-05-20.")
    show("as of 2025-04-30 -> no Feb row yet", VIEW_COLS,
         _between(as_of_view(conn, "C106", "2025-04-30"), "2025-01", "2025-04"))
    show("as of 2025-05-31 -> Feb row visible", VIEW_COLS,
         _between(as_of_view(conn, "C106", "2025-05-31"), "2025-01", "2025-04"))

    print("\n" + "=" * 78)
    print("3) Identity: Aster Process Equipment (C009) switched vendor id V009 -> V909 on")
    print("   2025-09-01. One company_id, one unbroken series (Aug-2025 is missing for")
    print("   everyone, see the problems list).")
    show("company_identifier for C009", ["company_id", "name", "status", "id_type", "id_value",
                                          "valid_from", "valid_to"], find_company(conn, "C009"))
    show("current view, C009 Jun-Dec 2025", VIEW_COLS,
         _between(current_view(conn, "C009"), "2025-06", "2025-12"))

    print("\n" + "=" * 78)
    print("4) Merger: C199 + C200 merged into C201 (Nova Industrial Components) on 2025-11-01.")
    rows = conn.execute(
        """SELECT d.company_id, d.company_name, d.status, d.active_to, d.successor_company_id,
                  MAX(f.period) AS last_period
           FROM dim_company d LEFT JOIN fact_operations f USING (company_id)
           WHERE d.company_id IN ('C199','C200','C201') GROUP BY d.company_id""").fetchall()
    show("dim_company", ["company_id", "name", "status", "active_to", "successor", "last_period"], rows)
    show("as of 2026-01-31, C201", VIEW_COLS, as_of_view(conn, "C201", "2026-01-31"))


def main(argv):
    with closing(sqlite3.connect(config.DB_PATH)) as conn:
        if not argv:
            examples(conn)
        elif argv[0] == "current":
            show(f"current view {argv[1]}", VIEW_COLS, current_view(conn, argv[1]))
        elif argv[0] == "asof":
            show(f"{argv[1]} as of {normalise_as_of(argv[2])}", VIEW_COLS, as_of_view(conn, argv[1], argv[2]))
        elif argv[0] == "history":
            show(f"{argv[1]} {argv[2]} versions", HIST_COLS, version_history(conn, argv[1], argv[2]))
        elif argv[0] == "find":
            show(f"matches for '{argv[1]}'", ["company_id", "name", "status", "id_type", "id_value",
                                              "valid_from", "valid_to"], find_company(conn, argv[1]))
        else:
            print(__doc__)


if __name__ == "__main__":
    main(sys.argv[1:])
