"""Clean layer: fact_operations as SCD Type 2 on (company_id, period).

Time convention
  period                 = the business month the numbers describe
  valid_from / valid_to  = when *we* knew this version: [valid_from, valid_to)
  as_of_date             = received_at of the file that first brought this version
                           (same instant as valid_from; kept as its own column
                           because it is what people filter on)

A re-delivery with identical values doesn't create a version; it only bumps
times_delivered. A changed value closes the old version at the new file's
received_at and opens a new one. Nothing is overwritten.
"""
import hashlib
from itertools import groupby

from . import config


def build_fact(conn, dq):
    rows = conn.execute(
        """SELECT company_id, period, capacity_units, production_units, employees,
                  source_file, row_number, published_at, received_at
           FROM stg_operations
           WHERE status != 'quarantined'
           ORDER BY company_id, period, received_at, source_file, row_number"""
    ).fetchall()

    versions = []
    for (cid, period), grp in groupby(rows, key=lambda r: (r[0], r[1])):
        current = None
        for (_, _, cap, prod, emp, src, rownum, pub, recv) in grp:
            vals = (cap, prod, emp)
            if current and current["vals"] == vals:
                current["times_delivered"] += 1
                continue
            if current:
                if current["valid_from"] == recv:
                    # two versions at the same instant would be a zero-length interval;
                    # staging should have caught this as a conflict
                    raise RuntimeError(f"two versions of {cid} {period} at {recv}")
                current["valid_to"] = recv
                current["is_current"] = 0
                changed = [f"{n}: {o} -> {v}" for n, o, v in
                           zip(("capacity", "production", "employees"), current["vals"], vals) if o != v]
                dq.add("REVISION", "info",
                       f"{cid} {period} revised by {src} ({'; '.join(changed)}); "
                       f"v{current['version']} closed at {recv}",
                       layer="clean", source_file=src, row_number=rownum, company_id=cid,
                       period=period, observed="; ".join(changed), action="new_version")
            version = (current["version"] + 1) if current else 1
            current = dict(company_id=cid, period=period, vals=vals, version=version,
                           change_type="initial" if version == 1 else "revision",
                           valid_from=recv, valid_to=config.OPEN_END, is_current=1,
                           as_of_date=recv, source_file=src, source_row_number=rownum,
                           published_at=pub, times_delivered=1)
            versions.append(current)

    with conn:
        conn.execute("DROP VIEW IF EXISTS v_operations_current")
        conn.execute("DROP TABLE IF EXISTS fact_operations")
        conn.execute(
            """CREATE TABLE fact_operations (
                fact_id           INTEGER PRIMARY KEY,
                company_id        TEXT NOT NULL REFERENCES dim_company(company_id),
                period            TEXT NOT NULL,
                capacity_units    INTEGER NOT NULL,
                production_units  INTEGER NOT NULL,
                employees         INTEGER,
                version           INTEGER NOT NULL,
                change_type       TEXT NOT NULL,     -- initial | revision
                valid_from        TEXT NOT NULL,
                valid_to          TEXT NOT NULL,
                is_current        INTEGER NOT NULL CHECK (is_current IN (0,1)),
                as_of_date        TEXT NOT NULL,
                source_file       TEXT NOT NULL,
                source_row_number INTEGER NOT NULL,
                published_at      TEXT,
                times_delivered   INTEGER NOT NULL,
                record_hash       TEXT NOT NULL,
                UNIQUE (company_id, period, version),
                UNIQUE (company_id, period, valid_from)
            )"""
        )
        conn.executemany(
            """INSERT INTO fact_operations (company_id, period, capacity_units, production_units,
                   employees, version, change_type, valid_from, valid_to, is_current, as_of_date,
                   source_file, source_row_number, published_at, times_delivered, record_hash)
               VALUES (?,?,?,?,?,?,?,?,?,?,?,?,?,?,?,?)""",
            [(v["company_id"], v["period"], *v["vals"], v["version"], v["change_type"],
              v["valid_from"], v["valid_to"], v["is_current"], v["as_of_date"], v["source_file"],
              v["source_row_number"], v["published_at"], v["times_delivered"],
              hashlib.sha256(repr((v["company_id"], v["period"], v["vals"])).encode()).hexdigest()[:16])
             for v in versions],
        )
        conn.execute("CREATE INDEX ix_fact_pit ON fact_operations (company_id, period, valid_from, valid_to)")
        conn.execute("CREATE INDEX ix_fact_current ON fact_operations (is_current, company_id)")
        conn.execute(
            """CREATE VIEW v_operations_current AS
               SELECT f.company_id, d.company_name, d.sector, f.period, f.capacity_units,
                      f.production_units, f.employees,
                      ROUND(1.0 * f.production_units / f.capacity_units, 4) AS utilisation,
                      f.version, f.as_of_date, f.source_file
               FROM fact_operations f JOIN dim_company d USING (company_id)
               WHERE f.is_current = 1"""
        )
    return len(versions)
