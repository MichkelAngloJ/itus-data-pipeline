"""Staging layer: typed, normalised operations rows with every issue attached.

Rows are never dropped silently. Each row ends up in stg_operations with a
status (accepted / accepted_with_warnings / quarantined). Quarantined rows
are also copied to `quarantine` with reason codes, and every issue goes to
data_quality_log.
"""
import hashlib
import json
import re
from collections import defaultdict
from datetime import date, datetime, timedelta

from . import config

FILE_RE = re.compile(r"^operations_(\d{4})_(\d{2})(?:_(v\d+|supplement))?\.csv$")
PERIOD_RE = re.compile(r"^\d{4}-(0[1-9]|1[0-2])$")
METRICS = ("capacity_units", "production_units", "employees")


def snake(name):
    name = re.sub(r"([a-z0-9])([A-Z])", r"\1_\2", name.strip())
    return re.sub(r"[^0-9a-zA-Z]+", "_", name).strip("_").lower()


def _clean(v):
    v = (v or "").strip()
    return v or None


def _to_int(v):
    """-> (value, problem). '1600.0' is accepted as 1600; '1600.5' is not."""
    if v is None:
        return None, None
    try:
        f = float(v.replace(",", ""))
    except ValueError:
        return None, "NON_NUMERIC"
    if f != int(f):
        return None, "NON_INTEGER"
    return int(f), None


def _to_ts(v):
    if v is None:
        return None
    for fmt in ("%Y-%m-%d %H:%M:%S", "%Y-%m-%dT%H:%M:%S", "%Y-%m-%d"):
        try:
            return datetime.strptime(v, fmt).strftime("%Y-%m-%d %H:%M:%S")
        except ValueError:
            pass
    return None


def period_bounds(period):
    y, m = map(int, period.split("-"))
    start = date(y, m, 1)
    nxt = date(y + (m == 12), m % 12 + 1, 1)
    return start.isoformat(), (nxt - timedelta(days=1)).isoformat()


def _months(first, last):
    y, m = map(int, first.split("-"))
    out = []
    while f"{y:04d}-{m:02d}" <= last:
        out.append(f"{y:04d}-{m:02d}")
        y, m = (y + 1, 1) if m == 12 else (y, m + 1)
    return out


class Row:
    __slots__ = ("source_file", "row_number", "received_at", "raw", "vendor_id", "company_name",
                 "isin", "period", "period_start", "period_end", "capacity_units",
                 "production_units", "employees", "employees_raw", "published_at", "company_id",
                 "resolved_by", "issues")

    def __init__(self, **kw):
        self.issues = []
        for k, v in kw.items():
            setattr(self, k, v)

    def flag(self, code, severity, message, field=None, observed=None, quarantine=None):
        if quarantine is None:
            quarantine = severity == "error"
        self.issues.append(dict(code=code, severity=severity, message=message, field=field,
                                observed=observed, quarantine=quarantine))

    @property
    def quarantined(self):
        return any(i["quarantine"] for i in self.issues)

    @property
    def values(self):
        return (self.capacity_units, self.production_units, self.employees)


def build_staging(conn, resolver, companies, dq):
    manifest = dict(conn.execute("SELECT filename, received_at FROM raw_file_manifest").fetchall())
    files = conn.execute(
        "SELECT file_name, table_name, sha256, columns FROM raw_file_log"
        " WHERE file_name LIKE 'operations%' ORDER BY file_name"
    ).fetchall()

    # ---- manifest reconciliation -------------------------------------------------
    on_disk = {f[0] for f in files}
    for name in sorted(set(manifest) - on_disk):
        dq.add("MANIFEST_FILE_MISSING", "error", f"{name} is in file_manifest but not on disk",
               source_file=name, action="logged")
    usable = []
    for name, table, sha, cols in files:
        if name not in manifest:
            dq.add("FILE_NOT_IN_MANIFEST", "error",
                   f"{name} has no receipt date in file_manifest; not loaded past raw",
                   source_file=name, action="skipped")
            continue
        if not FILE_RE.match(name):
            dq.add("UNEXPECTED_FILE_NAME", "error", f"{name} does not match operations_YYYY_MM[_suffix].csv",
                   source_file=name, action="skipped")
            continue
        usable.append((manifest[name], name, table, json.loads(cols)))
    usable.sort()  # process in the order we received the files

    rows, file_rows = [], []
    seen_content = {}

    for received_at, name, table, raw_cols in usable:
        y, m, suffix = FILE_RE.match(name).groups()
        file_period, kind = f"{y}-{m}", suffix or "regular"

        # ---- schema -------------------------------------------------------------
        norm_cols = [config.COLUMN_ALIASES.get(snake(c), snake(c)) for c in raw_cols]
        renamed = [c for c in raw_cols if snake(c) in config.COLUMN_ALIASES]
        missing = [c for c in config.OPS_COLUMNS if c not in norm_cols]
        extra = [c for c in norm_cols if c not in config.OPS_COLUMNS]
        if renamed:
            dq.add("SCHEMA_DRIFT", "warn",
                   "column(s) renamed by vendor: " + ", ".join(
                       f"{c} -> {config.COLUMN_ALIASES[snake(c)]}" for c in renamed),
                   source_file=name, field=",".join(renamed), action="mapped")
        if extra:
            dq.add("SCHEMA_EXTRA_COLUMN", "warn", "unexpected column(s) ignored: " + ", ".join(extra),
                   source_file=name, action="ignored")
        if missing:
            dq.add("SCHEMA_MISSING_COLUMN", "error", "missing column(s): " + ", ".join(missing),
                   source_file=name, action="logged")

        raw_rows = conn.execute(f'SELECT * FROM "{table}" ORDER BY _row_number').fetchall()
        file_recs = []
        for r in raw_rows:
            raw = dict(zip(raw_cols, r[:len(raw_cols)]))
            rec = {nc: _clean(raw[c]) for c, nc in zip(raw_cols, norm_cols) if nc in config.OPS_COLUMNS}
            for c in missing:
                rec[c] = None
            row = Row(source_file=name, row_number=r[-1], received_at=received_at, raw=raw,
                      vendor_id=rec["vendor_id"], company_name=rec["company_name"], isin=rec["isin"],
                      period=None, period_start=None, period_end=None, employees_raw=rec["employees"],
                      published_at=None, company_id=None, resolved_by=None,
                      capacity_units=None, production_units=None, employees=None)
            _parse_row(row, rec, received_at)
            file_recs.append(row)

        # ---- file-level content checks -----------------------------------------
        periods = sorted({r.period for r in file_recs if r.period})
        content = "\n".join(sorted("|".join(str(v) for v in (r.raw.get(c) for c in raw_cols))
                                   for r in file_recs))
        content_hash = hashlib.sha256(content.encode()).hexdigest()

        if periods != [file_period]:
            msg = (f"file name says {file_period} but rows contain {', '.join(periods) or 'no valid period'};"
                   " whole file quarantined")
            dq.add("FILE_PERIOD_MISMATCH", "error", msg, source_file=name, period=file_period,
                   observed=",".join(periods))
            for row in file_recs:
                row.flag("FILE_PERIOD_MISMATCH", "error", msg, field="period", observed=row.period)
        if content_hash in seen_content:
            dq.add("DUPLICATE_FILE_CONTENT", "warn",
                   f"data rows are identical to {seen_content[content_hash]}",
                   source_file=name, action="logged")
        seen_content.setdefault(content_hash, name)

        float_emp = sum(1 for r in file_recs if r.employees_raw and "." in r.employees_raw)
        if float_emp:
            dq.add("EMPLOYEES_FLOAT_FORMAT", "warn",
                   f"{float_emp} employee counts sent as decimals (e.g. '1600.0'); cast to integer",
                   source_file=name, field="employees", action="cast")
        if kind != "regular":
            dq.add("REISSUE_FILE", "info",
                   f"{kind} file for {file_period} received {received_at}; treated as new information "
                   "from that timestamp", source_file=name, period=file_period)

        _flag_in_file_duplicates(file_recs)
        file_rows.append((name, kind, file_period, ",".join(periods), received_at,
                          json.dumps(raw_cols), json.dumps(norm_cols), len(file_recs), content_hash))
        rows.extend(file_recs)

    for row in rows:
        if row.vendor_id or row.isin:
            _resolve(row, resolver, companies)

    _trailing_checks(rows)
    _coverage_checks(rows, companies, [f[2] for f in file_rows], dq)

    for row in rows:
        for i in row.issues:
            action = ("quarantined" if i["quarantine"] else
                      "loaded_with_warning" if i["severity"] == "warn" else "logged")
            if i["quarantine"] and i["code"] == "DUPLICATE_EXACT":
                action = "dropped_duplicate"
            dq.add(i["code"], i["severity"], i["message"], source_file=row.source_file,
                   row_number=row.row_number, company_id=row.company_id, period=row.period,
                   field=i["field"], observed=i["observed"], action=action)

    _write(conn, rows, file_rows)
    return rows


def _parse_row(row, rec, received_at):
    for f in ("vendor_id", "period", "capacity_units", "production_units"):
        if rec[f] is None:
            row.flag("MISSING_REQUIRED", "error", f"{f} is empty", field=f)
    if rec["employees"] is None:
        row.flag("MISSING_EMPLOYEES", "warn", "employees is empty; loaded as NULL", field="employees")
    if rec["isin"] is None:
        row.flag("MISSING_ISIN", "warn", "isin is empty; resolved on vendor_id", field="isin")
    if rec["company_name"] is None:
        row.flag("MISSING_NAME", "warn", "company_name is empty", field="company_name")

    if rec["period"] is not None:
        if PERIOD_RE.match(rec["period"]):
            row.period = rec["period"]
            row.period_start, row.period_end = period_bounds(row.period)
        else:
            row.flag("INVALID_PERIOD", "error", "period is not YYYY-MM", field="period",
                     observed=rec["period"])

    for f in METRICS:
        val, problem = _to_int(rec[f])
        if problem:
            sev = "warn" if f == "employees" else "error"
            row.flag(problem, sev, f"{f} '{rec[f]}' is not a whole number", field=f, observed=rec[f])
        elif val is not None and val < 0:
            row.flag("NEGATIVE_VALUE", "error", f"{f} is negative", field=f, observed=val)
        setattr(row, f, val)

    if (row.capacity_units is not None and row.production_units is not None
            and row.production_units > row.capacity_units):
        row.flag("PRODUCTION_EXCEEDS_CAPACITY", "error",
                 f"production {row.production_units} > capacity {row.capacity_units}",
                 field="production_units", observed=row.production_units)

    row.published_at = _to_ts(rec["published_at"])
    if rec["published_at"] and row.published_at is None:
        row.flag("INVALID_TIMESTAMP", "warn", "published_at not parseable", field="published_at",
                 observed=rec["published_at"])
    if row.period:
        if row.period_start > received_at[:10]:
            row.flag("FUTURE_PERIOD", "error", f"period {row.period} starts after the file was received",
                     field="period", observed=row.period)
        if row.published_at and row.published_at[:10] <= row.period_end:
            row.flag("PUBLISHED_BEFORE_PERIOD_END", "warn",
                     f"published_at {row.published_at} is before {row.period} ended; as-of uses "
                     "received_at so the row is still usable", field="published_at",
                     observed=row.published_at)
    if row.published_at and row.published_at > received_at:
        row.flag("PUBLISHED_AFTER_RECEIPT", "warn", "published_at is later than received_at",
                 field="published_at", observed=row.published_at)


def _flag_in_file_duplicates(recs):
    groups = defaultdict(list)
    for r in recs:
        if r.vendor_id and r.period:
            groups[(r.vendor_id, r.period)].append(r)
    for (vid, period), grp in groups.items():
        if len(grp) < 2:
            continue
        if len({r.values for r in grp}) == 1:
            for r in grp[1:]:
                r.flag("DUPLICATE_EXACT", "warn",
                       f"exact copy of row {grp[0].row_number}; dropped", quarantine=True)
        else:
            rows_txt = ", ".join(str(r.row_number) for r in grp)
            for r in grp:
                r.flag("DUPLICATE_CONFLICT", "error",
                       f"{vid} {period} appears on rows {rows_txt} with different values; "
                       "can't tell which is right", observed=r.values)


def _resolve(row, resolver, companies):
    on = row.period_start or row.received_at[:10]
    by_vendor = resolver.resolve("vendor_id", row.vendor_id, on) if row.vendor_id else set()
    by_isin = resolver.resolve("isin", row.isin, on) if row.isin else set()

    if row.vendor_id and not by_vendor:
        if resolver.resolve_any_date("vendor_id", row.vendor_id):
            row.flag("IDENTIFIER_OUT_OF_WINDOW", "warn",
                     f"vendor_id {row.vendor_id} is not valid for {on}", field="vendor_id",
                     observed=row.vendor_id)
        else:
            row.flag("UNKNOWN_VENDOR_ID", "warn", f"vendor_id {row.vendor_id} not in master",
                     field="vendor_id", observed=row.vendor_id)
    if row.isin and not by_isin:
        row.flag("UNKNOWN_ISIN", "warn", f"isin {row.isin} not in master", field="isin", observed=row.isin)

    candidates = by_vendor | by_isin
    if len(candidates) > 1:
        row.flag("IDENTITY_CONFLICT", "error",
                 f"vendor_id points to {sorted(by_vendor)}, isin points to {sorted(by_isin)}")
        return
    if not candidates:
        row.flag("UNRESOLVED_ENTITY", "error", "no company matches vendor_id or isin")
        return

    cid = candidates.pop()
    row.company_id = cid
    row.resolved_by = "+".join(k for k, s in (("vendor_id", by_vendor), ("isin", by_isin)) if s)

    names = resolver.values_for(cid, "name", on)
    if row.company_name and row.company_name not in names:
        row.flag("NAME_MISMATCH", "warn",
                 f"name '{row.company_name}' doesn't match {cid}'s name for {on}: {sorted(names)}",
                 field="company_name", observed=row.company_name)
    c = companies[cid]
    if not (c["active_from"] <= on <= c["active_to"]):
        row.flag("OUTSIDE_COMPANY_LIFETIME", "error",
                 f"{cid} is only active {c['active_from']}..{c['active_to']}", field="period",
                 observed=row.period)


def _trailing_checks(rows):
    """Compare each row with the last accepted value of the previous period.

    Only uses data that was already known (earlier periods), so it works the
    same in an incremental run.
    """
    by_company = defaultdict(lambda: defaultdict(list))
    for r in rows:
        if r.company_id and r.period and not r.quarantined:
            by_company[r.company_id][r.period].append(r)

    for cid, periods in by_company.items():
        prev = None
        for period in sorted(periods):
            versions = sorted(periods[period], key=lambda r: r.received_at)
            for r in versions:
                if prev is None:
                    continue
                pc, pp, pe = prev.values
                if pc and r.capacity_units is not None:
                    ratio = r.capacity_units / pc
                    if ratio < 1 / config.CAPACITY_SCALE_RATIO or ratio > config.CAPACITY_SCALE_RATIO:
                        r.flag("CAPACITY_SCALE_BREAK", "error",
                               f"capacity {r.capacity_units} vs {pc} in {prev.period} "
                               f"({ratio:.4g}x) looks like a unit error", field="capacity_units",
                               observed=r.capacity_units)
                        continue
                    if abs(ratio - 1) >= config.CAPACITY_STEP_PCT:
                        r.flag("CAPACITY_STEP_CHANGE", "warn",
                               f"capacity moved {ratio - 1:+.0%} vs {prev.period}; review",
                               field="capacity_units", observed=r.capacity_units)
                if pp and r.production_units is not None:
                    chg = r.production_units / pp - 1
                    if abs(chg) > config.PRODUCTION_JUMP_PCT:
                        r.flag("PRODUCTION_JUMP", "warn", f"production moved {chg:+.0%} vs {prev.period}",
                               field="production_units", observed=r.production_units)
                if pe and r.employees is not None:
                    chg = r.employees / pe - 1
                    if abs(chg) > config.EMPLOYEES_JUMP_PCT:
                        r.flag("EMPLOYEES_JUMP", "warn", f"employees moved {chg:+.0%} vs {prev.period}",
                               field="employees", observed=r.employees)
            good = [r for r in versions if not r.quarantined]
            if good:
                prev = good[-1]


def _coverage_checks(rows, companies, file_periods, dq):
    if not file_periods:
        return
    delivered = {r.period for r in rows if r.period and not r.quarantined}
    calendar = _months(min(file_periods), max(file_periods))
    for p in calendar:
        if p not in delivered:
            dq.add("MISSING_PERIOD", "error", f"no usable file delivered data for {p}", period=p,
                   action="logged")

    have = defaultdict(list)
    for r in rows:
        if r.company_id and r.period:
            have[(r.company_id, r.period)].append(r)

    for p in calendar:
        if p not in delivered:
            continue
        start, end = period_bounds(p)
        for cid, c in companies.items():
            if not (c["active_from"] <= start <= c["active_to"]):
                continue
            got = have.get((cid, p), [])
            good = sorted((r for r in got if not r.quarantined), key=lambda r: r.received_at)
            if not got:
                dq.add("MISSING_COMPANY_PERIOD", "warn", f"{cid} has no row for {p} in any file",
                       company_id=cid, period=p, action="logged")
            elif not good:
                codes = sorted({i["code"] for r in got for i in r.issues if i["quarantine"]})
                dq.add("MISSING_COMPANY_PERIOD", "warn",
                       f"{cid} {p} only has quarantined rows ({', '.join(codes)}); no trusted value",
                       company_id=cid, period=p, action="logged")
            else:
                limit = (date.fromisoformat(end) + timedelta(days=config.LATE_AFTER_DAYS)).isoformat()
                if good[0].received_at[:10] > limit:
                    dq.add("LATE_ARRIVAL", "warn",
                           f"{cid} {p} first arrived {good[0].received_at} in {good[0].source_file}",
                           source_file=good[0].source_file, company_id=cid, period=p, action="logged")


def _write(conn, rows, file_rows):
    with conn:
        for t in ("stg_file", "stg_operations", "quarantine"):
            conn.execute(f"DROP TABLE IF EXISTS {t}")
        conn.execute(
            """CREATE TABLE stg_file (
                file_name       TEXT PRIMARY KEY,
                file_kind       TEXT NOT NULL,   -- regular | v2 | supplement
                file_period     TEXT NOT NULL,   -- period from the file name
                content_periods TEXT,            -- periods actually inside
                received_at     TEXT NOT NULL,
                raw_columns     TEXT NOT NULL,
                columns         TEXT NOT NULL,   -- normalised
                row_count       INTEGER NOT NULL,
                content_hash    TEXT NOT NULL
            )"""
        )
        conn.execute(
            """CREATE TABLE stg_operations (
                source_file      TEXT NOT NULL,
                row_number       INTEGER NOT NULL,
                received_at      TEXT NOT NULL,
                published_at     TEXT,
                vendor_id        TEXT,
                company_name     TEXT,
                isin             TEXT,
                period           TEXT,
                period_start     TEXT,
                capacity_units   INTEGER,
                production_units INTEGER,
                employees        INTEGER,
                employees_raw    TEXT,
                company_id       TEXT,
                resolved_by      TEXT,
                status           TEXT NOT NULL,
                issue_codes      TEXT,
                PRIMARY KEY (source_file, row_number)
            )"""
        )
        conn.execute(
            """CREATE TABLE quarantine (
                quarantine_id INTEGER PRIMARY KEY,
                source_file   TEXT NOT NULL,
                row_number    INTEGER NOT NULL,
                received_at   TEXT NOT NULL,
                vendor_id     TEXT,
                period        TEXT,
                company_id    TEXT,
                reason_codes  TEXT NOT NULL,
                reason_detail TEXT NOT NULL,
                raw_record    TEXT NOT NULL,
                UNIQUE (source_file, row_number)
            )"""
        )
        conn.executemany("INSERT INTO stg_file VALUES (?,?,?,?,?,?,?,?,?)", file_rows)
        stg, q = [], []
        for r in rows:
            codes = sorted({i["code"] for i in r.issues})
            status = ("quarantined" if r.quarantined else
                      "accepted_with_warnings" if any(i["severity"] == "warn" for i in r.issues)
                      else "accepted")
            stg.append((r.source_file, r.row_number, r.received_at, r.published_at, r.vendor_id,
                        r.company_name, r.isin, r.period, r.period_start, r.capacity_units,
                        r.production_units, r.employees, r.employees_raw, r.company_id,
                        r.resolved_by, status, ",".join(codes) or None))
            if r.quarantined:
                qi = [i for i in r.issues if i["quarantine"]]
                q.append((r.source_file, r.row_number, r.received_at, r.vendor_id, r.period,
                          r.company_id, ",".join(sorted({i["code"] for i in qi})),
                          "; ".join(i["message"] for i in qi), json.dumps(r.raw)))
        conn.executemany(f"INSERT INTO stg_operations VALUES ({','.join('?' * 17)})", stg)
        conn.executemany(
            "INSERT INTO quarantine (source_file, row_number, received_at, vendor_id, period,"
            " company_id, reason_codes, reason_detail, raw_record) VALUES (?,?,?,?,?,?,?,?,?)", q)
