"""Company identity: one company_id per legal entity, with dated identifiers.

company_master gives the starting point. company_events then closes/opens
identifier windows (name change, vendor id change) and ends companies that
merged. config/identity_overrides.csv holds analyst-confirmed mappings the
vendor master doesn't have yet (e.g. a new merger successor).

Names are stored for validation only. They are never used to resolve a row,
because the master itself has two different companies called
"Aster Electronics Ltd".
"""
import csv
from collections import defaultdict
from datetime import date, timedelta

from . import config

OPEN, BEGIN = config.OPEN_END, config.BEGIN


def _day_before(d):
    return (date.fromisoformat(d) - timedelta(days=1)).isoformat()


class Resolver:
    def __init__(self, identifiers, companies):
        self.companies = companies
        self._by_value = defaultdict(list)
        self._by_company = defaultdict(list)
        for i in identifiers:
            span = (i["company_id"], i["valid_from"], i["valid_to"])
            self._by_value[(i["id_type"], i["id_value"])].append(span)
            self._by_company[(i["company_id"], i["id_type"])].append(
                (i["id_value"], i["valid_from"], i["valid_to"]))

    def resolve(self, id_type, value, on_date):
        return {c for c, f, t in self._by_value.get((id_type, value), []) if f <= on_date <= t}

    def resolve_any_date(self, id_type, value):
        return {c for c, _, _ in self._by_value.get((id_type, value), [])}

    def values_for(self, company_id, id_type, on_date):
        return {v for v, f, t in self._by_company.get((company_id, id_type), []) if f <= on_date <= t}


def _read_overrides(path):
    if not path.exists():
        return []
    with open(path, newline="", encoding="utf-8") as fh:
        return list(csv.DictReader(fh))


def build_identity(conn, dq, overrides_path=None):
    overrides_path = overrides_path or (config.CONFIG_DIR / "identity_overrides.csv")
    master = conn.execute(
        "SELECT entity_id, vendor_id, company_name, isin, sector FROM raw_company_master ORDER BY entity_id"
    ).fetchall()
    events = conn.execute(
        "SELECT event_id, event_type, effective_date, entity_id, old_value, new_value, notes"
        " FROM raw_company_events ORDER BY effective_date, event_id"
    ).fetchall()

    companies, identifiers = {}, []

    def add_ident(cid, id_type, value, valid_from, source, valid_to=OPEN):
        if value:
            identifiers.append(dict(company_id=cid, id_type=id_type, id_value=value.strip(),
                                    valid_from=valid_from, valid_to=valid_to, source=source))

    for cid, vid, name, isin, sector in master:
        companies[cid] = dict(company_id=cid, sector=sector, status="active", active_from=BEGIN,
                              active_to=OPEN, successor_company_id=None, source="company_master")
        add_ident(cid, "vendor_id", vid, BEGIN, "company_master")
        add_ident(cid, "isin", isin, BEGIN, "company_master")
        add_ident(cid, "name", name, BEGIN, "company_master")

    # same name on more than one company -> name can't be a key
    by_name = defaultdict(list)
    for cid, _, name, _, _ in master:
        by_name[name].append(cid)
    for name, ids in by_name.items():
        if len(ids) > 1:
            dq.add("MASTER_DUPLICATE_NAME", "warn",
                   f"'{name}' is used by {', '.join(ids)}; rows are resolved on vendor_id/ISIN only",
                   source_file="company_master.csv", field="company_name", observed=name, action="logged")

    for o in _read_overrides(overrides_path):
        cid = o["company_id"]
        companies[cid] = dict(company_id=cid, sector=o["sector"] or None, status="active",
                              active_from=o["valid_from"], active_to=OPEN,
                              successor_company_id=None, source="identity_overrides")
        for t, col in (("vendor_id", "vendor_id"), ("isin", "isin"), ("name", "company_name")):
            add_ident(cid, t, o[col], o["valid_from"], "identity_overrides")
        dq.add("MASTER_MISSING_ENTITY", "warn",
               f"{cid} is not in company_master; added from config/identity_overrides.csv. {o['note']}",
               source_file="company_master.csv", company_id=cid, action="logged")

    for eid, etype, eff, cid, old, new, _notes in events:
        if cid not in companies:
            dq.add("EVENT_UNKNOWN_ENTITY", "error", f"{eid} references unknown entity {cid}",
                   source_file="company_events.csv", company_id=cid, action="ignored")
            continue
        if etype in ("name_change", "vendor_id_change"):
            id_type = "name" if etype == "name_change" else "vendor_id"
            match = [i for i in identifiers if i["company_id"] == cid and i["id_type"] == id_type
                     and i["valid_to"] == OPEN and i["id_value"] == old]
            if not match:
                dq.add("EVENT_OLD_VALUE_MISMATCH", "error",
                       f"{eid}: {cid} has no open {id_type} '{old}' to replace",
                       source_file="company_events.csv", company_id=cid, action="ignored")
                continue
            match[0]["valid_to"] = _day_before(eff)
            add_ident(cid, id_type, new, eff, f"company_events:{eid}")
        elif etype == "merger":
            successor = new
            if successor not in companies:
                companies[successor] = dict(company_id=successor, sector=None, status="active",
                                            active_from=eff, active_to=OPEN,
                                            successor_company_id=None, source=f"company_events:{eid}")
                dq.add("MASTER_MISSING_ENTITY", "warn",
                       f"merger successor {successor} has no identifiers; its feed rows will be "
                       "unresolved until it is added to identity_overrides.csv",
                       source_file="company_events.csv", company_id=successor, action="logged")
            companies[cid].update(status="merged", active_to=_day_before(eff),
                                  successor_company_id=successor)
        else:
            dq.add("EVENT_UNKNOWN_TYPE", "warn", f"{eid}: event type '{etype}' not handled",
                   source_file="company_events.csv", company_id=cid, action="ignored")

    _write(conn, companies, identifiers)
    return Resolver(identifiers, companies), companies


def _latest(identifiers, cid, id_type):
    vals = sorted((i["valid_from"], i["id_value"]) for i in identifiers
                  if i["company_id"] == cid and i["id_type"] == id_type)
    return vals[-1][1] if vals else None


def _write(conn, companies, identifiers):
    with conn:
        conn.execute("DROP TABLE IF EXISTS dim_company")
        conn.execute("DROP TABLE IF EXISTS company_identifier")
        conn.execute(
            """CREATE TABLE dim_company (
                company_id           TEXT PRIMARY KEY,
                company_name         TEXT,          -- latest known name
                vendor_id            TEXT,          -- latest known vendor id
                isin                 TEXT,
                sector               TEXT,
                status               TEXT NOT NULL, -- active | merged
                active_from          TEXT NOT NULL,
                active_to            TEXT NOT NULL,
                successor_company_id TEXT REFERENCES dim_company(company_id),
                source               TEXT NOT NULL
            )"""
        )
        conn.execute(
            """CREATE TABLE company_identifier (
                company_id TEXT NOT NULL REFERENCES dim_company(company_id),
                id_type    TEXT NOT NULL,   -- vendor_id | isin | name
                id_value   TEXT NOT NULL,
                valid_from TEXT NOT NULL,
                valid_to   TEXT NOT NULL,
                source     TEXT NOT NULL,
                PRIMARY KEY (company_id, id_type, id_value, valid_from)
            )"""
        )
        conn.executemany(
            "INSERT INTO dim_company VALUES (?,?,?,?,?,?,?,?,?,?)",
            [(c["company_id"], _latest(identifiers, cid, "name"), _latest(identifiers, cid, "vendor_id"),
              _latest(identifiers, cid, "isin"), c["sector"], c["status"], c["active_from"],
              c["active_to"], c["successor_company_id"], c["source"])
             for cid, c in sorted(companies.items())],
        )
        conn.executemany(
            "INSERT INTO company_identifier VALUES (?,?,?,?,?,?)",
            [(i["company_id"], i["id_type"], i["id_value"], i["valid_from"], i["valid_to"], i["source"])
             for i in identifiers],
        )
