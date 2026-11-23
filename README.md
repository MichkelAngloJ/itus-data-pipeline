# Itus Capital – Data Engineer take-home

A small three-layer pipeline (raw → staging → clean) that takes the monthly
company operations feeds and turns them into one SQLite database you can query
two ways: "what's our best number today" and "what did we know on date X".

Python standard library + SQLite only. Nothing to pip install.

```
python run_pipeline.py          # builds itus_pipeline.db and output/dq_report.md (~1s)
python queries.py               # worked examples (current view + as-of)
python checks.py                # re-run the data-quality checks on its own
python -m unittest discover -s tests -v
```

Needs Python 3.9+ and SQLite 3.28+ (for window functions; anything recent is fine).
The built `itus_pipeline.db` is committed too, so you can open it directly
(e.g. DB Browser for SQLite) without running anything.

The original brief is in [README_ASSIGNMENT.md](README_ASSIGNMENT.md); the source
files are untouched in [raw/](raw/).

---

## Layout

```
raw/                      source CSVs exactly as supplied
config/identity_overrides.csv   analyst-confirmed company mappings the vendor master is missing
itus/
  raw.py                  raw layer loader (hash-based, idempotent)
  identity.py             company identity: dim_company + dated identifiers
  staging.py              typing, normalisation, issue detection, quarantine
  clean.py                SCD2 fact table
  dq.py                   data_quality_log writer
  config.py               paths and thresholds
run_pipeline.py           entry point
checks.py                 SQL data-quality checks (name, description, SQL, threshold, result)
queries.py                current-view / as-of queries + examples
tests/test_pipeline.py    end-to-end tests
docs/relationships_design.md    the relationships exercise (1 page)
output/dq_report.md       generated report: check results, issue counts, row-level issues
```

## How it works

### Raw
Every CSV is loaded as-is into `raw_<file name>` (all TEXT, original headers, no
trimming or casting) plus `_source_file`, `_loaded_at`, `_row_number`.
`raw_file_log` keeps the sha256 of each file. On a rerun, a file whose hash hasn't
changed is skipped, so raw never gets duplicate rows and `_loaded_at` keeps the
time of the first load.

### Staging
`stg_operations` holds one typed row per source row:

- column names snake_cased, known renames mapped (`units_produced` → `production_units`)
- numbers parsed (`"1600.0"` is accepted as 1600, `"1600.5"` isn't), timestamps normalised,
  period checked as `YYYY-MM`
- each row resolved to a `company_id` (see below)
- every problem attached to the row as an issue with a severity

`error` issues send the row to `quarantine` (with reason codes and the original
raw record as JSON). `warn` issues load the row but log it. Everything goes to
`data_quality_log`. Rows are never dropped silently. Even the duplicate copy we
throw away is in quarantine as `DUPLICATE_EXACT`.

What counts as an error (quarantine) vs a warning (load + flag) is a judgement call.
My rule: quarantine when the number itself is untrustworthy (production > capacity,
conflicting duplicates, a file labelled with the wrong month, unresolvable company).
Load with a warning when the number is fine but something around it is off
(missing ISIN, name typo, odd published_at, missing headcount) or when a move is
unusual but plausible (capacity expansions, a one-month production dip).

### Company identity
`dim_company` has one row per legal entity. `company_identifier` holds every
vendor_id / ISIN / name a company has had, with `valid_from` / `valid_to`.

1. Start from `company_master`.
2. Apply `company_events`: a name change or vendor-id change closes the old identifier
   the day before the effective date and opens the new one; a merger marks the company
   `merged`, ends it the day before, and points at the successor.
3. Add `config/identity_overrides.csv`. This is where an analyst puts mappings the vendor
   master doesn't have yet. Right now it has one row: C201 Nova Industrial Components
   (V201 / INX000000201), the merger successor that isn't in `company_master`.

A feed row is resolved by vendor_id **and** ISIN as of the first day of its period. If
they disagree it's quarantined (`IDENTITY_CONFLICT`). If neither matches, it's quarantined
(`UNRESOLVED_ENTITY`). **Names are only used to check, never to match.** The master has two
different companies called "Aster Electronics Ltd" (C003 and C151), and C151 sometimes
arrives with no ISIN and a misspelled name. A name match would have quietly put C151's
numbers on C003.

### Clean (SCD Type 2)
`fact_operations` is keyed on `(company_id, period)` and versioned:

| column | meaning |
|---|---|
| `period` | the month the numbers describe (business time) |
| `valid_from` / `valid_to` | when we held this version: `[valid_from, valid_to)`; open end = `9999-12-31` |
| `is_current` | 1 for the latest version |
| `as_of_date` | `received_at` of the file that first brought this version (from `file_manifest`) |
| `version`, `change_type` | 1 = initial, 2+ = revision |
| `source_file`, `source_row_number`, `published_at` | lineage back to raw |
| `times_delivered` | how many files sent this exact version |

Files are replayed in **received order**, not filename order (the Sep-24 v2 file
arrived after the Nov-24 file). For each company-month:

- same values as the current version → no new row, `times_delivered` goes up
- different values → close the current row at the new file's `received_at`, insert a new one

`as_of_date` and `valid_from` are the same instant by construction. I kept both because the
brief asked for both and `as_of_date` reads better in research queries.

`v_operations_current` is a convenience view of current rows joined to `dim_company`,
with utilisation.

### Time convention for as-of queries
"As of D" = everything **we had received** by D, using `file_manifest.received_at`.

- A bare date means **end of that day**: `2025-01-03` → `2025-01-03 23:59:59`.
- The vendor's `published_at` is deliberately not used. Data published at 09:00 that lands
  at 14:51 wasn't usable to us at 10:00. One row even has a published_at a year too early,
  which is another reason not to trust it.
- Timestamps are taken as given (no time zone in the source; I assume they're all the same zone).

```sql
-- current best view
SELECT * FROM fact_operations WHERE company_id = :cid AND is_current = 1;

-- what we knew as of :ts
SELECT * FROM fact_operations
WHERE company_id = :cid
  AND as_of_date <= :ts
  AND valid_from <= :ts AND :ts < valid_to;
```

### Idempotency
- Raw: skip-by-hash, as above.
- Staging, identity, clean, DQ log: dropped and rebuilt from raw every run. All timestamps
  that go into versions come from the manifest, not the wall clock, so the output is
  identical run after run. `test_rerun_is_idempotent` checks this table by table.
- `pipeline_runs` is the only table that grows: one audit row per run.

At this size a full rebuild takes about a second and is the simplest thing that's obviously correct.
The scaling section covers what changes when it isn't.

---

## Example queries (real output)

`python queries.py` runs all of these. Trimmed:

**1. A restatement.** Helios Industrial Systems (C074), Sep-2024 production went from
31,119 to 31,529 in `operations_2024_09_v2.csv`, received 2025-01-03 10:15.

```
python queries.py history C074 2024-09
  ver  type      production  valid_from           valid_to             current  source_file
  1    initial   31119       2024-10-12 14:40:00  2025-01-03 10:15:00  0        operations_2024_09.csv
  2    revision  31529       2025-01-03 10:15:00  9999-12-31           1        operations_2024_09_v2.csv

python queries.py asof C074 2024-12-31   ->  2024-09  production 31119  (v1)
python queries.py asof C074 2025-01-03   ->  2024-09  production 31529  (v2)
python queries.py current C074           ->  2024-09  production 31529  (v2)
```

**2. Late data.** Keystone Cooling Systems (C106) was missing from the Feb-2025 file and
only arrived in `operations_2025_02_supplement.csv` on 2025-05-20.

```
as of 2025-04-30:  2025-01, 2025-03            (no Feb – we didn't have it yet)
as of 2025-05-31:  2025-01, 2025-02, 2025-03, 2025-04
                   2025-02 as_of_date = 2025-05-20 14:37:00, source = supplement
```

**3. Identity across a vendor-id change.** C009 moved from V009 to V909 on 2025-09-01.
`python queries.py current C009` gives one series: …2025-06, 2025-07, 2025-09, 2025-10…
(August is missing for every company, see problem 15).

**4. Merger.** C199 and C200 end at 2025-10 with `status = merged`, `successor_company_id = C201`;
C201 starts at 2025-11.

---

## Data-quality checks

[checks.py](checks.py) has 40 checks. Each one is a SQL statement that returns a count, with a
threshold, severity, category and description. Results go to `dq_check_results` and
[output/dq_report.md](output/dq_report.md).

- **Completeness**: required fields, missing employees/ISIN, active company with no row,
  company-months with no trusted value
- **Uniqueness**: exact and conflicting duplicates, master id/name uniqueness
- **Validity**: negatives, production ≤ capacity, capacity scale breaks, step changes, jumps,
  number format, period format, published_at sanity, future periods
- **Consistency**: schema contract, filename period vs content, duplicate file content, name vs
  identity history, feed ids missing from master, vendor/ISIN agreement, ids used outside their
  window, activity outside company lifetime, restatements
- **Timeliness**: missing months, late first delivery, manifest vs files, reissued files
- **Integrity** (clean layer, must always pass): one current version, no overlaps, no gaps,
  FK to dim, nothing quarantined leaked in, as_of matches manifest

The source checks run on **staging, before quarantine**, so on this data 8 FAIL and 14 WARN.
That's intended: each one is a real problem in the files, and the same SQL will catch the same
kind of problem in next month's file. `FutureFileTest` proves that with a made-up Jan-2026 file
(negative value, unknown company, blank production, month 13).

Current result: `PASS=16 WARN=14 FAIL=8 INFO=2`. All integrity checks pass, and the pipeline exits
non-zero if one ever doesn't.

---

## Problems I found in the source data

| # | Problem | Where | What the pipeline does | Check that catches it |
|---|---|---|---|---|
| 1 | Exact duplicate row | `operations_2024_04.csv` V037, rows 37 and 201 | keep one, the copy goes to quarantine (`DUPLICATE_EXACT`) | `no_exact_duplicate_rows` |
| 2 | Same key twice with different values | `operations_2024_06.csv` V044, rows 44 / 201: production 12,477 vs 13,202 | both quarantined. No way to tell which is right, so C044 has no Jun-24 value | `no_conflicting_duplicates`, `trusted_value_exists` |
| 3 | Company missing from a monthly file | `operations_2024_08.csv` has no V086 | logged (`MISSING_COMPANY_PERIOD`) | `active_company_has_row` |
| 4 | Restated value, delivered out of order | `operations_2024_09_v2.csv` (received 2025-01-03, after the Nov file): V074 production 31,119 → 31,529. The other 199 rows are identical apart from published_at | new SCD2 version for C074. Unchanged rows don't create versions | `restatements`, `reissued_files` |
| 5 | Production 7× capacity | `operations_2024_10.csv` V122: 114,160 vs capacity 16,000 (Sep 11,292, Nov 11,776, so probably an extra digit) | quarantined | `production_within_capacity` |
| 6 | Capacity unit error | V063, Jan–Mar 2025: capacity 25 instead of 25,000 | quarantined | `capacity_scale_stable`, `production_within_capacity` |
| 7 | Company left out of a file, sent 2 months later | V106 missing from `operations_2025_02.csv`, arrives in `operations_2025_02_supplement.csv` on 2025-05-20 | loaded with as_of 2025-05-20, so as-of queries before that correctly don't see it | `first_delivery_on_time` |
| 8 | Impossible published_at | `operations_2025_03.csv` V170: published 2024-04-12 for period 2025-03 | loaded (values look fine, and as-of uses received_at), flagged | `published_after_period_end` |
| 9 | Column renamed | `operations_2025_04.csv`: `production_units` → `units_produced` | mapped through `COLUMN_ALIASES`, logged as `SCHEMA_DRIFT` | `schema_matches_contract` |
| 10 | Headcount sent as decimals | `operations_2025_05.csv`: all employees like `1600.0` | cast to integer (only if it really is whole) | `employees_integer_format` |
| 11 | Missing headcount | `operations_2025_05.csv` V055 | loaded with employees NULL | `employees_present` |
| 12 | Missing ISIN | V151 in `operations_2025_06.csv` and `operations_2025_09.csv` | resolved on vendor_id | `isin_present` |
| 13 | Misspelled name | `operations_2025_09.csv` V151 "Aster Electronic Ltd" | resolved on vendor_id, flagged | `name_matches_identity` |
| 14 | Two companies with the same name in the master | C003 and C151 are both "Aster Electronics Ltd" | names are never used for matching. Combined with #12, a name match would have merged two companies | `master_names_unique` |
| 15 | File labelled August actually contains July; August never delivered | `operations_2025_08.csv` is row-for-row identical to `operations_2025_07.csv` (period 2025-07) | whole file quarantined (`FILE_PERIOD_MISMATCH`). Aug-2025 logged as a missing month | `file_period_matches_content`, `no_duplicate_file_content`, `no_missing_months` |
| 16 | Vendor id changed | V009 → V909 from 2025-09 (event E002) | handled by dated identifiers, one company C009 | `identifier_used_in_window`, `vendor_and_isin_agree` |
| 17 | Name changed | C007 Aster Motion → Aster Mobility from 2025-07 (E001) | handled by dated identifiers. The new name isn't flagged after the change date | `name_matches_identity` |
| 18 | Merger successor missing from master | E003/E004 point to C201. Feed has V201 / INX000000201 from 2025-11, master doesn't | added through `identity_overrides.csv` (logged). Without it those rows would be quarantined as `UNRESOLVED_ENTITY` | `feed_ids_in_master`, `merger_successor_in_master` |
| 19 | Capacity step-ups | V009 +10% Oct-24, V006 +25% Feb-25, V001 +25% Apr-25, V003 +28% Jul-25 | they persist afterwards, so they look like real expansions (V001's lines up with "makes Distribution Transformer from 2025-04-01" in relationships.csv). Loaded and flagged for review | `capacity_step_changes` |
| 20 | One-month production dip | C005 Jun-25 −37%, back the next month | loaded and flagged. Could be an outage, could be a typo, so it needs a human | `production_jump` |
| 21 | Headcount drop | C008 May-25 −20%, stays down | loaded and flagged (looks real) | `employees_jump` |
| 22 | Reference files have no receipt date | `company_master`, `company_events`, `relationships` aren't in the manifest | treated as current reference data. We can't time-travel the master itself (see below) | — (design gap, noted) |

A few things I checked and found fine: no negative values, no future periods, no published_at
after receipt, vendor_id and ISIN never point at different companies, nothing for C199/C200
after the merger, and manifest and files on disk match exactly.

---

## Design decisions worth defending

- **`fact_operations`, not `fact_financials`.** The feeds are capacity / production / headcount,
  not financials, so I named the table after what's in it.
- **Quarantine both sides of a conflicting duplicate** instead of "last row wins". Picking one is
  a coin flip that ends up in the trusted layer looking like fact. Leaving a gap is honest, and it
  shows up in `trusted_value_exists`.
- **Quarantine the whole mislabelled August file**, even though its values match July. If a
  mislabelled file ever carries different numbers, loading it would quietly "restate" July.
- **Trailing comparisons only use earlier periods**, so the warnings come out the same whether
  you run the full history or one new file.
- **Reissues are new information, not corrections to the past.** The v2 file gets its own
  as_of. We don't backdate it to the original file's date. That's the whole point of point-in-time.
- **An absent row is not a deletion.** If a company disappears from a later file, its earlier
  versions stay current. A vendor that wants to retract a value would need an explicit signal.

## Scaling to 20 years and thousands of companies

Rough size: 5,000 companies × 240 months × a few versions is about 5M fact rows. The volume is
fine. What has to change is how the work is done.

- **Database**: Postgres (or DuckDB over Parquet for research). Partition `fact_operations` by
  period (year). In Postgres I'd model the knowledge window as a `tstzrange` with an exclusion
  constraint, so overlapping versions are impossible at the database level instead of only being checked.
- **Incremental, not rebuild**: process only new manifest entries. For each new file, stage it,
  then do a set-based SCD2 merge just for the `(company_id, period)` keys it touches (compare a
  value hash, close and insert in one transaction). Keep the full rebuild as a disaster-recovery
  path, since it's deterministic.
- **Raw in object storage**: immutable files under `raw/<feed>/<received_date>/`, with the manifest
  as a table. The warehouse only keeps a pointer and hash.
- **Long format for metrics** once there are more than a handful: `(company_id, period, metric,
  value, valid_from, valid_to)` so a new metric doesn't need a schema change and revisions are per metric.
- **Identity becomes its own service**: the master gets versioned too (receipt date on every
  master/events delivery). Unmatched identifiers go to a review queue with fuzzy-match suggestions
  instead of a hand-edited CSV.
- **Checks scoped to the new file** plus a trailing window, not full-table scans. Keep thresholds
  in config per feed, store results over time to spot trends (e.g. completeness slipping).
- **Orchestration**: one Airflow/Dagster run per received file, with DQ as a gate. Integrity failures
  stop the publish. Source failures quarantine and alert but don't block other companies.

## Relationships

See [docs/relationships_design.md](docs/relationships_design.md).

## What I'd do with more time

- Version `company_master` and `company_events` by receipt date so identity is point-in-time too.
- Store an explicit "retracted" signal and a delete path in SCD2.
- A small review UI over `quarantine` + `data_quality_log` so an analyst can release or fix
  rows, with the decision itself versioned.
- Property-based tests for the SCD2 merge (random delivery orders → same result).
