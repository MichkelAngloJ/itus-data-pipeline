# Relationships at scale

`relationships.csv` is a small graph: companies make products, products are sold
into end markets, end markets are affected by drivers. Each edge has a validity
window, a source and a confidence. Here's how I'd store it once it's millions of edges.

## Model: nodes + bitemporal edges

```
node(node_id PK, node_type, canonical_name, company_id NULL, created_at)
     -- node_type: company | product | end_market | driver
     -- company nodes point at dim_company.company_id

node_alias(node_id, alias, valid_from, valid_to, source)

edge(edge_id PK,
     subject_id  -> node, predicate, object_id -> node,
     valid_from, valid_to,          -- business time: when the relationship is true in the world
     recorded_from, recorded_to,    -- system time: when we believed it
     source, confidence, evidence_ref)
```

- **Ids, never names.** The sample already shows why. "Aster Motion Technologies Ltd makes
  Drive Assembly" ends 2025-06-30 and "Aster Mobility Technologies Ltd makes Drive Assembly"
  starts 2025-07-01. That's the same company (C007) and the same edge, just a rename. On ids it's
  one continuous fact. Names go in `node_alias`, using the same identity resolution as the main pipeline.
- **Two time axes.** `valid_*` is what the source says about the world. `recorded_*` is when
  we knew it. This is the same idea as `fact_operations`. "What did we think Nova made, as of
  2025-10-15?" has to be answerable even after later corrections.
- **Never update in place.** A changed confidence, a corrected end date, or a retraction closes
  `recorded_to` on the old row and inserts a new one. Conflicting sources are separate edges
  side by side, each with its own source and confidence. Merging them is a query-time choice, not a storage one.
- **Predicates are a controlled vocabulary** (`makes`, `sold_to`, `affected_by`, ...) with an
  allowed subject/object type per predicate. That's enforced by a lookup table and a DQ check, the
  same way the operations feed is checked.

## Storage and indexing

Millions of edges is comfortable in Postgres:

- `edge` partitioned by `predicate` (or hash on `subject_id` if one predicate dominates)
- indexes on `(subject_id, predicate)` and `(object_id, predicate)` for walking both directions
- a GiST index / exclusion constraint on `(subject_id, predicate, object_id, tstzrange(valid_from, valid_to))`
  for the current recorded rows, so the same edge can't have overlapping validity
- a view `edge_current` = `recorded_to = 'infinity'`

## Querying

Most questions are 2–4 hops with a time filter, e.g. *"which companies are exposed to
Cloud Capex as of 2024-06-30?"*:

```sql
WITH RECURSIVE walk(node_id, depth, path, min_conf) AS (
    SELECT n.node_id, 0, ARRAY[n.node_id], 1.0
    FROM node n WHERE n.canonical_name = 'Cloud Capex'
  UNION ALL
    SELECT e.subject_id, w.depth + 1, w.path || e.subject_id, LEAST(w.min_conf, e.confidence)
    FROM walk w
    JOIN edge e ON e.object_id = w.node_id
    WHERE :as_of BETWEEN e.valid_from AND COALESCE(e.valid_to, 'infinity')
      AND :known_at >= e.recorded_from AND :known_at < e.recorded_to
      AND w.depth < 4 AND NOT e.subject_id = ANY(w.path)
)
SELECT DISTINCT n.company_id, w.depth, w.min_conf
FROM walk w JOIN node n USING (node_id)
WHERE n.node_type = 'company';
```

Here that returns Aster Cooling Systems (Industrial Chiller → Data Centres → Cloud Capex), with
the weakest link's confidence (0.8).

If multi-hop traversals become the main workload (deep paths, centrality, "everything within
3 hops of X" across the whole graph), I'd keep Postgres as the system of record and project the
current snapshot into a graph engine (Neo4j, or DuckDB/networkx for batch analytics) on a schedule,
rather than make the graph DB the source of truth. For heavy repeated questions ("company → driver
exposure"), precompute a `company_driver_exposure(company_id, driver_id, path, confidence,
valid_from, valid_to)` table on each load. Researchers query that directly, and it's versioned like
everything else.
