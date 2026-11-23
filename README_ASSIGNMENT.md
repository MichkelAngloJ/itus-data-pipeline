# Itus Capital — Data Engineer Take-Home Assignment (V3.1)

## Context
You are given fictional recurring company-data feeds covering 2024–2025. Assume an investment research team depends on these datasets. Source data may contain errors, inconsistencies, revisions and changes typical of real-world feeds. A file parsing successfully does not mean its data is correct.

`file_manifest.csv` records when our system received each source file.

## Task
Build a production-style pipeline that ingests the supplied files and creates a clean, historically correct, queryable dataset.

It should let a researcher answer:
1. What is our current best view of the historical data for a company?
2. What information would have been available to us as of a specified past date?


## Requirements
- Use Python and SQL.
- Ingest and normalize the supplied raw files.
- Resolve company identity where necessary.
- Preserve revisions rather than silently overwriting history.
- Make reruns safe without creating duplicates.
- Detect, report and/or quarantine source data that should not enter the trusted layer.
- Add automated data-quality checks.
- Produce a clean queryable database/table structure.
- Demonstrate both historical queries with examples, and explain the time convention your as-of queries use.
- Briefly explain how you would extend the design to 20 years of history and thousands of companies.
- **For every source-data problem you identify, show the automated validation/check that would detect the same class of problem if it occurred again in a future file.**

PostgreSQL, SQLite, DuckDB or another local SQL database is acceptable.

## Relationship design exercise
`relationships.csv` is an early example connecting companies, products, end markets and external drivers, with relationships that can change through time.

You do not need to build this part. In no more than one page, explain how you would store, version and query it if it eventually grew to millions of relationships.

## Deliverables
- Source code
- README with clear run instructions
- Database/output produced by the pipeline
- Automated tests/data-quality checks
- Short design note
- A section titled **Problems I found in the source data**

We should be able to start from a clean checkout, follow your README and reproduce your output.

## Time
Please spend no more than **4–6 hours**. We do not expect a fully productionized platform.

AI tools are welcome. You should be prepared to explain and defend your design decisions and important code. An honest, incomplete submission with clear reasoning is better than a polished submission you cannot explain.
