# 5. DuckDB for the warehouse, Dagster for orchestration, Spark for rating at scale

Status: accepted

## Context

The simulated fleet is 236 VMs and 378k events over six weeks. A real GPU cloud is orders of
magnitude larger, and the rating step (every session expanded to minutes, netted against
commitments) grows with fleet size times time.

## Decision

- **DuckDB** holds bronze views, silver, gold and marts in one file. A run takes about two seconds.
  Models are plain SQL files, so they can be read, reviewed and traced (`acm lineage`).
- **Dagster** runs the same pipeline steps as assets, with every data-quality check as an asset
  check on the asset it guards, a landing-file sensor, a three-hourly schedule and a schedule at
  the period close (`dagster_defs.py`). The warehouse is a single writer, so the job runs in process.
- **Spark** runs the rating step (`spark_rating.py`) over silver exported as Parquet. It is the
  same rule in the DataFrame API, and `acm spark-rate` checks it equals DuckDB to the cent
  (72 customer-months, $0.00 difference).

## Consequences

- One machine is enough for this repository and for a long way past it; the boundary where it is
  not is known (rating) and already has a distributed implementation.
- What is not here: silver on Spark, an object-store lake, a multi-writer catalog (Iceberg or
  Delta), streaming ingestion. Each is a step this design allows, not one it has taken.
