# 2. Rebuild silver every run; recompute only the months new data touched

Status: accepted

## Context

Incremental models (merge new rows into silver) are where metering pipelines usually go wrong:
a late stop event changes a session that was already merged, a corrected revision must replace an
older one, a renewal changes every minute of an overlap. Each needs its own merge logic and its
own bug.

## Decision

Silver is rebuilt from bronze on every run. At this volume (hundreds of thousands of events a
month) that is about a second in DuckDB. Gold, which expands sessions to minutes, is recomputed
only for months touched by newly ingested events, plus the open month, plus every month when the
contract register or CRM changed (`pipeline.ingest`). Bronze files are compacted per event date
once a day has settled, so the rebuild reads a few hundred files rather than thousands.

## Consequences

- One code path for first runs, re-runs and late data. The replay runs 97 times over the period
  and ends equal to the truth.
- Cost grows with history. When silver no longer rebuilds in minutes, the first step is to rebuild
  only the open and recently touched months (bronze is already partitioned for it), and the
  rating step moves to Spark (ADR 5).
