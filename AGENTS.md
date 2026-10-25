# Working on this repository

## Fast feedback

- `pytest -q -m "not slow"` (about a minute): contracts, ingestion, metering cases worked out by
  hand, periods, quality, privacy, lineage, CLI.
- `pytest -q -m slow` (several minutes): a replay of the whole period, drills, Dagster
  materialisation and Spark rating (needs `pip install -e ".[spark]"` and a JVM) on small worlds.
- `ruff check src tests`, `acm contracts --check --against origin/main`.
- `acm replay` regenerates `reports/EVALUATION.md`, `reports/RECONCILIATION.md`, `reports/replay.json`,
  `reports/lineage.json` and `docs/LINEAGE.md`; `acm drill` regenerates `reports/DRILLS.md`;
  `acm spark-rate --root data/replay` regenerates `reports/spark_rating.json`. Numbers in the README
  come from these files; regenerate them, never edit them by hand.

## Invariants (each has a test)

- The pipeline never reads the simulator's truth. Only `replay.py`, `drills.py` and the harness
  commands in `cli.py` may import `acm.sim` (`tests/test_isolation.py`).
- Usage belongs to the month it happened in; heartbeat time comes from the schedule, never the
  host clock (`test_host_clocks_cannot_move_money`).
- A closed month's `closed_total` never changes; changes are adjustment entries with a reason.
  The ledger is never updated or deleted.
- Records that break their contract are quarantined with the reason, never coerced. Schema changes
  go into the contract as a new version with `renames`; never loosen the validator.
- Personal data (`pii` in producer contracts, CRM `contact_email`) never reaches a mart
  (`test_personal_data_never_reaches_a_mart`), and erasure must verify zero remaining copies.
- Rating on Spark equals rating in DuckDB to the cent (`tests/test_spark.py`). Change both or neither.
- Thresholds and business settings live in `config/` (pricing, checks, periods), not in code.

## Where things are

- `src/acm/sim/`: the simulated cloud, its faulty delivery and the billing system's invoices. The
  harness, not the product.
- `src/acm/models/*.sql`: silver and gold, plain SQL run by `transform.py`.
- `pipeline.py`: one run as steps, used by the CLI, the replay and the Dagster assets.
- `docs/BILLING_RULES.md` is the specification; `docs/adr/` says why.
