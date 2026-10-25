# 3. Closed months are an append-only ledger; changes are adjustments with a reason

Status: accepted

## Context

Finance reports a month once it is closed. Recomputing from scratch is right for the platform
and wrong for finance: the number they reported must still be retrievable, and a change must say
why it happened.

## Decision

- At the close time in `config/periods.yaml`, each customer's lines for the month are written to
  `revenue_ledger` as `close` entries.
- Every later run compares the recomputed lines with the ledger. A difference of half a cent or
  more becomes an `adjustment` entry whose reason names what arrived after the close ("events
  ingested after close: 362 gpu_heartbeat, 266 inference_usage, 208 storage_snapshot") or, if no
  events did, that reference data changed.
- `mart_finance_revenue` shows `closed_total`, `adjustments` and `total`; `revenue_as_of(month, t)`
  answers what was reported at any instant. The `restatement` check tells finance.

## Consequences

- In the replay, September closes at $449,724.84 and the outage data restates it by $44.46 the
  next day, with the reason, without touching the closed figure.
- Ledger entries are never updated or deleted, including by erasure (they hold no personal data).
