# Runbook

One section per check in `config/checks.yaml`. An `error` opens an alert that stays open until the
check passes; a `warn` notifies the owning team. Alerts live in the `alerts` table and in Dagster
as failed asset checks.

First, for any alert: `acm run --root <lake>` once more. Most data problems are late data that
the next run absorbs; an alert that survives a second run is real.

## freshness (error): no data from a region

A continuous producer (inference usage, storage snapshots) has sent nothing from one region for
longer than `max_lag_hours`.

1. One region silent and the others fine: the region's collector. Page its owner (`contact` in
   `contracts/producers/<dataset>.yaml`).
2. Every region silent at once: our ingestion, not the producers. Check the landing bucket and
   `ingest_log`.
3. Do not close a month while a freshness alert is open for it without telling finance: the data
   will arrive and restate it. In the replay this is exactly what happened on 4 October.

## duplicates (warn)

More than `max_share` of the last 24 hours' events were duplicate copies. Billing is not affected
(duplicates are removed by event id, and heartbeats by `(vm_id, seq)`); the producer is retrying
or replaying more than it should. Tell its owner.

## quarantine (error): records failing their contract

`quarantine/<dataset>/*.jsonl` holds each record with its reason. Usually a producer shipped a
schema change without a new contract version. Revenue for those records is missing until fixed.

1. Read the reasons: `jq -r .reason quarantine/<dataset>/*.jsonl | sort | uniq -c`.
2. If the change is legitimate, add a version with `renames` to the producer contract, run
   `acm contracts --check --against origin/main`, and re-deliver the files.
3. Never edit the validator to accept a record the contract does not describe.

## token_plausibility (error): inference usage in the wrong unit

Median input tokens per request far below anything a model sees (the replay's unit bug reported
thousands). Revenue for those windows is wrong until the gateway re-sends them with a higher
revision, which the platform picks up automatically; the alert resolves when it does. If it has
not resolved in a day, ask the gateway team for the corrected revision.

## heartbeat_coverage, clock_skew, inferred_session_end (warn)

Signals about the node agent and control plane that do not change bills by themselves:

- `heartbeat_coverage`: sessions with under 80% of the expected heartbeats. Capacity planning's
  utilisation for those days is flagged (`low_coverage`) rather than hidden.
- `clock_skew`: hosts whose clocks are off by more than a minute. Billing uses the schedule clock;
  tell the fleet team.
- `inferred_session_end`: stop events that never arrived. Each such session is billed to one
  heartbeat after the last (docs/BILLING_RULES.md); tell the control-plane team.

## unpriced_usage (error)

GPU-seconds with no list price, usually a new GPU type or region before `config/pricing.yaml`
has it. Add the price (with its `from` date) and re-run; the affected months recompute.

## unknown_customers (error)

Usage from a customer id the CRM export does not have. Either the CRM export is stale or a
producer has a bad customer id. Do not bill until it is resolved.

## mart_contracts (error)

A mart no longer matches `contracts/marts/<name>.yaml` (a column missing or the wrong type,
nulls, duplicate grain). Finance and product read these; fix the model, not the contract, unless
the consumers agreed to the change.

## restatement (warn): a closed month changed

The message says by how much and why. Send finance the adjustment entries:

```sql
SELECT customer_id, component, amount, reason FROM revenue_ledger
WHERE month = '2026-09' AND entry_type = 'adjustment' ORDER BY abs(amount) DESC;
```

The alert stays open as a record; resolve it in the ticket once finance has acknowledged.

## reconciliation (error): an invoice we cannot explain

`recon_waterfall` explains each invoice step by step. A customer with more than $1 unexplained
means the billing system does something the waterfall does not model yet. Look at that customer's
components in `ref_invoices` against `mart_finance_revenue`, find the behaviour, and add it as a
step in `recon.py` with a test, rather than raising the threshold.

## Erasure requests

`acm erase --root <lake> --email <address> --request-id <ticket>` exits non-zero unless every copy
is gone and revenue is unchanged. It does not reach backups, exports already sent to finance, or
the billing system's own copy; list those in the ticket and forward the request.
