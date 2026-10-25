# Billing rules

What the platform charges, written so finance, the billing team and an engineer can agree on it.
Each rule names where it is implemented and the test that pins it.

## Time

- **Usage belongs to the month it happened in**, in UTC, never the month its data arrived.
  Bronze is partitioned by event date (`ingest.py`); `test_bronze_is_partitioned_by_event_date_not_arrival`.
- **The control plane's clock is the clock.** Lifecycle events (`created`, `started`, `stopped`,
  `deleted`) are stamped by the control plane. Heartbeats are stamped by the host, whose clock can
  be minutes off, so a heartbeat's time is `started_at + seq × 300 s`; the host clock is kept only
  to measure its skew (`models/03_silver_heartbeats.sql`). Lineage proves host clocks cannot reach
  a money column (`test_host_clocks_cannot_move_money`).

## GPU time

- **Per second, from `started` to `stopped`, with a 60-second minimum** per session
  (`models/04_silver_vm_sessions.sql`; `test_per_second_billing…`, `test_sixty_second_minimum`).
- **A lost stop event**: the session ends at the earlier of `deleted` and one heartbeat period
  (300 s) after the last heartbeat, once the VM has been silent for a heartbeat plus 15 minutes
  (`GRACE_S`). This can over-bill by at most one heartbeat period; for the largest VM (8 × B200 at
  $5.50/h) that is $3.67. `end_source = 'inferred'` marks these sessions, and the
  `inferred_session_end` check tells the control-plane team.
- **A VM still running** is metered up to the run's watermark (`now`), and recomputed on the next run.
- **Duplicates**: a lifecycle event counts once per `event_id`; a heartbeat once per `(vm_id, seq)`,
  because an agent that restarts re-sends its spool with new event ids (`test_duplicates_and_replayed_spools_count_once`).

## Commitments (reserved capacity)

- A commitment reserves `gpus` of one GPU type in one region for its term, at its own rate. It is
  charged for every second of its term whether used or not, pro rata within the month.
- **Usage is netted per minute**: in each minute, a customer's GPU-seconds of that type and region
  up to `gpus × 60` are covered by the commitment; the rest is on-demand at the list price in
  force at that minute (`models/10_gold_gpu_minutes.sql`).
- **A renewal replaces the commitment it renews** (`replaces` in the contract register): the old
  term ends when the renewal starts, so the overlap is not charged twice
  (`models/07_silver_commitment_terms.sql`; `test_renewal_replaces_the_commitment_it_renews`).

## Prices

- List prices are effective-dated in `config/pricing.yaml`. A price applies from its `from` time
  until the next entry's. A minute is priced at the price in force at its start
  (`test_price_change_at_month_boundary`). GPU-seconds with no list price (a GPU type before its
  launch, say) are counted as `unpriced_seconds` and page on-call; they are never priced at zero
  silently.

## Inference and storage

- **Inference** is charged per million input and output tokens, per 10-minute window. The gateway
  corrects a window by re-sending its `event_id` with a higher `revision`: the highest revision
  wins, and retries of the same revision count once (`models/05_silver_inference.sql`).
- **Storage** is charged per GB-month: hourly snapshots of bytes stored, each worth
  `GB × price / 730` (`models/11_gold_charges.sql`).

## Credits

- Prepaid credits pay for on-demand GPU, inference and storage, never for commitments, oldest
  month first (`periods.apply_credits`; `test_credits_pay_on_demand_oldest_month_first_never_commitments`).

## Rounding

- Each invoice line (on-demand GPU, commitments, inference, storage, credits) is rounded to the
  cent per customer and month; the total is the sum of the rounded lines. Credits applied never
  exceed the rounded lines they pay for (`periods.current`, `sim/truth.Bill.rounded`).

## Closing a month

- A month closes at the time in `config/periods.yaml` (3rd of the next month, 12:00 UTC). Closing
  writes its lines to an append-only ledger. After that, anything that changes the month (late
  events, a corrected batch, a contract fix) is an **adjustment entry with a reason**; the closed
  figures are never edited (`periods.py`; `test_late_usage_restates_a_closed_month_without_changing_what_was_reported`).
- "What did we report for September on 5 October?" is `periods.revenue_as_of`.
