# 1. Usage is placed by event time, and heartbeats by the schedule

Status: accepted

## Context

Usage data reaches the platform late (a collector outage held eu-north1 back for three days),
twice (producer retries, agent spool replays) and with clocks that disagree (15% of hosts in the
simulation are 30 s to 5 min off). A pipeline that assigns usage to the month its file arrived in
moves revenue between months whenever delivery is late; one that trusts host clocks moves it
between minutes, and so between commitment-covered and on-demand.

## Decision

- Bronze is partitioned by the record's own event date. Late data lands in the right partition and
  marks its month for recomputation.
- The control plane's lifecycle events are the clock for sessions. A heartbeat's time is
  `started_at + seq × 300 s`; its own timestamp is kept only to measure the host's skew, which the
  `clock_skew` check reports.
- Heartbeats are identified by `(vm_id, seq)`, not by event id.

## Consequences

- The naive pipeline (`acm.naive`), which uses arrival months and host time, is $30k off for
  September in the replay; this platform is within a cent.
- A host with a bad clock cannot change a bill; `tests/test_lineage.py` proves no money column
  depends on `host_time`.
- If the control plane's own clock were wrong, everything would be wrong consistently. The
  control plane is NTP-disciplined infrastructure; the hosts are customer-facing machines.
