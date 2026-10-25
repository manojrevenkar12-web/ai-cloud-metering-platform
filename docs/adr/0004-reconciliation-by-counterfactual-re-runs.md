# 4. Reconcile with the billing system by re-running our own models under its behaviours

Status: accepted

## Context

The billing system is another team's service. It invoices on its own schedule with what it has,
and with defects of its own. A reconciliation that only reports "we differ by $5,756" is a ticket
nobody can close; one built from guessed rules ("probably rounding") explains what it assumes.

## Decision

The difference is explained as a waterfall over charges before credits (`recon.py`):

1. **late_usage**: our models re-run on exactly the data visible at the invoice cutoff.
2. Each suspected billing behaviour (stop events missing → month end, hourly rounding in
   us-central1, renewals on top of the commitment they replace, inference retries not
   de-duplicated) is applied to our own silver tables and the models re-run, one at a time,
   cumulatively.
3. What is left is **unexplained**, and the reconciliation check fails above $1 per customer.

A hypothesis that is wrong explains nothing, so the residual stays visible. Results are cached by
a fingerprint of the reference data and of what was visible at the cutoff.

Credits are reported beside the waterfall: an overcharge a customer's prepaid credits paid leaves
the invoice total equal and only shows in the balance.

## Consequences

- September's $5,756.37 difference is explained to $0.00; $1,300.75 of it was paid from customers'
  prepaid credits and would have been invisible on net totals.
- Each step costs a model re-run over the cutoff data (two seconds at this volume, cached between
  runs).
