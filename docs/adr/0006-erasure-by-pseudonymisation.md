# 6. Erasure replaces personal data with a salted pseudonym everywhere, and proves it

Status: accepted

## Context

GDPR Art. 17 requests reach a metering platform through two fields: the inference gateway's
`api_key_owner` (an email) and the CRM's `contact_email`. Invoices must still add up after a person
is erased, and the request must be provable afterwards without keeping the email.

## Decision

`acm erase` replaces the email with `erased:<salted hash>` in landing files that have arrived,
bronze Parquet (the contract's `pii` columns), quarantine and every string column in the warehouse;
records the new checksums of rewritten landing files so ingestion does not report them as
tampering; scans everything again and fails unless nothing is left and revenue is unchanged; and
writes an audit row with the salted hash only. The hash goes on a suppression list, so the same
person's data that arrives later is pseudonymised at ingestion, landing copy included.

## Consequences

- Usage stays attributed to the customer (a company), which is not personal data.
- The salt lives in the warehouse; whoever holds the warehouse can test a guessed email against
  the audit. In production the salt belongs in a secret store.
- Backups and anything exported before the request are out of this tool's reach; the runbook
  lists them.
