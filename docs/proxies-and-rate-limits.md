# Proxies and Rate Limits

This project currently uses direct `requests.get()` calls and does not yet include first-class proxy pools.

## Current controls

- Per-monitor polling cadence: `poll_interval_seconds`
- Global loop cadence: `POLL_LOOP_SECONDS` environment variable
- Plan minimum intervals:
  - `basic`: 20s
  - `pro`: 10s
  - `team`: 5s

## Operational guidance

- Keep monitor intervals conservative for large monitor sets.
- Stagger monitor intervals across tasks to reduce burst pressure.
- Prefer retailer-specific parsers over broad keyword matching to reduce retries.
- Add request retry and timeout telemetry before increasing concurrency.

## Next enhancements

- Add optional proxy per monitor/workspace
- Add rotating proxy pools
- Add retailer-specific backoff and adaptive throttling
