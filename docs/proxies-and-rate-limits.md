# Proxies and Rate Limits

This project now uses a shared request/session utility (`network/session_manager.py`) for monitor fetches,
webhook deliveries/tests, and update checks.

## Current controls

- Per-monitor polling cadence: `poll_interval_seconds`
- Global loop cadence: `POLL_LOOP_SECONDS` environment variable
- Per-task persistent sessions (cookie jar persisted under `.session_cookies/`)
- Configurable timeout + retry + backoff in request helper calls
- Structured request telemetry logged for latency/status/error class
- Optional proxy assignment at workspace/monitor level via DB fields:
  - `workspaces.proxy_url` (workspace default)
  - `monitors.proxy_url` (monitor override)
  - Session metadata placeholders:
    - `workspaces.session_metadata`
    - `monitors.session_task_key`
    - `monitors.session_metadata`
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

- Add rotating proxy pools
- Add retailer-specific backoff and adaptive throttling
