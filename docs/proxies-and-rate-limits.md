# Proxies and Rate Limits

This project now uses a shared request/session utility (`network/session_manager.py`) for monitor fetches,
webhook deliveries/tests, and update checks.

## Current controls

- Per-monitor polling cadence: `poll_interval_seconds`
- Global loop cadence: `POLL_LOOP_SECONDS` environment variable
- Queue-side randomized scheduling:
  - `QUEUE_ENQUEUE_JITTER_SECONDS` (default `1.25s`) spreads newly due jobs
  - `WORKER_IDLE_SLEEP_JITTER_SECONDS` (default `0.75s`) avoids synchronized worker wakes
  - `WORKER_ACTIVE_JITTER_SECONDS` (default `0.2s`) adds bounded spacing between claimed jobs
- Per-task persistent sessions (cookie jar persisted under `.session_cookies/`)
- Configurable timeout + retry + backoff in request helper calls
- Structured request telemetry logged for latency/status/error class plus pacing fields:
  - `pacing_profile`
  - `planned_delay_ms`
  - `applied_delay_ms`
  - `adaptive_level`
  - `throttled`
  - `throttle_reason`
- Optional proxy assignment at workspace/monitor level via DB fields:
  - `workspaces.proxy_url` (workspace default)
  - `monitors.proxy_url` (monitor override)
- Session metadata placeholders:
    - `workspaces.session_metadata`
    - `monitors.session_task_key`
    - `monitors.session_metadata`
- Request behavior metadata:
  - `workspaces.behavior_metadata` (workspace default policy)
  - `monitors.behavior_metadata` (monitor override policy)
- Plan minimum intervals:
  - `basic`: 20s
  - `pro`: 10s
  - `team`: 5s

## Policy defaults

Default request behavior policy:

- `base_delay_seconds`: `0.2`
- `jitter_ratio`: `0.2`
- `min_delay_seconds`: `0.05`
- `max_delay_seconds`: `2.5`
- `adaptive_backoff_enabled`: `true`
- `adaptive_backoff_step_seconds`: `0.4`
- `adaptive_backoff_cap_seconds`: `5.0`
- Retailer profiles (override default profile):
  - walmart: base `0.3s`, max `3.0s`
  - target: base `0.25s`, max `2.5s`
  - bestbuy: base `0.2s`, max `2.0s`
  - pokemoncenter: base `0.35s`, max `3.5s`

## Operational guidance

- Keep monitor intervals conservative for large monitor sets (target 20s+ unless paid plan and proven stable).
- Stagger monitor intervals and keep enqueue jitter enabled to reduce synchronized bursts.
- Prefer retailer-specific parsers over broad keyword matching to reduce retries.
- Use behavior metadata overrides per workspace/monitor instead of globally shrinking delays.
- Tune only one dimension at a time (poll interval, base delay, or backoff step), and observe telemetry for at least 30 minutes.

## Safe operational limits

- Recommended `poll_interval_seconds` floor:
  - `basic`: `>= 20s`
  - `pro`: `>= 10s`
  - `team`: `>= 5s`
- Recommended request pacing bounds:
  - `base_delay_seconds`: `0.15s` to `1.5s`
  - `jitter_ratio`: `0.1` to `0.35`
  - `max_delay_seconds`: `<= 5s`
  - `adaptive_backoff_cap_seconds`: `<= 10s`
- If throttle signals (`429`/`503`) or `throttled=1` increase for more than 10 minutes:
  - raise base delay by `+0.1s` to `+0.25s`,
  - keep jitter at or above `0.2`,
  - avoid increasing worker count before throttle rate stabilizes.

## Next enhancements

- Add rotating proxy pools
- Add explicit throttle outcome aggregation endpoints for dashboard tuning
