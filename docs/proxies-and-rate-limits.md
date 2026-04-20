# Proxies and Rate Limits

This service now has first-class proxy pool management with lock-based leasing so monitor checks and checkout
tasks can share a pool safely.

## Data model

- `proxies`
  - Required: `provider`, `endpoint`, `proxy_type`, `status`, `fail_streak`
  - Health telemetry counters: `request_count`, `success_count`, `timeout_count`, `rate_limited_count`,
    `forbidden_count`, `failure_count`, `health_score`
  - Control fields: `cooldown_until`, `quarantine_reason`, `region_code`, `is_residential`,
    `sticky_session_seconds`, `last_used_at`
- `proxy_leases`
  - Lease ownership: `proxy_id`, `owner_type`, `owner_id`, `lease_key`
  - Lock lifecycle: `acquired_at`, `expires_at`, `released_at`
  - Active leases are unique per proxy (`released_at is null`).

## Lease semantics

- Lease acquisition is atomic (`BEGIN IMMEDIATE`) and releases expired leases before selecting candidates.
- Candidate selection rejects proxies that are:
  - disabled/quarantined (`status != 'active'`)
  - still cooling down (`cooldown_until > now`)
  - currently leased by another owner.
- Monitors lease a proxy for each check and release it after request completion.
- Checkout tasks lease on start (`/start`) and release on pause/stop/success/failure transitions.

## Proxy policy constraints (monitor + task config)

You can constrain allocator selection through:

- `residential_only` / monitor field `proxy_residential_only`
- `region` / monitor field `proxy_region`
- `sticky_session_seconds` / monitor field `proxy_sticky_session_seconds`
- `type` / monitor field `proxy_type`

When present, these policies are enforced in allocator SQL filters before a proxy is leased.

## Health scoring and auto-quarantine

Each proxied request records telemetry (success/failure + status code class):

- Timeout pressure (`Timeout`, `ReadTimeout`, `ConnectTimeout`)
- Anti-bot pressure (`429` and `403` rates)
- Success ratio

`health_score` is recalculated after each request. Proxies are automatically quarantined (`status='quarantined'`)
when severe trends are detected, for example:

- fail streak reaches 5+
- low score after warmup traffic
- sustained high severe failure rate.

Quarantine applies a cooldown window (`cooldown_until`), so the allocator will skip that endpoint until expiry
or operator intervention.

## Operator guidance

- Seed multiple providers/endpoints to avoid single-proxy hotspots.
- Prefer residential-only policy only where truly needed (it reduces available pool capacity).
- Use region targeting only when retailer behavior requires geolocation affinity.
- Keep sticky sessions short for monitor checks; use longer sticky windows for checkout workflows.
- Investigate proxies with repeated `auto_health_quarantine` reasons and high `403` rates first.
