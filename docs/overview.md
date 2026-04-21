# Stock Sentinel Documentation

Stock Sentinel is a Flask-based monitor and alert bot for retailer product availability.

## Architecture at a glance

- **Web app/API:** Flask (`app.py`) provides dashboard routes and JSON endpoints.
- **Storage:** SQLite tables for workspaces, monitors, events, webhooks, delivery results, and billing schema state.
- **Background checks:** in-process monitor loop polls enabled monitors and emits events.
- **Notifications:** Discord webhooks receive rich embeds when a monitor is eligible.

## Billing schema summary (schema-only)

`init_db()` now provisions billing state tables only (no Stripe SDK import and no network calls):

- **`billing_customers`**
  - Maps a workspace/user relationship to an external billing customer identity.
  - Key fields:
    - `workspace_id`, `user_id` (unique pair)
    - `provider` (default: `stripe`)
    - `provider_customer_id` (unique index; nullable for pre-link records)
    - `created_at`, `updated_at`

- **`billing_subscriptions`**
  - Stores one subscription record per workspace and links to provider subscription metadata.
  - Key fields:
    - `workspace_id` (unique)
    - `provider` (default: `stripe`)
    - `provider_subscription_id` (unique index; nullable for pre-link records)
    - `billing_customer_id` (FK to `billing_customers`)
    - `status`
    - `current_period_end`
    - `cancel_at_period_end`
    - plan mapping fields: `plan_code`, `plan_interval`, `plan_lookup_key`
    - `created_at`, `updated_at`

## Runtime flow

1. Operator adds monitors and webhooks in the dashboard.
2. Optional checkout tasks are created via `POST /api/checkout/tasks` using an existing monitor id.
3. Task lifecycle uses canonical checkout endpoints (`/api/checkout/tasks/<id>/start|pause|stop|state`).
4. Poll loop evaluates each enabled monitor on its interval.
5. Eligibility logic applies stock markers + optional keyword/price/MSRP filters.
6. Eligible checks are deduplicated and inserted into `events`.
7. Event payloads are delivered to all enabled webhooks and logged in `deliveries`.

## Canonical checkout task API

- Create task: `POST /api/checkout/tasks`
- Start task: `POST /api/checkout/tasks/<id>/start`
- Pause task: `POST /api/checkout/tasks/<id>/pause`
- Stop task: `POST /api/checkout/tasks/<id>/stop`
- Inspect state: `GET /api/checkout/tasks/<id>/state`

Legacy `/api/tasks*` compatibility endpoints have been removed; use `/api/checkout/tasks*` as the single source of truth for task lifecycle.

## Workspace usage limits API

Authenticated endpoint: `GET /api/workspace/usage-limits`

Stable response shape:

- `plan`: current workspace plan code.
- `usage`
  - `monitor_count`: total monitors in workspace.
  - `min_poll_interval_seconds`: minimum configured monitor poll interval, or `null` for empty workspaces.
- `limits`
  - `max_monitors`: monitor cap for the current plan.
  - `min_poll_seconds`: minimum allowed poll interval for the current plan.
- `derived`
  - `monitor_slots_remaining`: available monitor slots before hitting plan cap.
  - `monitor_limit_reached`: whether monitor cap has been reached.
  - `poll_minimum_satisfied`: whether current minimum poll interval respects the plan minimum.

## Where to start

- Task setup: `docs/task-creation.md`
- Proxy/rate guidance: `docs/proxies-and-rate-limits.md`
- Error glossary: `docs/errors-and-statuses.md`
- Retailer-specific guidance: `docs/retailer-playbooks/`
