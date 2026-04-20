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
2. Poll loop evaluates each enabled monitor on its interval.
3. Eligibility logic applies stock markers + optional keyword/price/MSRP filters.
4. Eligible checks are deduplicated and inserted into `events`.
5. Event payloads are delivered to all enabled webhooks and logged in `deliveries`.

## Where to start

- Task setup: `docs/task-creation.md`
- Proxy/rate guidance: `docs/proxies-and-rate-limits.md`
- Error glossary: `docs/errors-and-statuses.md`
- Retailer-specific guidance: `docs/retailer-playbooks/`
