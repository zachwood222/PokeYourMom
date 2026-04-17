# Stock Sentinel Documentation

Stock Sentinel is a Flask-based monitor and alert bot for retailer product availability.

## Architecture at a glance

- **Web app/API:** Flask (`app.py`) provides dashboard routes and JSON endpoints.
- **Storage:** SQLite tables for workspaces, monitors, events, webhooks, and delivery results.
- **Background checks:** in-process monitor loop polls enabled monitors and emits events.
- **Notifications:** Discord webhooks receive rich embeds when a monitor is eligible.

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
