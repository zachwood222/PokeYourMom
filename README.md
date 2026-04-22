# Stock Sentinel

Stock Sentinel is a Flask app for **retailer stock monitoring + Discord webhook alerts + an experimental checkout workflow**.

> Checkout support is experimental and workflow-focused; this project does **not** implement anti-bot bypass behavior.

## Features

- Multi-monitor setup for Walmart / Target / Best Buy product URLs.
- Plan-based limits (`basic`, `pro`, `team`) for monitor count and poll frequency.
- Background polling loop.
- In-stock event dedupe.
- Discord webhook notifications with embeds.
- Lightweight dashboard for monitor management.

## Run locally

```bash
python -m venv .venv
source .venv/bin/activate
pip install -r requirements.txt
python app.py
```

Then open `http://localhost:5000`.

## CI checks locally

Run the same commands used in CI:

```bash
python -m pip install --upgrade pip
pip install -r requirements.txt
pip install pytest ruff
ruff check tests
python -m compileall -q app.py tests
pytest -q tests
pytest -q tests/test_billing_schema.py::test_init_db_creates_billing_tables_and_columns
pytest -q tests/test_app.py::test_init_db_migrates_existing_monitors_table_with_msrp_column
```

The last two commands are explicit migration-safety checks for:

- fresh database creation via `init_db()`,
- legacy `monitors` schema upgrade behavior (including `msrp_cents` migration).


## API quick start

- API authentication is required for all `/api/*` routes.
- In production/non-dev environments, set `API_AUTH_TOKEN` and `SECRET_ENCRYPTION_KEY` explicitly before startup.
- In local development mode (`APP_ENV=development`), safe local defaults are used (`API_AUTH_TOKEN=dev-token`, `SECRET_ENCRYPTION_KEY=local-dev-secret-key`).
- Send either:
  - `Authorization: Bearer <token>`, or
  - `X-API-Token: <token>`
- CORS behavior:
  - Development mode allows `*`.
  - Production mode requires explicit origins via `ALLOWED_ORIGINS` (comma-separated, for example `https://app.example.com,https://admin.example.com`).
- `POST /api/webhooks` to add Discord webhook.
- `POST /api/monitors` to add product monitor.
- `POST /api/checkout/tasks` to create a checkout task for an existing monitor.
- `POST /api/checkout/tasks/:id/start|pause|stop` to manage checkout task lifecycle.
- `GET /api/checkout/tasks/:id/state` to read canonical task state and last attempt metadata.
- `POST /api/billing/stripe/webhook` (and alias `POST /api/stripe/webhook`) for Stripe subscription lifecycle ingestion (signature-verified and idempotent by `event.id`).
- `POST /api/start` to begin background checks.
- `POST /api/checkout/tasks` to create a checkout task from an existing monitor.
- `POST /api/checkout/tasks/:id/start` to transition task into `monitoring`.
- `POST /api/checkout/tasks/:id/run` to execute the checkout state machine (`monitoring` → `carting` → `shipping` → `payment` → `submitting`).
- `GET /api/checkout/tasks/:id/attempts` to fetch execution attempts (use `?include_created=1` to include initialization rows).
- `POST /api/start` to begin background monitor checks.
- `POST /api/monitors/<id>/check` to run an immediate check.
- `GET /api/workspace/usage-limits` to retrieve plan limits + current usage snapshot.

Stripe webhook configuration:

- `STRIPE_WEBHOOK_SECRET` (required): signing secret used to verify `Stripe-Signature`.
- `STRIPE_WEBHOOK_TOLERANCE_SECONDS` (optional, default `300`): max allowed timestamp drift for webhook signatures.

Billing state transitions (workspace plan sync):

- `customer.subscription.created` / `customer.subscription.updated`: workspace `subscription_status` is updated and plan is mapped from Stripe plan metadata (`pro`/`team` lookup keys map to higher tiers, otherwise `basic`).
- `customer.subscription.deleted`: workspace is forced to `basic` with `subscription_status=canceled` (preserves existing plan enforcement behavior for monitor count/poll minimums).

Update-check configuration (`GET /api/meta/check-update`):

- `UPDATE_CHECK_URL`: upstream URL for latest-version metadata (JSON `latest_version`/`version`/`tag_name`, or plain text version).
- `UPDATE_CHECK_TIMEOUT_SECONDS` (optional, default `2.0`): upstream timeout.
- `UPDATE_CHECK_AUTH_HEADER` (optional, default `Authorization`): header name used when auth token is configured.
- `UPDATE_CHECK_AUTH_TOKEN` (optional): auth value sent to the upstream update-check source.

Monitor check response compatibility notes:

- `POST /api/monitors/:id/check` now includes `availability_reason` and `parser_confidence`.
- Existing response fields are unchanged.
- `parser_confidence` is normalized to the range `[0.0, 1.0]`; if unavailable/invalid, it is `null`.
- Clients that do not use these new fields can safely ignore them.

Example:

```bash
curl -H "Authorization: Bearer ${API_AUTH_TOKEN}" http://localhost:5000/api/workspace
```

Usage limits snapshot example:

```bash
curl -H "Authorization: Bearer ${API_AUTH_TOKEN}" http://localhost:5000/api/workspace/usage-limits
```

### Environment variables (security-sensitive)

Required in non-dev (`APP_ENV` not set to `development`/`test`):

- `API_AUTH_TOKEN`: bearer token required by `/api/*` routes.
- `SECRET_ENCRYPTION_KEY`: secret key used for encrypt/decrypt of stored sensitive values.

Recommended in non-dev:

- `ALLOWED_ORIGINS`: comma-separated CORS allowlist for Socket.IO/browser clients.

Safe production example:

```bash
export APP_ENV=production
export API_AUTH_TOKEN='replace-with-strong-random-token'
export SECRET_ENCRYPTION_KEY='replace-with-32+-char-random-secret'
export ALLOWED_ORIGINS='https://app.example.com,https://admin.example.com'
python app.py
```

```json
{
  "plan": "basic",
  "usage": {
    "monitor_count": 3,
    "min_poll_interval_seconds": 20
  },
  "limits": {
    "max_monitors": 20,
    "min_poll_seconds": 20
  },
  "derived": {
    "monitor_slots_remaining": 17,
    "monitor_limit_reached": false,
    "poll_minimum_satisfied": true
  }
}
```

Schema notes:
- `plan`: active workspace plan (`basic`, `pro`, `team`).
- `usage.monitor_count`: current number of monitors in the workspace.
- `usage.min_poll_interval_seconds`: smallest configured poll interval among workspace monitors (`null` when no monitors exist).
- `limits.max_monitors` / `limits.min_poll_seconds`: enforced values from `PLAN_LIMITS`.
- `derived.*`: convenience fields based on usage + limits.

## Notes

This project is a scaffold for a subscription monitoring SaaS and should be expanded with:
- stronger checkout automations and provider integrations,
- stronger HTML parsing adapters per retailer,
- richer observability and error dashboards.

## Phase 2 baseline shipped

- Keyword filters are now evaluated during page checks and can suppress alerts when no match is found.
- Max price filters are now enforced before creating in-stock events and sending Discord webhooks.
- Pokemon monitors can enforce MSRP protection by setting `msrp_cents`; alerts only fire when current price is within `$10` of MSRP (configurable with `POKEMON_MSRP_BUFFER_CENTS`).
- `last_in_stock` now reflects "eligible for alert" state (in stock + filters pass), not raw page stock marker detection.

## Documentation

- Architecture and runbooks: `docs/overview.md`
- Task creation: `docs/task-creation.md`
- Error glossary: `docs/errors-and-statuses.md`
- Retailer playbooks: `docs/retailer-playbooks/`

## Parser fixture tests

Retailer parser snapshots live under `tests/fixtures/<retailer>/` and use this naming convention:

- `in_stock.html`
- `out_of_stock.html`
- `ambiguous.html`

The regression harness in `tests/test_parser_fixtures.py` asserts two expectations for each fixture:

- `expected_in_stock` (`True`/`False`)
- `expected_status` (`in_stock` or `out_or_unknown`)
- failure output includes the exact fixture path (for example `walmart/ambiguous.html`) to make regressions easy to triage.

To add a new fixture case:

1. Add the HTML snapshot in `tests/fixtures/<retailer>/`.
2. Register the expected output in `PARSER_FIXTURE_EXPECTATIONS` in `tests/parser_fixture_harness.py`.
3. Ensure each retailer still includes the required baseline snapshots: `in_stock`, `out_of_stock`, and `ambiguous`.
4. Run `pytest tests/test_parser_fixtures.py` to validate the new snapshot.
