# Stock Sentinel

Stock Sentinel is a starter Flask app for **retailer stock monitoring + Discord webhook alerts**.

> This project intentionally does **not** implement auto-checkout or anti-bot bypass behavior.

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
- Set `API_AUTH_TOKEN` (defaults to `dev-token`) and send either:
  - `Authorization: Bearer <token>`, or
  - `X-API-Token: <token>`
- `POST /api/webhooks` to add Discord webhook.
- `POST /api/monitors` to add product monitor.
- `POST /api/checkout/tasks` to create a checkout task for an existing monitor.
- `POST /api/checkout/tasks/:id/start|pause|stop` to manage checkout task lifecycle.
- `GET /api/checkout/tasks/:id/state` to read canonical task state and last attempt metadata.
- `POST /api/billing/stripe/webhook` for Stripe subscription lifecycle ingestion (signature-verified and idempotent by `event.id`).
- `POST /api/start` to begin background checks.
- `POST /api/monitors/:id/check` to run an immediate check.
- `GET /api/workspace/usage-limits` to retrieve plan limits + current usage snapshot.

Stripe webhook configuration:

- `STRIPE_WEBHOOK_SECRET` (required): signing secret used to verify `Stripe-Signature`.
- `STRIPE_WEBHOOK_TOLERANCE_SECONDS` (optional, default `300`): max allowed timestamp drift for webhook signatures.

Monitor check response compatibility notes:

- `POST /api/monitors/:id/check` now includes `availability_reason` and `parser_confidence`.
- Existing response fields are unchanged.
- `parser_confidence` is normalized to the range `[0.0, 1.0]`; if unavailable/invalid, it is `null`.
- Clients that do not use these new fields can safely ignore them.

Example:

```bash
curl -H "Authorization: Bearer dev-token" http://localhost:5000/api/workspace
```

Usage limits snapshot example:

```bash
curl -H "Authorization: Bearer dev-token" http://localhost:5000/api/workspace/usage-limits
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
- authentication and multi-tenant authz,
- Stripe billing webhooks,
- stronger HTML parsing adapters per retailer,
- observability and error dashboards.

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
