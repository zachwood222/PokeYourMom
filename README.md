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
- `POST /api/start` to begin background checks.
- `POST /api/monitors/:id/check` to run an immediate check.

Example:

```bash
curl -H "Authorization: Bearer dev-token" http://localhost:5000/api/workspace
```

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

To add a new fixture case:

1. Add the HTML snapshot in `tests/fixtures/<retailer>/`.
2. Register a `pytest.param(...)` case in `tests/parser_fixture_harness.py` (case IDs are formatted as `<retailer>:<fixture_name>`).
3. Run `pytest tests/test_parser_fixtures.py` to validate the new snapshot.
