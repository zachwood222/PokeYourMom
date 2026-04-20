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

## Configuration

- `APP_VERSION` (default: `0.1.0`): current running app version used by `/api/meta` and `/api/meta/check-update`.
- `RELEASE_CHANNEL` (default: `stable`): metadata channel returned by meta endpoints.
- `UPDATE_CHECK_URL` (default: empty): optional upstream URL used by `/api/meta/check-update` to resolve the latest available version.
  - Supports JSON payloads with `latest_version`, `version`, or `tag_name`.
  - Supports plain text payloads containing just the version string.
  - If unset or if the upstream request fails/parsing fails, the endpoint returns a non-fatal fallback payload and includes `source_error`.
- `UPDATE_CHECK_TIMEOUT_SECONDS` (default: `3.0`): timeout (seconds) for the update check request.

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
