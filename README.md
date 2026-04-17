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

- `POST /api/webhooks` to add Discord webhook.
- `POST /api/monitors` to add product monitor.
- `POST /api/start` to begin background checks.
- `POST /api/monitors/:id/check` to run an immediate check.

## Notes

This project is a scaffold for a subscription monitoring SaaS and should be expanded with:
- authentication and multi-tenant authz,
- Stripe billing webhooks,
- stronger HTML parsing adapters per retailer,
- observability and error dashboards.
