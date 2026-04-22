# Checkout Task Creation Guide

Canonical task lifecycle is handled by `/api/checkout/tasks*`.
A checkout task references a monitor (`monitor_id`) and stores task metadata in `checkout_tasks`.

## Canonical API flow

Monitor management endpoints are canonical at `/api/monitors` + `/api/monitors/<id>` (GET/PATCH/DELETE). Immediate checks use `POST /api/monitors/<id>/check`.

- `retailer`: one of `walmart`, `target`, `bestbuy`, `pokemoncenter`
  - aliases accepted for Pokemon Center: `pokemon-center`, `pokemon_center`, `pokemon center`
- `category`: one of `pokemon`, `sports_cards`, `one_piece`, `lorcana`
- `product_url`: must start with `http://` or `https://`
- `poll_interval_seconds`: integer > 0 and must satisfy plan minimum

## Supported retailer + category pairs

- `target`: `pokemon`, `sports_cards`, `one_piece`, `lorcana`
- `pokemoncenter`: `pokemon`, `sports_cards`, `one_piece`, `lorcana`
- `walmart`: `pokemon`
- `bestbuy`: `pokemon`

## Optional filters

- `keyword`: alerts only when page text contains this keyword.
- `max_price_cents`: alerts only when parsed price is <= this value.
- `msrp_cents`: when keyword includes `pokemon`, enforces a configurable MSRP buffer.

## API example

```bash
curl -X POST http://localhost:5000/api/monitors \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "retailer": "target",
    "category": "pokemon",
    "product_url": "https://www.target.com/p/example",
    "poll_interval_seconds": 20,
    "keyword": "pokemon",
    "max_price_cents": 4000,
    "msrp_cents": 499
  }'
```

## Checkout task lifecycle (canonical API)

After creating a monitor, use its `id` to create/manage checkout tasks:

```bash
curl -X POST http://localhost:5000/api/checkout/tasks \
  -H 'Content-Type: application/json' \
  -H 'Authorization: Bearer dev-token' \
  -d '{
    "monitor_id": 1,
    "task_name": "Target task",
    "task_config": {
      "profile": "default",
      "account": "acct-1",
      "payment": "visa"
    }
  }'
```

Lifecycle endpoints:

- `POST /api/checkout/tasks/<id>/start`
- `POST /api/checkout/tasks/<id>/pause`
- `POST /api/checkout/tasks/<id>/stop`
- `GET /api/checkout/tasks/<id>/state`
