# Task Creation Guide

A **task** maps to a monitor row in the `monitors` table.

## Required fields

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
