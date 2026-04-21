# Checkout Task Creation Guide

A checkout task is represented by a row in `checkout_tasks` and is linked to a monitor (`monitor_id`).

## Canonical API flow

1. Create a monitor:

```bash
curl -X POST http://localhost:5000/api/monitors \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "retailer": "target",
    "product_url": "https://www.target.com/p/example",
    "poll_interval_seconds": 20,
    "keyword": "pokemon",
    "max_price_cents": 4000,
    "msrp_cents": 499
  }'
```

2. Create a checkout task bound to that monitor:

```bash
curl -X POST http://localhost:5000/api/checkout/tasks \
  -H 'Authorization: Bearer dev-token' \
  -H 'Content-Type: application/json' \
  -d '{
    "monitor_id": 1,
    "task_name": "Target checkout",
    "task_config": {
      "retailer": "target",
      "product_url": "https://www.target.com/p/example",
      "profile": "profile-main",
      "account": "acc-primary",
      "payment": "visa-ending-4242"
    }
  }'
```

3. Start and/or run the task state machine:

```bash
curl -X POST http://localhost:5000/api/checkout/tasks/1/start \
  -H 'Authorization: Bearer dev-token'

curl -X POST http://localhost:5000/api/checkout/tasks/1/run \
  -H 'Authorization: Bearer dev-token'
```

## Task profile bindings (recommended)

`task_profile_bindings` can bind a monitor to:
- `checkout_profile_id`
- `retailer_account_id`
- `payment_method_id`

At execution time, checkout reads bindings and resolves context values used in payment/submitting phases. If required binding/config values are missing, task execution fails fast with actionable error codes (for example: `binding_payment_missing`, `missing_payment_binding_or_config`).
