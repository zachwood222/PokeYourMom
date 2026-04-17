# Common Errors and Statuses

## API validation errors

- `Unsupported retailer '<value>'`
  - Use one of: `walmart`, `target`, `bestbuy`.
- `product_url must start with http:// or https://`
  - Ensure fully-qualified product URL.
- `poll_interval_seconds must be > 0`
  - Use a positive integer.
- `Plan <plan> minimum poll interval is <n> seconds`
  - Increase interval to satisfy plan constraints.

## Monitor status fields

- `in_stock`: raw in-stock marker evaluation from page text.
- `eligible_for_alert`: in-stock + all filters passed.
- `keyword_matched`: keyword pass/fail when keyword is configured.
- `price_within_limit`: max-price pass/fail when configured.
- `within_msrp_delta`: pokemon MSRP pass/fail when configured.

## Delivery statuses

- `sent`: webhook returned 2xx.
- `failed`: request exception or non-2xx response.
