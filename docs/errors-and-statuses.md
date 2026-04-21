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

## Ownership and tenancy policy

- Monitor read/update/delete/check endpoints are workspace-scoped.
- If a monitor ID does not exist **or** belongs to a different workspace, the API returns:
  - `404` with `{"error":"Monitor not found"}`
- This avoids cross-tenant resource enumeration and keeps behavior consistent.

## Monitor status fields

- `in_stock`: raw in-stock marker evaluation from page text.
- `eligible_for_alert`: in-stock + all filters passed.
- `keyword_matched`: keyword pass/fail when keyword is configured.
- `price_within_limit`: max-price pass/fail when configured.
- `within_msrp_delta`: pokemon MSRP pass/fail when configured.

## Delivery statuses

- `sent`: webhook returned 2xx.
- `failed`: request exception or non-2xx response.

## Checkout OTP statuses

- `verification_pending`
  - Checkout is waiting for an OTP email.
- `verification_code_received`
  - OTP was found and normalized in task config.

## Checkout OTP error codes

- `OTP_CONFIG_ERROR`
  - Missing OTP-required config (e.g., no `mailbox_credential_id`) or mailbox credential not found.
  - Runbook:
    1. Verify `task_config.otp_required=true` includes `mailbox_credential_id`.
    2. Confirm mailbox exists in `GET /api/mailboxes` for the same workspace.
    3. Confirm the mailbox secret type is one of `mailbox_password`, `mailbox_oauth_refresh_token`, `mailbox_oauth_access_token`.

- `OTP_PROVIDER_ERROR`
  - IMAP/provider failure (unsupported provider, auth failure, host mismatch, inbox select/search failure).
  - Runbook:
    1. Validate provider name (`gmail`, `outlook`, `imap`, `custom_imap`).
    2. Re-test mailbox auth credentials and app-password/OAuth token validity.
    3. For custom IMAP, verify `imap_host`, `imap_port`, and SSL settings.

- `OTP_TIMEOUT`
  - OTP did not arrive before timeout.
  - Runbook:
    1. Increase `otp_timeout_seconds`.
    2. Lower `otp_poll_interval_seconds` if provider rate limits permit.
    3. Tighten/adjust `sender_filter`, `subject_filter`, and `otp_regex`.
    4. Confirm message delivery path (spam/junk/quarantine rules).
