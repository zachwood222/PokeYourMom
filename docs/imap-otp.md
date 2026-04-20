# IMAP / OTP Integration Notes

Stock Sentinel now supports mailbox-backed OTP retrieval for checkout verification steps.

## Supported providers

- `gmail` (IMAP: `imap.gmail.com:993`, SSL)
- `outlook` (IMAP: `outlook.office365.com:993`, SSL)
- `imap` / `custom_imap` (explicit host/port)

## Secure secret storage

Mailbox passwords and OAuth tokens are stored in `account_secrets` using existing encryption helpers:

- `encrypt_secret_value(...)`
- `decrypt_secret_value(...)`

New mailbox secret types:

- `mailbox_password`
- `mailbox_oauth_refresh_token`
- `mailbox_oauth_access_token`

Create mailbox credentials via `POST /api/mailboxes`. Password/token material is encrypted at rest and linked by `mailbox_credentials.secret_id`.

## OTP polling behavior

When a checkout task transitions to `payment` with `task_config.otp_required = true`, the task will:

1. Resolve mailbox credentials.
2. Poll IMAP until `otp_timeout_seconds` expires.
3. Filter candidate messages by sender and subject (if configured).
4. Extract OTP using regex (`otp_regex`, default `\b(\d{6})\b`).
5. Store normalized payload under `task_config.consumed_otp`.

If no OTP arrives in time, state becomes `verification_pending`.
If OTP is found, state becomes `verification_code_received`.

## Concrete config examples

### 1) Gmail mailbox credential

```json
{
  "provider": "gmail",
  "email": "buyer@example.com",
  "password": "app-password-here",
  "secret_type": "mailbox_password",
  "poll_interval_seconds": 5,
  "timeout_seconds": 90,
  "sender_filter": "no-reply@target.com",
  "subject_filter": "verification code",
  "otp_regex": "\\b(\\d{6})\\b"
}
```

### 2) Outlook mailbox credential

```json
{
  "provider": "outlook",
  "email": "buyer@outlook.com",
  "password": "outlook-app-password",
  "secret_type": "mailbox_password",
  "poll_interval_seconds": 4,
  "timeout_seconds": 75,
  "sender_filter": "account-security@microsoft.com",
  "subject_filter": "one-time",
  "otp_regex": "code[:\\s]+(\\d{6})"
}
```

### 3) Custom IMAP mailbox credential

```json
{
  "provider": "custom_imap",
  "email": "buyer@corp-mail.example",
  "password": "vault-reference-or-token",
  "secret_type": "mailbox_oauth_access_token",
  "imap_host": "mail.corp.example",
  "imap_port": 993,
  "use_ssl": true,
  "poll_interval_seconds": 3,
  "timeout_seconds": 60,
  "sender_filter": "checkout@bestbuy.com",
  "subject_filter": "security code",
  "otp_regex": "OTP[^0-9]*(\\d{8})"
}
```

### 4) Checkout task config requiring OTP

```json
{
  "monitor_id": 42,
  "task_name": "Target checkout with OTP",
  "task_config": {
    "otp_required": true,
    "mailbox_credential_id": 5,
    "otp_timeout_seconds": 120,
    "otp_poll_interval_seconds": 5,
    "otp_allowed_senders": ["no-reply@target.com"],
    "otp_subject_keywords": ["verification", "security"],
    "otp_regex": "\\b(\\d{6})\\b"
  }
}
```
