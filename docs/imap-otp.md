# IMAP / OTP Integration Notes

Stock Sentinel does not currently ingest OTP or email verification codes.

If you add OTP-assisted flows in future modules, document:

- mailbox provider compatibility
- auth method (app password/OAuth)
- polling cadence and timeout policy
- parsing rules for code extraction
- security boundaries (encryption at rest, secret rotation)

## Suggested implementation checklist

1. Create secure credential storage for mailbox secrets.
2. Add provider adapters (Gmail, Outlook, custom IMAP).
3. Add a normalized endpoint for "fetch latest OTP".
4. Log OTP fetch outcomes with sensitive data redacted.
5. Add failure modes into `docs/errors-and-statuses.md`.
