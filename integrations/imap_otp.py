from __future__ import annotations

import imaplib
import re
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from email import message_from_bytes
from email.message import Message
from typing import Any


class OTPIntegrationError(RuntimeError):
    def __init__(self, code: str, message: str):
        super().__init__(message)
        self.code = code


@dataclass
class OTPExtractionRule:
    otp_pattern: str
    allowed_senders: tuple[str, ...] = ()
    subject_keywords: tuple[str, ...] = ()

    def compiled_regex(self) -> re.Pattern[str]:
        return re.compile(self.otp_pattern, flags=re.IGNORECASE)


class BaseIMAPOTPAdapter:
    def __init__(self, *, host: str, port: int, username: str, password: str, use_ssl: bool = True) -> None:
        self.host = host
        self.port = port
        self.username = username
        self.password = password
        self.use_ssl = use_ssl

    def _connect(self) -> imaplib.IMAP4 | imaplib.IMAP4_SSL:
        if self.use_ssl:
            client: imaplib.IMAP4 | imaplib.IMAP4_SSL = imaplib.IMAP4_SSL(self.host, self.port)
        else:
            client = imaplib.IMAP4(self.host, self.port)
        client.login(self.username, self.password)
        return client

    def poll_for_otp(
        self,
        *,
        rule: OTPExtractionRule,
        timeout_seconds: int,
        poll_interval_seconds: int,
    ) -> dict[str, Any] | None:
        deadline = time.monotonic() + timeout_seconds
        while time.monotonic() < deadline:
            payload = self._read_latest_otp(rule)
            if payload:
                return payload
            time.sleep(max(poll_interval_seconds, 1))
        return None

    def _read_latest_otp(self, rule: OTPExtractionRule) -> dict[str, Any] | None:
        mail = self._connect()
        try:
            status, _ = mail.select("INBOX")
            if status != "OK":
                raise OTPIntegrationError("OTP_IMAP_SELECT_FAILED", "Unable to select mailbox inbox")
            status, data = mail.search(None, "ALL")
            if status != "OK":
                raise OTPIntegrationError("OTP_IMAP_SEARCH_FAILED", "Unable to search mailbox")
            message_ids = data[0].split()
            for message_id in reversed(message_ids):
                status, raw_message = mail.fetch(message_id, "(RFC822)")
                if status != "OK" or not raw_message or raw_message[0] is None:
                    continue
                msg = message_from_bytes(raw_message[0][1])
                payload = extract_otp_from_message(msg, rule)
                if payload:
                    payload["message_id"] = message_id.decode("ascii", errors="ignore")
                    return payload
            return None
        finally:
            try:
                mail.logout()
            except Exception:
                pass


class GmailOTPAdapter(BaseIMAPOTPAdapter):
    def __init__(self, *, username: str, password: str) -> None:
        super().__init__(host="imap.gmail.com", port=993, username=username, password=password, use_ssl=True)


class OutlookOTPAdapter(BaseIMAPOTPAdapter):
    def __init__(self, *, username: str, password: str) -> None:
        super().__init__(host="outlook.office365.com", port=993, username=username, password=password, use_ssl=True)


class CustomIMAPOTPAdapter(BaseIMAPOTPAdapter):
    pass


def _message_text(msg: Message) -> str:
    if msg.is_multipart():
        parts: list[str] = []
        for part in msg.walk():
            if part.get_content_type() != "text/plain":
                continue
            payload = part.get_payload(decode=True) or b""
            parts.append(payload.decode(part.get_content_charset() or "utf-8", errors="ignore"))
        return "\n".join(parts)
    payload = msg.get_payload(decode=True) or b""
    return payload.decode(msg.get_content_charset() or "utf-8", errors="ignore")


def extract_otp_from_message(msg: Message, rule: OTPExtractionRule) -> dict[str, Any] | None:
    sender = (msg.get("From") or "").strip().lower()
    subject = (msg.get("Subject") or "").strip()
    if rule.allowed_senders and not any(token.lower() in sender for token in rule.allowed_senders):
        return None
    if rule.subject_keywords and not any(token.lower() in subject.lower() for token in rule.subject_keywords):
        return None

    text = _message_text(msg)
    match = rule.compiled_regex().search(text)
    if not match:
        return None

    otp_code = match.group(1) if match.groups() else match.group(0)
    return {
        "code": otp_code.strip(),
        "sender": sender,
        "subject": subject,
        "received_at": datetime.now(timezone.utc).isoformat(),
    }


def poll_for_otp_with_provider(
    *,
    provider: str,
    username: str,
    password: str,
    rule: OTPExtractionRule,
    timeout_seconds: int,
    poll_interval_seconds: int,
    host: str | None = None,
    port: int = 993,
    use_ssl: bool = True,
) -> dict[str, Any] | None:
    provider_key = (provider or "").strip().lower()
    if provider_key == "gmail":
        adapter = GmailOTPAdapter(username=username, password=password)
    elif provider_key == "outlook":
        adapter = OutlookOTPAdapter(username=username, password=password)
    elif provider_key in {"imap", "custom_imap", "custom"}:
        if not host:
            raise OTPIntegrationError("OTP_IMAP_HOST_REQUIRED", "host is required for custom IMAP")
        adapter = CustomIMAPOTPAdapter(host=host, port=port, username=username, password=password, use_ssl=use_ssl)
    else:
        raise OTPIntegrationError("OTP_PROVIDER_UNSUPPORTED", f"Unsupported OTP provider '{provider}'")

    payload = adapter.poll_for_otp(rule=rule, timeout_seconds=timeout_seconds, poll_interval_seconds=poll_interval_seconds)
    if not payload:
        return None
    return {
        "otp_code": payload["code"],
        "sender": payload["sender"],
        "subject": payload["subject"],
        "received_at": payload["received_at"],
        "provider": provider_key,
        "message_id": payload.get("message_id"),
    }
