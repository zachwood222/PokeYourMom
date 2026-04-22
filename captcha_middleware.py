from __future__ import annotations

from typing import Callable

import requests
from flask import Request


class CaptchaVerifier:
    def __init__(
        self,
        *,
        secret_key: str,
        verify_url: str,
        timeout_seconds: float,
        logger: Callable[..., None],
    ) -> None:
        self.secret_key = secret_key
        self.verify_url = verify_url
        self.timeout_seconds = timeout_seconds
        self.log = logger

    def extract_token(self, request: Request) -> str:
        header_token = (request.headers.get("X-CAPTCHA-Token") or request.headers.get("X-Captcha-Token") or "").strip()
        if header_token:
            return header_token

        payload = request.get_json(silent=True)
        if isinstance(payload, dict):
            for key in (
                "captcha_token",
                "captchaToken",
                "captcha-response",
                "captchaResponse",
                "cf-turnstile-response",
                "g-recaptcha-response",
            ):
                candidate = payload.get(key)
                if isinstance(candidate, str) and candidate.strip():
                    return candidate.strip()

        for key in ("captcha_token", "cf-turnstile-response", "g-recaptcha-response"):
            candidate = (request.form.get(key) or "").strip()
            if candidate:
                return candidate
        return ""

    def is_captcha_protected_request(self, request: Request) -> bool:
        if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
            return False
        if request.path == "/api/billing/stripe/webhook":
            return False
        if request.path == "/api/internal/checkout/captcha-handoffs/consume":
            return False
        return request.path.startswith("/api/")

    def verify_token(self, token: str, request: Request) -> tuple[bool, str]:
        if not self.secret_key:
            return True, "skipped_not_configured"
        if not token:
            return False, "missing_token"
        payload = {
            "secret": self.secret_key,
            "response": token,
            "remoteip": request.remote_addr,
        }
        try:
            response = requests.post(
                self.verify_url,
                data=payload,
                timeout=self.timeout_seconds,
            )
        except Exception:  # noqa: BLE001
            return False, "provider_unreachable"
        if response.status_code >= 500:
            return False, "provider_error"
        if response.status_code >= 400:
            return False, "provider_rejected"
        try:
            body = response.json()
        except ValueError:
            self.log("CAPTCHA verification provider returned invalid JSON", level="warning")
            return False, "provider_invalid_response"
        if bool(body.get("success")):
            return True, "ok"
        return False, "invalid_token"

    def enforce_or_error(self, request: Request) -> tuple[bool, str | None]:
        if not self.is_captcha_protected_request(request):
            return True, None
        token = self.extract_token(request)
        is_valid, reason = self.verify_token(token, request)
        return is_valid, reason
