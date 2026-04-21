from __future__ import annotations

import base64
import json
import os
import re
import random
import secrets
import sqlite3
import threading
import time
import traceback
import hashlib
import hmac
from datetime import datetime, timezone
from dataclasses import dataclass
from functools import wraps
from typing import Any
from uuid import uuid4

import requests
from flask import Flask, g, has_request_context, jsonify, render_template, request
from flask_socketio import SocketIO
from captcha_middleware import CaptchaVerifier
from checkout_captcha import (
    CaptchaChallengeService,
    ManualFallbackSolveProvider,
    serialize_challenge,
)
from retailers import (
    MonitorResult,
    canonical_retailer,
    default_parser,
    parse_monitor_html,
    resolve_retailer_adapter,
    run_retailer_flow,
)
from tasks.parsers import MonitorInputValidationError, parse_monitor_input

from network.session_manager import RequestResult, SessionManager
from network.session_manager import RequestBehaviorPolicy

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

DB_PATH = os.getenv("DB_PATH", "bot.db")
POLL_LOOP_SECONDS = int(os.getenv("POLL_LOOP_SECONDS", "15"))
WORKER_IDLE_SLEEP_SECONDS = float(os.getenv("WORKER_IDLE_SLEEP_SECONDS", "2.0"))
WORKER_IDLE_SLEEP_JITTER_SECONDS = float(os.getenv("WORKER_IDLE_SLEEP_JITTER_SECONDS", "0.75"))
WORKER_ACTIVE_JITTER_SECONDS = float(os.getenv("WORKER_ACTIVE_JITTER_SECONDS", "0.2"))
WORKER_LOCK_TIMEOUT_SECONDS = int(os.getenv("WORKER_LOCK_TIMEOUT_SECONDS", "60"))
WORKER_ID = os.getenv("WORKER_ID", f"worker-{uuid4()}")
ACCOUNT_START_DELAY_MIN_SECONDS = int(os.getenv("ACCOUNT_START_DELAY_MIN_SECONDS", "1"))
ACCOUNT_START_DELAY_MAX_SECONDS = int(os.getenv("ACCOUNT_START_DELAY_MAX_SECONDS", "8"))
APP_ROLE = os.getenv("APP_ROLE", "api").lower()
ENABLE_EMBEDDED_WORKER = os.getenv("ENABLE_EMBEDDED_WORKER", "0") == "1"
DEFAULT_PLAN = os.getenv("DEFAULT_PLAN", "basic")
POKEMON_MSRP_BUFFER_CENTS = int(os.getenv("POKEMON_MSRP_BUFFER_CENTS", "1000"))
APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
RELEASE_CHANNEL = os.getenv("RELEASE_CHANNEL", "stable")
CORRELATION_ID_HEADER = "X-Correlation-ID"
_api_auth_token_raw = os.getenv("API_AUTH_TOKEN")
API_AUTH_TOKEN = _api_auth_token_raw.strip() if _api_auth_token_raw is not None else "dev-token"
SECRET_ENCRYPTION_KEY = os.getenv("SECRET_ENCRYPTION_KEY", "local-dev-secret-key")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
UPDATE_CHECK_URL = os.getenv("UPDATE_CHECK_URL", "")
UPDATE_CHECK_TIMEOUT_SECONDS = float(os.getenv("UPDATE_CHECK_TIMEOUT_SECONDS", "2.0"))
CAPTCHA_PROVIDER = os.getenv("CAPTCHA_PROVIDER", "turnstile")
CAPTCHA_SITE_KEY = os.getenv("CAPTCHA_SITE_KEY", "")
CAPTCHA_SCRIPT_URL = os.getenv("CAPTCHA_SCRIPT_URL", "https://challenges.cloudflare.com/turnstile/v0/api.js")
CAPTCHA_SECRET_KEY = os.getenv("CAPTCHA_SECRET_KEY", "")
DEFAULT_CAPTCHA_VERIFY_URL = "https://www.google.com/recaptcha/api/siteverify"
CAPTCHA_VERIFY_URL = os.getenv(
    "CAPTCHA_VERIFY_URL",
    DEFAULT_CAPTCHA_VERIFY_URL,
)
CAPTCHA_VERIFY_TIMEOUT_SECONDS = float(os.getenv("CAPTCHA_VERIFY_TIMEOUT_SECONDS", "2.0"))
TASK_STEP_DELAY_SECONDS = float(os.getenv("TASK_STEP_DELAY_SECONDS", "0.5"))
QUEUE_ENQUEUE_JITTER_SECONDS = float(os.getenv("QUEUE_ENQUEUE_JITTER_SECONDS", "1.25"))
STRICT_API_AUTH_TOKEN = (os.getenv("STRICT_API_AUTH_TOKEN", "1") or "").strip().lower() not in {"0", "false", "no", "off"}
CHECKOUT_CAPTCHA_SOLVE_PROVIDER = os.getenv("CHECKOUT_CAPTCHA_SOLVE_PROVIDER", "manual")

PLAN_LIMITS = {
    "basic": {"max_monitors": 20, "min_poll_seconds": 20},
    "pro": {"max_monitors": 100, "min_poll_seconds": 10},
    "team": {"max_monitors": 500, "min_poll_seconds": 5},
}
PLAN_LOOKUP_TO_INTERNAL_PLAN = {
    "basic": "basic",
    "pro": "pro",
    "team": "team",
}
SUPPORTED_RETAILERS = {"walmart", "target", "bestbuy", "pokemoncenter"}
SUPPORTED_MONITOR_CATEGORIES = {"pokemon", "sports_cards", "one_piece", "lorcana"}
RETAILER_CATEGORY_SUPPORT = {
    "target": SUPPORTED_MONITOR_CATEGORIES,
    "pokemoncenter": SUPPORTED_MONITOR_CATEGORIES,
    "walmart": {"pokemon"},
    "bestbuy": {"pokemon"},
}
POKEMON_CENTER_TASK_GROUP_SCHEMA_VERSION = 3
POKEMON_CENTER_SITES = {"us", "ca", "uk"}
POKEMON_CENTER_MODES = {"default", "create_account", "newsletter_subscribe"}
POKEMON_CENTER_MODE_DESCRIPTIONS = {
    "default": "Standard checkout flow with monitor, queue wait, shipping, and payment.",
    "create_account": "Create a new account profile and subscribe it to newsletters.",
    "newsletter_subscribe": "Subscribe an existing account to newsletters only.",
}
POKEMON_CENTER_MODE_SITE_SUPPORT = {
    "default": {"us", "ca", "uk"},
    "create_account": {"us"},
    "newsletter_subscribe": {"us"},
}
POKEMON_CENTER_REQUIRED_FIELDS_BY_MODE = {
    "default": set(),
    "create_account": {"profile_email", "profile_first_name", "profile_last_name", "account_output_target"},
    "newsletter_subscribe": {"existing_account_source"},
}
POKEMON_CENTER_DEFAULT_TASK_FIELDS = {
    "site": "us",
    "mode": "default",
    "monitor_input": "",
    "product_quantity": 1,
    "monitor_delay_ms": 3500,
    "queue_entry_delay_ms": None,
    "discount_code": None,
    "wait_for_queue": False,
    "loop_checkout": False,
    "group_limits": {
        "max_retries": None,
        "antibot_event_threshold": 3,
        "antibot_cooldown_seconds": 60,
    },
}

POKEMON_CENTER_CREATE_ACCOUNT_FIELDS = (
    "profile_email",
    "profile_first_name",
    "profile_last_name",
    "account_output_target",
)
POKEMON_CENTER_NEWSLETTER_FIELDS = ("existing_account_source",)
DEFAULT_WORKSPACE = {
    "name": "My Workspace",
    "plan": DEFAULT_PLAN if DEFAULT_PLAN in PLAN_LIMITS else "basic",
}
DEFAULT_USER_EMAIL = os.getenv("DEFAULT_USER_EMAIL", "owner@local.test")
DEFAULT_USER_NAME = os.getenv("DEFAULT_USER_NAME", "Workspace Owner")
DEFAULT_BEARER_TOKEN = os.getenv("DEFAULT_BEARER_TOKEN", "dev-token")

DEFAULT_USER = {
    "id": "local-dev",
    "email": "local-dev@example.com",
    "name": "Local Developer",
}

worker_running = False
worker_thread: threading.Thread | None = None
worker_lock = threading.Lock()

CHECKOUT_TASK_STATES = {
    "idle",
    "starting",
    "waiting_for_queue",
    "solving_hcaptcha",
    "in_queue",
    "passed_queue",
    "waiting_for_monitor_input",
    "monitoring_product",
    "adding_to_cart",
    "checking_out",
    "success",
    "decline",
    "requeued",
    "antibot_datadome",
    "antibot_incapsula",
    "error",
    "paused",
    "stopped",
}

CHECKOUT_TERMINAL_STATES = {"success", "decline", "error", "stopped"}
CHECKOUT_ACTIVE_STATES = CHECKOUT_TASK_STATES - CHECKOUT_TERMINAL_STATES - {"paused", "idle"}

CHECKOUT_STEP_SEQUENCE = ["monitoring_product", "adding_to_cart", "checking_out"]
CHECKOUT_STEP_RETRY_POLICY = {
    "monitoring_product": {"max_attempts": 3},
    "adding_to_cart": {"max_attempts": 3},
    "checking_out": {"max_attempts": 3},
}

TASK_STATUS_LABELS = {
    "idle": "Idle",
    "starting": "Starting",
    "waiting_for_queue": "Queue Wait",
    "solving_hcaptcha": "Captcha",
    "in_queue": "In Queue",
    "passed_queue": "Queue Passed",
    "waiting_for_monitor_input": "Input Wait",
    "monitoring_product": "Monitoring",
    "adding_to_cart": "Adding to Cart",
    "checking_out": "Checking Out",
    "success": "Success",
    "decline": "Declined",
    "requeued": "Requeued",
    "antibot_datadome": "DataDome",
    "antibot_incapsula": "Incapsula",
    "error": "Error",
    "paused": "Paused",
    "stopped": "Stopped",
}
CHECKOUT_RETRY_PRESETS = {
    "antibot": {"max_attempts": 4, "base_backoff_seconds": 1.5},
    "network": {"max_attempts": 3, "base_backoff_seconds": 0.5},
    "decline": {"max_attempts": 1, "base_backoff_seconds": 0.0},
    "other": {"max_attempts": 2, "base_backoff_seconds": 0.25},
}

MAILBOX_SECRET_TYPES = {"mailbox_password", "mailbox_oauth_refresh_token", "mailbox_oauth_access_token"}
OTP_ERROR_TIMEOUT = "OTP_TIMEOUT"
OTP_ERROR_PROVIDER = "OTP_PROVIDER_ERROR"
OTP_ERROR_CONFIG = "OTP_CONFIG_ERROR"


@dataclass
class Job:
    id: int
    job_type: str
    monitor_id: int | None
    status: str
    attempt_count: int
    next_run_at: str
    locked_by: str | None
    locked_at: str | None
    payload_json: str | None
    last_error: str | None = None
    created_at: str | None = None
    updated_at: str | None = None


@dataclass(frozen=True)
class ProxyLease:
    lease_id: int
    proxy_id: int
    endpoint: str
    lease_key: str
    owner_type: str
    owner_id: int | None


def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def parse_json_object(raw: str | None) -> dict[str, Any]:
    if not raw:
        return {}
    try:
        parsed = json.loads(raw)
    except (TypeError, json.JSONDecodeError):
        return {}
    return parsed if isinstance(parsed, dict) else {}


def is_dev_environment() -> bool:
    mode = (
        os.getenv("FLASK_ENV")
        or os.getenv("APP_ENV")
        or os.getenv("ENV")
        or os.getenv("PYTHON_ENV")
        or ""
    ).strip().lower()
    if mode in {"dev", "development", "local", "test", "testing"}:
        return True
    return (os.getenv("FLASK_DEBUG", "0") or "").strip() == "1"


def validate_startup_configuration() -> None:
    if API_AUTH_TOKEN:
        return
    warning_message = (
        "API_AUTH_TOKEN is empty after normalization. /api/* endpoints will return 401 "
        "unless requests include a valid bearer token."
    )
    log(warning_message, level="error")
    if STRICT_API_AUTH_TOKEN and not is_dev_environment():
        raise RuntimeError(
            f"{warning_message} Set API_AUTH_TOKEN to a non-empty value before startup."
        )


def verify_stripe_webhook_signature(payload: bytes, signature_header: str | None) -> None:
    if not STRIPE_WEBHOOK_SECRET or not signature_header:
        raise PermissionError("Missing Stripe webhook secret or signature")
    parts = {}
    for item in signature_header.split(","):
        if "=" not in item:
            continue
        key, value = item.split("=", 1)
        parts.setdefault(key.strip(), []).append(value.strip())
    timestamp_raw = (parts.get("t") or [None])[0]
    signatures = parts.get("v1") or []
    if not timestamp_raw or not signatures:
        raise PermissionError("Malformed Stripe-Signature header")
    try:
        timestamp = int(timestamp_raw)
    except ValueError as exc:
        raise PermissionError("Invalid Stripe signature timestamp") from exc
    if abs(time.time() - timestamp) > 300:
        raise PermissionError("Stripe signature timestamp outside tolerance")
    signed_payload = f"{timestamp}.{payload.decode('utf-8')}".encode("utf-8")
    expected_signature = hmac.new(
        STRIPE_WEBHOOK_SECRET.encode("utf-8"),
        signed_payload,
        hashlib.sha256,
    ).hexdigest()
    if not any(hmac.compare_digest(expected_signature, candidate) for candidate in signatures):
        raise PermissionError("Invalid Stripe signature")


def _encryption_keystream(secret_key: str, nonce: bytes, length: int) -> bytes:
    stream = bytearray()
    counter = 0
    key_bytes = secret_key.encode("utf-8")
    while len(stream) < length:
        block = hashlib.sha256(key_bytes + nonce + counter.to_bytes(8, "big")).digest()
        stream.extend(block)
        counter += 1
    return bytes(stream[:length])


def encrypt_secret_value(plaintext: str) -> str:
    nonce = secrets.token_bytes(16)
    payload = plaintext.encode("utf-8")
    keystream = _encryption_keystream(SECRET_ENCRYPTION_KEY, nonce, len(payload))
    cipher = bytes(a ^ b for a, b in zip(payload, keystream))
    mac = hmac.new(SECRET_ENCRYPTION_KEY.encode("utf-8"), nonce + cipher, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(nonce + cipher + mac).decode("ascii")


def decrypt_secret_value(ciphertext: str) -> str:
    raw = base64.urlsafe_b64decode(ciphertext.encode("ascii"))
    if len(raw) < 48:
        raise ValueError("Invalid secret payload")
    nonce = raw[:16]
    mac = raw[-32:]
    cipher = raw[16:-32]
    expected_mac = hmac.new(SECRET_ENCRYPTION_KEY.encode("utf-8"), nonce + cipher, hashlib.sha256).digest()
    if not hmac.compare_digest(mac, expected_mac):
        raise ValueError("Secret integrity check failed")
    keystream = _encryption_keystream(SECRET_ENCRYPTION_KEY, nonce, len(cipher))
    payload = bytes(a ^ b for a, b in zip(cipher, keystream))
    return payload.decode("utf-8")


SENSITIVE_FIELD_MARKERS = ("token", "secret", "password", "authorization", "webhook_url")


def redact_sensitive_payload(value: Any) -> Any:
    if isinstance(value, dict):
        out: dict[str, Any] = {}
        for key, item in value.items():
            lower = key.lower()
            if any(marker in lower for marker in SENSITIVE_FIELD_MARKERS):
                out[key] = "[redacted]"
            else:
                out[key] = redact_sensitive_payload(item)
        return out
    if isinstance(value, list):
        return [redact_sensitive_payload(item) for item in value]
    return value


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    normalized_type = (secret_type or "").strip().lower()
    if not normalized_type:
        raise ValueError("secret_type is required")
    ciphertext = encrypt_secret_value(plaintext)
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, normalized_type, ciphertext, now_iso, now_iso),
    )
    return int(cur.lastrowid)


def get_secret_plaintext(
    conn: sqlite3.Connection,
    *,
    workspace_id: int,
    secret_id: int,
    allowed_types: set[str] | None = None,
) -> str:
    row = conn.execute(
        "select * from account_secrets where id = ? and workspace_id = ?",
        (secret_id, workspace_id),
    ).fetchone()
    if not row:
        raise ValueError("Secret not found")
    if allowed_types and row["secret_type"] not in allowed_types:
        raise ValueError("Secret type not allowed")
    return decrypt_secret_value(row["ciphertext"])


def redact_webhook_url(url: str) -> str:
    value = (url or "").strip()
    if len(value) <= 20:
        return "[redacted]"
    return f"{value[:18]}...{value[-6:]}"


def resolve_webhook_url(conn: sqlite3.Connection, webhook_row: sqlite3.Row) -> str:
    secret_id = webhook_row["webhook_secret_id"]
    if not secret_id:
        return webhook_row["webhook_url"]
    return get_secret_plaintext(
        conn,
        workspace_id=webhook_row["workspace_id"],
        secret_id=int(secret_id),
        allowed_types={"webhook_url"},
    )


def resolve_mailbox_credential(conn: sqlite3.Connection, workspace_id: int, credential_id: int) -> dict[str, Any] | None:
    row = conn.execute(
        "select * from mailbox_credentials where id = ? and workspace_id = ?",
        (credential_id, workspace_id),
    ).fetchone()
    if not row:
        return None
    payload = dict(row)
    payload["password"] = get_secret_plaintext(
        conn,
        workspace_id=workspace_id,
        secret_id=int(row["secret_id"]),
        allowed_types=MAILBOX_SECRET_TYPES,
    )
    return payload


def await_and_consume_checkout_otp(
    conn: sqlite3.Connection,
    *,
    workspace_id: int,
    task_row: sqlite3.Row,
) -> tuple[dict[str, Any] | None, str | None]:
    config = json.loads(task_row["task_config"] or "{}")
    if not config.get("otp_required"):
        return None, None

    mailbox_credential_id = config.get("mailbox_credential_id")
    if not mailbox_credential_id:
        return None, f"{OTP_ERROR_CONFIG}: mailbox_credential_id is required when otp_required=true"

    credential = resolve_mailbox_credential(conn, workspace_id, int(mailbox_credential_id))
    if credential is None:
        return None, f"{OTP_ERROR_CONFIG}: mailbox credential not found"

    try:
        rule = OTPExtractionRule(
            otp_pattern=config.get("otp_regex") or credential.get("otp_regex") or r"\b(\d{6})\b",
            allowed_senders=tuple(config.get("otp_allowed_senders") or ((credential.get("sender_filter") or "").split(",") if credential.get("sender_filter") else [])),
            subject_keywords=tuple(config.get("otp_subject_keywords") or ((credential.get("subject_filter") or "").split(",") if credential.get("subject_filter") else [])),
        )
        payload = poll_for_otp_with_provider(
            provider=credential["provider"],
            username=credential["email"],
            password=credential["password"],
            host=credential.get("imap_host"),
            port=int(credential.get("imap_port") or 993),
            use_ssl=bool(credential.get("use_ssl", 1)),
            rule=rule,
            timeout_seconds=int(config.get("otp_timeout_seconds") or credential.get("timeout_seconds") or 90),
            poll_interval_seconds=int(config.get("otp_poll_interval_seconds") or credential.get("poll_interval_seconds") or 5),
        )
    except OTPIntegrationError as exc:
        return None, f"{OTP_ERROR_PROVIDER}:{exc.code}:{exc}"

    if not payload:
        return None, f"{OTP_ERROR_TIMEOUT}: OTP was not received within timeout"

    config["consumed_otp"] = payload
    conn.execute(
        "update checkout_tasks set task_config = ?, updated_at = ? where id = ? and workspace_id = ?",
        (json.dumps(config), utc_now(), task_row["id"], workspace_id),
    )
    return payload, None


def _workspace_id_from_subscription_object(subscription: dict[str, Any]) -> int | None:
    metadata = subscription.get("metadata") or {}
    raw_workspace_id = metadata.get("workspace_id")
    if raw_workspace_id is None:
        return None
    try:
        return int(raw_workspace_id)
    except (TypeError, ValueError):
        return None


def _iso_from_unix_timestamp(value: Any) -> str | None:
    if value is None:
        return None
    try:
        return datetime.fromtimestamp(int(value), tz=timezone.utc).isoformat()
    except (TypeError, ValueError, OSError):
        return None


def _resolve_workspace_user_id(conn: sqlite3.Connection, workspace_id: int) -> int | None:
    row = conn.execute(
        """
        select user_id
        from workspace_members
        where workspace_id = ?
        order by case when role = 'owner' then 0 else 1 end, id asc
        limit 1
        """,
        (workspace_id,),
    ).fetchone()
    return int(row["user_id"]) if row else None


def sync_billing_subscription_event(conn: sqlite3.Connection, event: dict[str, Any]) -> None:
    event_type = event.get("type", "")
    subscription = ((event.get("data") or {}).get("object") or {})
    if not isinstance(subscription, dict):
        return
    workspace_id = _workspace_id_from_subscription_object(subscription)
    if workspace_id is None:
        return
    provider_customer_id = subscription.get("customer")
    billing_customer_id = None
    if isinstance(provider_customer_id, str) and provider_customer_id:
        user_id = _resolve_workspace_user_id(conn, workspace_id)
        if user_id is not None:
            now_iso = utc_now()
            conn.execute(
                """
                insert into billing_customers(workspace_id, user_id, provider, provider_customer_id, created_at, updated_at)
                values (?, ?, 'stripe', ?, ?, ?)
                on conflict(workspace_id, user_id)
                do update set provider_customer_id = excluded.provider_customer_id, updated_at = excluded.updated_at
                """,
                (workspace_id, user_id, provider_customer_id, now_iso, now_iso),
            )
            customer_row = conn.execute(
                """
                select id from billing_customers
                where workspace_id = ? and user_id = ?
                """,
                (workspace_id, user_id),
            ).fetchone()
            billing_customer_id = int(customer_row["id"]) if customer_row else None

    status = subscription.get("status") or "incomplete"
    if event_type == "customer.subscription.deleted":
        status = "canceled"
    cancel_at_period_end = int(bool(subscription.get("cancel_at_period_end", False)))
    if event_type == "customer.subscription.deleted":
        cancel_at_period_end = 1

    plan_obj = subscription.get("plan") or {}
    now_iso = utc_now()
    conn.execute(
        """
        insert into billing_subscriptions(
            workspace_id,
            provider,
            provider_subscription_id,
            billing_customer_id,
            status,
            current_period_end,
            cancel_at_period_end,
            plan_code,
            plan_interval,
            plan_lookup_key,
            created_at,
            updated_at
        )
        values (?, 'stripe', ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(workspace_id)
        do update set
            provider_subscription_id = excluded.provider_subscription_id,
            billing_customer_id = coalesce(excluded.billing_customer_id, billing_subscriptions.billing_customer_id),
            status = excluded.status,
            current_period_end = excluded.current_period_end,
            cancel_at_period_end = excluded.cancel_at_period_end,
            plan_code = excluded.plan_code,
            plan_interval = excluded.plan_interval,
            plan_lookup_key = excluded.plan_lookup_key,
            updated_at = excluded.updated_at
        """,
        (
            workspace_id,
            subscription.get("id"),
            billing_customer_id,
            status,
            _iso_from_unix_timestamp(subscription.get("current_period_end")),
            cancel_at_period_end,
            plan_obj.get("id"),
            plan_obj.get("interval"),
            (subscription.get("items") or {}).get("data", [{}])[0].get("price", {}).get("lookup_key")
            if isinstance((subscription.get("items") or {}).get("data"), list)
            and (subscription.get("items") or {}).get("data")
            else None,
            now_iso,
            now_iso,
        ),
    )


def format_log_entry(
    level: str,
    message: str,
    workspace_id: int | None = None,
    monitor_id: int | None = None,
    correlation_id: str | None = None,
) -> dict[str, Any]:
    inferred_workspace_id = workspace_id
    inferred_correlation_id = correlation_id
    if inferred_workspace_id is None and has_request_context():
        inferred_workspace_id = getattr(g, "workspace_id", None)
    if inferred_correlation_id is None and has_request_context():
        inferred_correlation_id = getattr(g, "correlation_id", None)
    return {
        "timestamp": utc_now(),
        "level": level.lower(),
        "message": message,
        "workspace_id": inferred_workspace_id,
        "monitor_id": monitor_id,
        "correlation_id": inferred_correlation_id,
    }


def log(
    message: str,
    *,
    level: str = "info",
    workspace_id: int | None = None,
    monitor_id: int | None = None,
    correlation_id: str | None = None,
) -> None:
    sanitized_message = re.sub(
        r"https://discord\.com/api/webhooks/[^\s]+",
        "https://discord.com/api/webhooks/***redacted***",
        message,
    )
    entry = format_log_entry(
        level=level,
        message=sanitized_message,
        workspace_id=workspace_id,
        monitor_id=monitor_id,
        correlation_id=correlation_id,
    )
    print(json.dumps(entry, sort_keys=True))
    socketio.emit("log", entry)


captcha_verifier = CaptchaVerifier(
    secret_key=CAPTCHA_SECRET_KEY,
    verify_url=CAPTCHA_VERIFY_URL,
    timeout_seconds=CAPTCHA_VERIFY_TIMEOUT_SECONDS,
    logger=log,
)
checkout_captcha_service = CaptchaChallengeService(now_fn=utc_now)
checkout_solve_provider = ManualFallbackSolveProvider()


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    columns = {r["name"] for r in conn.execute(f"pragma table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"alter table {table} add column {column} {ddl}")


def table_exists(conn: sqlite3.Connection, table: str) -> bool:
    row = conn.execute(
        "select 1 from sqlite_master where type = 'table' and name = ?",
        (table,),
    ).fetchone()
    return row is not None


def normalize_legacy_task_state(raw_state: Any) -> str:
    state = str(raw_state or "").strip().lower()
    compat_map = {
        "queued": "idle",
        "idle": "idle",
        "running": "monitoring_product",
        "monitoring": "monitoring_product",
        "carting": "adding_to_cart",
        "shipping": "checking_out",
        "payment": "checking_out",
        "submitting": "checking_out",
        "complete": "success",
        "completed": "success",
        "failed": "decline",
        "cancelled": "stopped",
        "canceled": "stopped",
    }
    state = compat_map.get(state, state or "idle")
    if state not in CHECKOUT_TASK_STATES:
        return "idle"
    return state


def _coerce_optional_int(value: Any) -> int | None:
    if value is None or value == "":
        return None
    try:
        parsed = int(value)
    except (TypeError, ValueError):
        return None
    return parsed if parsed >= 0 else None


def _is_pokemon_center_task_config(task_config: dict[str, Any], monitor_row: sqlite3.Row | None = None) -> bool:
    monitor_retailer = (monitor_row["retailer"] if monitor_row else "") or ""
    return (
        str(task_config.get("retailer") or "").strip().lower() == "pokemoncenter"
        or str(monitor_retailer).strip().lower() == "pokemoncenter"
    )


def _pokemon_center_mode(config: dict[str, Any]) -> str:
    mode = str(config.get("mode") or POKEMON_CENTER_DEFAULT_TASK_FIELDS["mode"]).strip().lower()
    return mode if mode in POKEMON_CENTER_MODES else POKEMON_CENTER_DEFAULT_TASK_FIELDS["mode"]


def validate_pokemon_center_mode_site(mode: str, site: str) -> str | None:
    supported_sites = POKEMON_CENTER_MODE_SITE_SUPPORT.get(mode, POKEMON_CENTER_MODE_SITE_SUPPORT["default"])
    if site not in supported_sites:
        return f"Unsupported site '{site}' for mode '{mode}'"
    return None


def validate_pokemon_center_mode_requirements(config: dict[str, Any]) -> str | None:
    mode = _pokemon_center_mode(config)
    missing_fields = []
    for field in sorted(POKEMON_CENTER_REQUIRED_FIELDS_BY_MODE.get(mode, set())):
        value = config.get(field)
        if value is None or str(value).strip() == "":
            missing_fields.append(field)
    if missing_fields:
        return f"Missing required fields for mode '{mode}': {', '.join(missing_fields)}"
    return None


def normalize_task_config_for_monitor(
    task_config: dict[str, Any] | None,
    *,
    monitor_row: sqlite3.Row | None = None,
) -> dict[str, Any]:
    normalized = dict(task_config or {})
    if monitor_row and not normalized.get("retailer"):
        normalized["retailer"] = monitor_row["retailer"]
    if monitor_row and not normalized.get("product_url"):
        normalized["product_url"] = monitor_row["product_url"]
    if not _is_pokemon_center_task_config(normalized, monitor_row):
        return normalized

    site = str(normalized.get("site") or POKEMON_CENTER_DEFAULT_TASK_FIELDS["site"]).strip().lower()
    normalized["site"] = site if site in POKEMON_CENTER_SITES else POKEMON_CENTER_DEFAULT_TASK_FIELDS["site"]

    mode = str(normalized.get("mode") or POKEMON_CENTER_DEFAULT_TASK_FIELDS["mode"]).strip().lower()
    normalized["mode"] = mode if mode in POKEMON_CENTER_MODES else POKEMON_CENTER_DEFAULT_TASK_FIELDS["mode"]

    monitor_input = normalized.get("monitor_input")
    normalized["monitor_input"] = str(monitor_input).strip() if monitor_input is not None else ""

    quantity = _coerce_optional_int(normalized.get("product_quantity"))
    normalized["product_quantity"] = quantity if quantity and quantity > 0 else POKEMON_CENTER_DEFAULT_TASK_FIELDS["product_quantity"]

    monitor_delay = _coerce_optional_int(normalized.get("monitor_delay_ms"))
    normalized["monitor_delay_ms"] = (
        monitor_delay if monitor_delay is not None else POKEMON_CENTER_DEFAULT_TASK_FIELDS["monitor_delay_ms"]
    )
    normalized["queue_entry_delay_ms"] = _coerce_optional_int(normalized.get("queue_entry_delay_ms"))

    discount = normalized.get("discount_code")
    discount_normalized = str(discount).strip() if discount is not None else ""
    normalized["discount_code"] = discount_normalized or None

    normalized["wait_for_queue"] = bool(normalized.get("wait_for_queue", POKEMON_CENTER_DEFAULT_TASK_FIELDS["wait_for_queue"]))
    normalized["loop_checkout"] = bool(normalized.get("loop_checkout", POKEMON_CENTER_DEFAULT_TASK_FIELDS["loop_checkout"]))
    raw_limits = normalized.get("group_limits") if isinstance(normalized.get("group_limits"), dict) else {}
    default_limits = POKEMON_CENTER_DEFAULT_TASK_FIELDS["group_limits"]
    max_retries = _coerce_optional_int(raw_limits.get("max_retries"))
    antibot_threshold = _coerce_optional_int(raw_limits.get("antibot_event_threshold"))
    antibot_cooldown_seconds = _coerce_optional_int(raw_limits.get("antibot_cooldown_seconds"))
    normalized["group_limits"] = {
        "max_retries": max_retries,
        "antibot_event_threshold": antibot_threshold if antibot_threshold and antibot_threshold > 0 else default_limits["antibot_event_threshold"],
        "antibot_cooldown_seconds": (
            antibot_cooldown_seconds
            if antibot_cooldown_seconds is not None
            else default_limits["antibot_cooldown_seconds"]
        ),
    }

    for field_name in (*POKEMON_CENTER_CREATE_ACCOUNT_FIELDS, *POKEMON_CENTER_NEWSLETTER_FIELDS):
        value = normalized.get(field_name)
        normalized[field_name] = str(value).strip() if value is not None else ""

    mode = normalized["mode"]
    if mode == "create_account":
        normalized["existing_account_source"] = ""
        normalized["profile"] = None
        normalized["payment"] = None
    elif mode == "newsletter_subscribe":
        normalized["profile_email"] = ""
        normalized["profile_first_name"] = ""
        normalized["profile_last_name"] = ""
        normalized["account_output_target"] = ""
        normalized["profile"] = None
        normalized["payment"] = None

    products = normalized.get("products")
    if isinstance(products, list):
        patched_products = []
        for product in products:
            if isinstance(product, dict):
                copy = dict(product)
                copy["skip_if_oos"] = bool(copy.get("skip_if_oos", False))
                patched_products.append(copy)
            else:
                patched_products.append(product)
        normalized["products"] = patched_products

    normalized["task_group_version"] = POKEMON_CENTER_TASK_GROUP_SCHEMA_VERSION
    return normalized


RUNNING_TASK_STATES = {"queued", "monitoring", "carting", "shipping", "payment", "submitting", "paused"}


def _is_quick_edit_input(raw_input: str) -> bool:
    text = (raw_input or "").strip()
    if not text:
        return False
    segments = [segment.strip() for segment in text.split(",")]
    return len(segments) > 1 or any(":" in segment for segment in segments)


def _coerce_product_rows(value: Any) -> list[dict[str, Any]]:
    rows = value if isinstance(value, list) else []
    normalized_rows: list[dict[str, Any]] = []
    for row in rows:
        if not isinstance(row, dict):
            continue
        pid = str(row.get("pid") or "").strip()
        if not pid:
            continue
        quantity = _coerce_optional_int(row.get("quantity"))
        normalized_rows.append(
            {
                "pid": pid,
                "quantity": quantity if quantity and quantity > 0 else 1,
                "skip_if_oos": bool(row.get("skip_if_oos", False)),
            }
        )
    return normalized_rows


def apply_product_group_operation(task_config: dict[str, Any], operation: dict[str, Any]) -> dict[str, Any]:
    config = dict(task_config or {})
    existing_products = _coerce_product_rows(config.get("products"))
    op = str(operation.get("mode") or "").strip().lower()
    if op not in {"edit", "add", "remove"}:
        raise ValueError("mode must be one of: edit, add, remove")

    updated_products = list(existing_products)
    if op in {"edit", "add"}:
        raw_input = str(operation.get("input") or "").strip()
        if not raw_input:
            raise ValueError("input is required for edit/add operations")
        quick_edit = _is_quick_edit_input(raw_input)
        parsed = parse_monitor_input(
            raw_input,
            is_edit_flow=True,
            existing_product_count=1 if op == "add" else len(existing_products),
        )
        if op == "edit":
            if quick_edit and len(existing_products) != 1:
                raise ValueError("Quick Edit is only allowed when editing a task with exactly one product.")
            if not updated_products:
                updated_products = [parsed[0]]
            elif quick_edit:
                updated_products = [parsed[0], *parsed[1:]]
            else:
                updated_products[0] = parsed[0]
        else:
            updated_products.extend(parsed)

    if op == "remove":
        indices = operation.get("remove_indices")
        if not isinstance(indices, list) or not indices:
            raise ValueError("remove_indices must include at least one row index")
        valid_indices = {idx for idx in indices if isinstance(idx, int) and idx >= 0}
        updated_products = [row for idx, row in enumerate(updated_products) if idx not in valid_indices]

    skip_updates = operation.get("skip_updates")
    if isinstance(skip_updates, list):
        for patch in skip_updates:
            if not isinstance(patch, dict):
                continue
            idx = patch.get("index")
            if not isinstance(idx, int) or idx < 0 or idx >= len(updated_products):
                continue
            updated_products[idx]["skip_if_oos"] = bool(patch.get("skip_if_oos"))

    config["products"] = updated_products
    return normalize_task_config_for_monitor(config)


def run_pokemon_center_task_group_config_migration(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists schema_migrations (
            key text primary key,
            applied_at text not null
        )
        """
    )
    migration_key = "2026_04_21_checkout_task_group_pokemoncenter_defaults_v3"
    already_applied = conn.execute(
        "select 1 from schema_migrations where key = ?",
        (migration_key,),
    ).fetchone()
    if already_applied:
        return

    rows = conn.execute(
        """
        select ct.id, ct.task_config, m.retailer, m.product_url
        from checkout_tasks ct
        join monitors m on m.id = ct.monitor_id
        where m.retailer = 'pokemoncenter'
        """
    ).fetchall()
    for row in rows:
        raw_config = row["task_config"] or "{}"
        try:
            parsed_config = json.loads(raw_config)
        except (TypeError, json.JSONDecodeError):
            parsed_config = {}
        normalized = normalize_task_config_for_monitor(
            parsed_config if isinstance(parsed_config, dict) else {},
            monitor_row=row,
        )
        conn.execute(
            "update checkout_tasks set task_config = ?, updated_at = ? where id = ?",
            (json.dumps(normalized), utc_now(), row["id"]),
        )

    conn.execute(
        "insert into schema_migrations(key, applied_at) values (?, ?)",
        (migration_key, utc_now()),
    )


def run_legacy_tasks_migration(conn: sqlite3.Connection) -> None:
    conn.execute(
        """
        create table if not exists schema_migrations (
            key text primary key,
            applied_at text not null
        )
        """
    )
    migration_key = "2026_04_20_legacy_tasks_to_checkout_tasks"
    already_applied = conn.execute(
        "select 1 from schema_migrations where key = ?",
        (migration_key,),
    ).fetchone()
    if already_applied:
        return

    if not table_exists(conn, "tasks"):
        conn.execute(
            "insert into schema_migrations(key, applied_at) values (?, ?)",
            (migration_key, utc_now()),
        )
        return

    task_columns = {row["name"] for row in conn.execute("pragma table_info(tasks)").fetchall()}
    if "id" not in task_columns:
        conn.execute(
            "insert into schema_migrations(key, applied_at) values (?, ?)",
            (migration_key, utc_now()),
        )
        return

    ensure_column(conn, "checkout_tasks", "legacy_task_id", "integer")
    conn.execute(
        """
        create unique index if not exists idx_checkout_tasks_legacy_task_id
        on checkout_tasks(legacy_task_id)
        where legacy_task_id is not null
        """
    )

    legacy_tasks = conn.execute("select * from tasks order by id asc").fetchall()
    migrated_count = 0
    for row in legacy_tasks:
        legacy_task_id = int(row["id"])
        existing = conn.execute(
            "select id from checkout_tasks where legacy_task_id = ?",
            (legacy_task_id,),
        ).fetchone()
        if existing:
            continue

        workspace_id = int(row["workspace_id"]) if "workspace_id" in task_columns and row["workspace_id"] else 1
        retailer = (row["retailer"] if "retailer" in task_columns else None) or "walmart"
        product_url = (
            (row["product_url"] if "product_url" in task_columns else None)
            or (row["url"] if "url" in task_columns else None)
            or ""
        )
        profile = (row["profile"] if "profile" in task_columns else None) or ""
        account = (row["account"] if "account" in task_columns else None) or ""
        payment = (row["payment"] if "payment" in task_columns else None) or ""
        normalized_state = normalize_legacy_task_state(row["state"] if "state" in task_columns else None)
        last_error = (row["last_error"] if "last_error" in task_columns else None) or None
        created_at = (row["created_at"] if "created_at" in task_columns else None) or utc_now()
        updated_at = (row["updated_at"] if "updated_at" in task_columns else None) or created_at
        last_step = (row["last_step"] if "last_step" in task_columns else None) or normalized_state
        retries = int(row["retries"]) if "retries" in task_columns and row["retries"] is not None else 0

        monitor_row = conn.execute(
            """
            select id from monitors
            where workspace_id = ? and retailer = ? and product_url = ?
            order by id asc
            limit 1
            """,
            (workspace_id, retailer, product_url),
        ).fetchone()
        if monitor_row:
            monitor_id = int(monitor_row["id"])
        else:
            cur = conn.execute(
                """
                insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, enabled, created_at)
                values (?, ?, ?, 20, ?, ?)
                """,
                (workspace_id, retailer, product_url, int(normalized_state == "monitoring"), created_at),
            )
            monitor_id = int(cur.lastrowid)

        task_config = json.dumps(
            {
                "retailer": retailer,
                "product_url": product_url,
                "profile": profile,
                "account": account,
                "payment": payment,
            }
        )
        enabled = int(normalized_state not in {"stopped", "success", "failed"})
        is_paused = int(normalized_state == "paused")
        cur = conn.execute(
            """
            insert into checkout_tasks(
                workspace_id,
                monitor_id,
                task_name,
                task_config,
                current_state,
                enabled,
                is_paused,
                last_error,
                created_at,
                updated_at,
                last_transition_at,
                legacy_task_id
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                workspace_id,
                monitor_id,
                f"{retailer} task",
                task_config,
                normalized_state,
                enabled,
                is_paused,
                last_error,
                created_at,
                updated_at,
                updated_at,
                legacy_task_id,
            ),
        )
        new_task_id = int(cur.lastrowid)
        conn.execute(
            """
            insert into checkout_attempts(task_id, workspace_id, monitor_id, state, status, details, error_text, created_at)
            values (?, ?, ?, ?, ?, ?, ?, ?)
            """,
            (
                new_task_id,
                workspace_id,
                monitor_id,
                normalized_state,
                "migrated",
                json.dumps({"legacy_task_id": legacy_task_id, "last_step": last_step, "retries": retries}),
                last_error,
                updated_at,
            ),
        )
        migrated_count += 1

    conn.execute(
        "insert into schema_migrations(key, applied_at) values (?, ?)",
        (migration_key, utc_now()),
    )
    if migrated_count:
        log(f"✅ Migrated {migrated_count} legacy rows from tasks to checkout_tasks")


def init_db() -> None:
    conn = db()
    conn.executescript(
        """
        create table if not exists workspaces (
            id integer primary key autoincrement,
            name text not null,
            plan text not null,
            proxy_url text,
            session_metadata text,
            behavior_metadata text,
            subscription_status text not null default 'inactive',
            subscription_source text not null default 'manual',
            subscription_updated_at text,
            created_at text not null
        );

        create table if not exists users (
            id integer primary key autoincrement,
            email text not null unique,
            name text not null,
            bearer_token text not null unique,
            created_at text not null
        );

        create table if not exists workspace_members (
            id integer primary key autoincrement,
            workspace_id integer not null,
            user_id integer not null,
            role text not null default 'member',
            created_at text not null,
            unique(workspace_id, user_id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(user_id) references users(id)
        );

        create table if not exists monitors (
            id integer primary key autoincrement,
            workspace_id integer not null,
            retailer text not null,
            category text not null default 'pokemon',
            product_url text not null,
            keyword text,
            max_price_cents integer,
            poll_interval_seconds integer not null,
            enabled integer not null default 1,
            last_checked_at text,
            last_in_stock integer,
            last_price_cents integer,
            proxy_url text,
            proxy_type text,
            proxy_region text,
            proxy_residential_only integer not null default 0,
            proxy_sticky_session_seconds integer,
            session_task_key text,
            session_metadata text,
            behavior_metadata text,
            created_at text not null,
            foreign key(workspace_id) references workspaces(id)
        );

        create table if not exists proxies (
            id integer primary key autoincrement,
            provider text not null,
            endpoint text not null unique,
            proxy_type text not null default 'http',
            status text not null default 'active',
            cooldown_until text,
            fail_streak integer not null default 0,
            request_count integer not null default 0,
            success_count integer not null default 0,
            timeout_count integer not null default 0,
            rate_limited_count integer not null default 0,
            forbidden_count integer not null default 0,
            failure_count integer not null default 0,
            health_score real not null default 1.0,
            quarantine_reason text,
            region_code text,
            is_residential integer not null default 0,
            sticky_session_seconds integer,
            last_used_at text,
            last_success_at text,
            last_failure_at text,
            created_at text not null,
            updated_at text not null
        );

        create table if not exists proxy_leases (
            id integer primary key autoincrement,
            proxy_id integer not null,
            lease_key text not null,
            owner_type text not null,
            owner_id integer,
            acquired_at text not null,
            expires_at text not null,
            released_at text,
            foreign key(proxy_id) references proxies(id)
        );

        create table if not exists webhooks (
            id integer primary key autoincrement,
            workspace_id integer not null,
            name text not null,
            webhook_url text not null,
            webhook_secret_id integer,
            enabled integer not null default 1,
            notify_success integer not null default 1,
            notify_failures integer not null default 0,
            notify_restock_only integer not null default 1,
            last_tested_at text,
            last_test_status text,
            last_delivery_status text,
            last_delivery_at text,
            fail_streak integer not null default 0,
            last_error text,
            last_status_code integer,
            created_at text not null,
            foreign key(workspace_id) references workspaces(id),
            foreign key(webhook_secret_id) references account_secrets(id)
        );

        create table if not exists account_secrets (
            id integer primary key autoincrement,
            workspace_id integer not null,
            user_id integer,
            secret_type text not null,
            ciphertext text not null,
            created_at text not null,
            updated_at text not null,
            foreign key(workspace_id) references workspaces(id),
            foreign key(user_id) references users(id)
        );

        create table if not exists events (
            id integer primary key autoincrement,
            monitor_id integer not null,
            event_type text not null,
            title text,
            product_url text not null,
            retailer text not null,
            price_cents integer,
            event_time text not null,
            dedupe_key text not null unique,
            foreign key(monitor_id) references monitors(id)
        );

        create table if not exists deliveries (
            id integer primary key autoincrement,
            event_id integer not null,
            webhook_id integer not null,
            status text not null,
            response_code integer,
            response_body text,
            delivered_at text,
            foreign key(event_id) references events(id),
            foreign key(webhook_id) references webhooks(id)
        );

        create table if not exists monitor_schedules (
            id integer primary key autoincrement,
            monitor_id integer not null,
            new_poll_interval_seconds integer not null,
            run_at text not null,
            applied_at text,
            created_at text not null,
            foreign key(monitor_id) references monitors(id)
        );

        create table if not exists monitor_failures (
            id integer primary key autoincrement,
            monitor_id integer not null,
            workspace_id integer not null,
            error_text text,
            failed_at text not null,
            foreign key(monitor_id) references monitors(id),
            foreign key(workspace_id) references workspaces(id)
        );

        create table if not exists jobs (
            id integer primary key autoincrement,
            job_type text not null default 'monitor_check',
            monitor_id integer,
            status text not null default 'queued',
            attempt_count integer not null default 0,
            next_run_at text not null,
            locked_by text,
            locked_at text,
            payload_json text,
            last_error text,
            created_at text not null,
            updated_at text not null,
            foreign key(monitor_id) references monitors(id)
        );

        create table if not exists checkout_tasks (
            id integer primary key autoincrement,
            workspace_id integer not null,
            monitor_id integer not null,
            task_name text,
            task_config text,
            active_proxy_id integer,
            active_proxy_lease_key text,
            current_state text not null default 'idle',
            enabled integer not null default 0,
            is_paused integer not null default 0,
            status_timestamps_json text,
            last_error text,
            created_at text not null,
            updated_at text not null,
            last_transition_at text,
            foreign key(workspace_id) references workspaces(id),
            foreign key(monitor_id) references monitors(id),
            foreign key(active_proxy_id) references proxies(id)
        );

        create table if not exists checkout_attempts (
            id integer primary key autoincrement,
            task_id integer not null,
            workspace_id integer not null,
            monitor_id integer not null,
            attempt_number integer,
            state text not null,
            step text,
            error text,
            status text not null,
            details text,
            error_text text,
            created_at text not null,
            updated_at text,
            foreign key(task_id) references checkout_tasks(id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(monitor_id) references monitors(id)
        );

        create table if not exists captcha_challenges (
            id integer primary key autoincrement,
            workspace_id integer not null,
            task_id integer not null,
            retailer_account_id integer,
            provider text not null,
            status text not null default 'pending',
            provider_payload text,
            manual_payload text,
            solved_token text,
            worker_handoff_token_hash text,
            handoff_issued_at text,
            handoff_expires_at text,
            handoff_used_at text,
            expires_at text,
            created_at text not null,
            updated_at text not null,
            solved_at text,
            foreign key(workspace_id) references workspaces(id),
            foreign key(task_id) references checkout_tasks(id),
            foreign key(retailer_account_id) references retailer_accounts(id)
        );

        create table if not exists task_logs (
            id integer primary key autoincrement,
            task_id integer not null,
            workspace_id integer not null,
            monitor_id integer not null,
            level text not null,
            event_type text not null,
            message text not null,
            payload text,
            created_at text not null,
            foreign key(task_id) references checkout_tasks(id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(monitor_id) references monitors(id)
        );

        create table if not exists billing_customers (
            id integer primary key autoincrement,
            workspace_id integer not null,
            user_id integer not null,
            provider text not null default 'stripe',
            provider_customer_id text,
            created_at text not null,
            updated_at text not null,
            unique(workspace_id, user_id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(user_id) references users(id)
        );

        create table if not exists billing_subscriptions (
            id integer primary key autoincrement,
            workspace_id integer not null,
            provider text not null default 'stripe',
            provider_subscription_id text,
            billing_customer_id integer,
            status text not null default 'incomplete',
            current_period_end text,
            cancel_at_period_end integer not null default 0,
            plan_code text,
            plan_interval text,
            plan_lookup_key text,
            created_at text not null,
            updated_at text not null,
            unique(workspace_id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(billing_customer_id) references billing_customers(id)
        );

        create unique index if not exists idx_billing_customers_provider_customer_id
            on billing_customers(provider_customer_id);

        create unique index if not exists idx_billing_subscriptions_provider_subscription_id
            on billing_subscriptions(provider_subscription_id);

        create table if not exists billing_webhook_events (
            id integer primary key autoincrement,
            event_id text not null unique,
            processed_at text not null,
            event_type text not null,
            workspace_id integer,
            foreign key(workspace_id) references workspaces(id)
        );

        create table if not exists checkout_profiles (
            id integer primary key autoincrement,
            workspace_id integer not null,
            name text not null,
            email text not null,
            phone text,
            shipping_address_json text not null,
            billing_address_json text not null,
            created_at text not null,
            updated_at text not null,
            foreign key(workspace_id) references workspaces(id)
        );

        create table if not exists payment_methods (
            id integer primary key autoincrement,
            workspace_id integer not null,
            label text not null,
            provider text,
            token_reference text not null,
            billing_profile_id integer,
            created_at text not null,
            updated_at text not null,
            foreign key(workspace_id) references workspaces(id),
            foreign key(billing_profile_id) references checkout_profiles(id)
        );

        create table if not exists retailer_accounts (
            id integer primary key autoincrement,
            workspace_id integer not null,
            retailer text not null,
            username text,
            email text,
            encrypted_credential_ref text not null,
            proxy_url text,
            proxy_lock_state text not null default 'unlocked',
            proxy_lock_owner text,
            proxy_lock_acquired_at text,
            last_used_at text,
            next_start_after text,
            session_status text not null default 'logged_out',
            created_at text not null,
            updated_at text not null,
            foreign key(workspace_id) references workspaces(id)
        );

        create table if not exists mailbox_credentials (
            id integer primary key autoincrement,
            workspace_id integer not null,
            provider text not null,
            email text not null,
            secret_id integer not null,
            imap_host text,
            imap_port integer not null default 993,
            use_ssl integer not null default 1,
            poll_interval_seconds integer not null default 5,
            timeout_seconds integer not null default 90,
            sender_filter text,
            subject_filter text,
            otp_regex text,
            created_at text not null,
            updated_at text not null,
            foreign key(workspace_id) references workspaces(id),
            foreign key(secret_id) references account_secrets(id)
        );

        create table if not exists task_profile_bindings (
            id integer primary key autoincrement,
            workspace_id integer not null,
            monitor_id integer not null,
            checkout_profile_id integer,
            retailer_account_id integer,
            payment_method_id integer,
            created_at text not null,
            updated_at text not null,
            unique(workspace_id, monitor_id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(monitor_id) references monitors(id),
            foreign key(checkout_profile_id) references checkout_profiles(id),
            foreign key(retailer_account_id) references retailer_accounts(id),
            foreign key(payment_method_id) references payment_methods(id)
        );

        create table if not exists alert_subscriptions (
            id integer primary key autoincrement,
            workspace_id integer not null,
            guild_id text not null,
            channel_id text not null,
            source text not null default 'discord',
            source_name text,
            retailer_filter text,
            url_patterns text not null default '[]',
            sku_patterns text not null default '[]',
            keyword_patterns text not null default '[]',
            enabled integer not null default 1,
            last_ingested_at text,
            created_at text not null,
            updated_at text not null,
            unique(workspace_id, guild_id, channel_id, source),
            foreign key(workspace_id) references workspaces(id)
        );

        create table if not exists alert_events (
            id integer primary key autoincrement,
            workspace_id integer not null,
            subscription_id integer not null,
            source_event_id text not null,
            source text not null,
            parse_status text not null,
            event_time text not null,
            retailer text,
            product_url text,
            sku text,
            title text,
            message text,
            payload_json text not null,
            normalized_json text not null,
            parse_error text,
            created_at text not null,
            unique(subscription_id, source_event_id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(subscription_id) references alert_subscriptions(id)
        );

        create table if not exists alert_event_actions (
            id integer primary key autoincrement,
            event_id integer not null,
            workspace_id integer not null,
            monitor_id integer not null,
            action_type text not null,
            status text not null,
            dedupe_key text not null unique,
            task_id integer,
            job_id integer,
            details text,
            created_at text not null,
            foreign key(event_id) references alert_events(id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(monitor_id) references monitors(id),
            foreign key(task_id) references checkout_tasks(id),
            foreign key(job_id) references jobs(id)
        );

        create index if not exists idx_alert_subscriptions_workspace_enabled
            on alert_subscriptions(workspace_id, enabled);
        create index if not exists idx_alert_events_workspace_created
            on alert_events(workspace_id, created_at);
        """
    )
    existing = conn.execute("select id from workspaces limit 1").fetchone()
    if not existing:
        conn.execute(
            "insert into workspaces(name, plan, created_at) values (?, ?, ?)",
            (DEFAULT_WORKSPACE["name"], DEFAULT_WORKSPACE["plan"], utc_now()),
        )
        log("✅ Initialized default workspace")
    workspace = conn.execute("select id from workspaces order by id asc limit 1").fetchone()
    user = conn.execute("select id from users where email = ?", (DEFAULT_USER_EMAIL,)).fetchone()
    if not user:
        token = DEFAULT_BEARER_TOKEN or secrets.token_urlsafe(24)
        cur = conn.execute(
            "insert into users(email, name, bearer_token, created_at) values (?, ?, ?, ?)",
            (DEFAULT_USER_EMAIL, DEFAULT_USER_NAME, token, utc_now()),
        )
        user_id = cur.lastrowid
        log(f"✅ Initialized default user {DEFAULT_USER_EMAIL}")
    else:
        user_id = user["id"]
    member = conn.execute(
        "select id from workspace_members where workspace_id = ? and user_id = ?",
        (workspace["id"], user_id),
    ).fetchone()
    if not member:
        conn.execute(
            """
            insert into workspace_members(workspace_id, user_id, role, created_at)
            values (?, ?, 'owner', ?)
            """,
            (workspace["id"], user_id, utc_now()),
        )
        log("✅ Linked default user to default workspace")
    conn.commit()
    ensure_column(conn, "monitors", "msrp_cents", "integer")
    ensure_column(conn, "monitors", "failure_streak", "integer not null default 0")
    ensure_column(conn, "monitors", "last_error", "text")
    ensure_column(conn, "webhooks", "notify_success", "integer not null default 1")
    ensure_column(conn, "webhooks", "notify_failures", "integer not null default 0")
    ensure_column(conn, "webhooks", "notify_restock_only", "integer not null default 1")
    ensure_column(conn, "webhooks", "last_tested_at", "text")
    ensure_column(conn, "webhooks", "last_test_status", "text")
    ensure_column(conn, "webhooks", "last_delivery_status", "text")
    ensure_column(conn, "webhooks", "last_delivery_at", "text")
    ensure_column(conn, "webhooks", "fail_streak", "integer not null default 0")
    ensure_column(conn, "webhooks", "last_error", "text")
    ensure_column(conn, "webhooks", "last_status_code", "integer")
    ensure_column(conn, "webhooks", "webhook_secret_id", "integer")
    ensure_column(conn, "workspaces", "subscription_status", "text not null default 'inactive'")
    ensure_column(conn, "workspaces", "subscription_source", "text not null default 'manual'")
    ensure_column(conn, "workspaces", "subscription_updated_at", "text")
    ensure_column(conn, "workspaces", "proxy_url", "text")
    ensure_column(conn, "workspaces", "session_metadata", "text")
    ensure_column(conn, "workspaces", "behavior_metadata", "text")
    ensure_column(conn, "monitors", "proxy_url", "text")
    ensure_column(conn, "monitors", "proxy_type", "text")
    ensure_column(conn, "monitors", "proxy_region", "text")
    ensure_column(conn, "monitors", "proxy_residential_only", "integer not null default 0")
    ensure_column(conn, "monitors", "proxy_sticky_session_seconds", "integer")
    ensure_column(conn, "monitors", "session_task_key", "text")
    ensure_column(conn, "monitors", "session_metadata", "text")
    ensure_column(conn, "monitors", "category", "text not null default 'pokemon'")
    ensure_column(conn, "jobs", "job_type", "text not null default 'monitor_check'")
    ensure_column(conn, "jobs", "monitor_id", "integer")
    ensure_column(conn, "jobs", "payload_json", "text")
    ensure_column(conn, "jobs", "last_error", "text")
    ensure_column(conn, "checkout_attempts", "attempt_number", "integer")
    ensure_column(conn, "checkout_attempts", "step", "text")
    ensure_column(conn, "checkout_attempts", "error", "text")
    ensure_column(conn, "checkout_attempts", "updated_at", "text")
    ensure_column(conn, "checkout_tasks", "status_timestamps_json", "text")
    run_legacy_tasks_migration(conn)
    run_pokemon_center_task_group_config_migration(conn)
    conn.commit()
    conn.close()


def normalize_proxy_policy(policy: dict[str, Any] | None) -> dict[str, Any]:
    policy = policy or {}
    normalized: dict[str, Any] = {
        "residential_only": bool(policy.get("residential_only")),
    }
    region = (policy.get("region") or "").strip().upper()
    if region:
        normalized["region"] = region
    proxy_type = (policy.get("type") or "").strip().lower()
    if proxy_type:
        normalized["type"] = proxy_type
    sticky_raw = policy.get("sticky_session_seconds")
    if sticky_raw is not None:
        sticky = int(sticky_raw)
        if sticky < 0:
            raise ValueError("sticky_session_seconds must be >= 0")
        normalized["sticky_session_seconds"] = sticky
    return normalized


class ProxyAllocator:
    def __init__(self, conn: sqlite3.Connection) -> None:
        self.conn = conn

    def acquire_lease(
        self,
        *,
        owner_type: str,
        owner_id: int | None,
        lease_key: str,
        policy: dict[str, Any] | None = None,
        lease_seconds: int = 60,
    ) -> ProxyLease | None:
        now_iso = utc_now()
        expires_iso = datetime.fromisoformat(now_iso).timestamp() + max(1, lease_seconds)
        expires_at = datetime.fromtimestamp(expires_iso, tz=timezone.utc).isoformat()
        normalized = normalize_proxy_policy(policy)
        filters = ["p.status = 'active'", "(p.cooldown_until is null or datetime(p.cooldown_until) <= datetime(?))"]
        params: list[Any] = [now_iso]
        if normalized.get("residential_only"):
            filters.append("p.is_residential = 1")
        if normalized.get("region"):
            filters.append("upper(p.region_code) = ?")
            params.append(normalized["region"])
        if normalized.get("type"):
            filters.append("p.proxy_type = ?")
            params.append(normalized["type"])
        if normalized.get("sticky_session_seconds"):
            filters.append("(p.sticky_session_seconds is null or p.sticky_session_seconds >= ?)")
            params.append(int(normalized["sticky_session_seconds"]))

        self.conn.execute("begin immediate")
        try:
            self.conn.execute(
                "update proxy_leases set released_at = ? where released_at is null and datetime(expires_at) <= datetime(?)",
                (now_iso, now_iso),
            )
            existing = self.conn.execute(
                """
                select pl.id as lease_id, p.id as proxy_id, p.endpoint
                from proxy_leases pl
                join proxies p on p.id = pl.proxy_id
                where pl.released_at is null
                  and pl.owner_type = ?
                  and pl.owner_id is ?
                  and pl.lease_key = ?
                  and datetime(pl.expires_at) > datetime(?)
                limit 1
                """,
                (owner_type, owner_id, lease_key, now_iso),
            ).fetchone()
            if existing:
                self.conn.commit()
                return ProxyLease(
                    lease_id=existing["lease_id"],
                    proxy_id=existing["proxy_id"],
                    endpoint=existing["endpoint"],
                    lease_key=lease_key,
                    owner_type=owner_type,
                    owner_id=owner_id,
                )
            candidate = self.conn.execute(
                f"""
                select p.*
                from proxies p
                where {' and '.join(filters)}
                  and not exists (
                    select 1
                    from proxy_leases pl
                    where pl.proxy_id = p.id
                      and pl.released_at is null
                      and datetime(pl.expires_at) > datetime(?)
                  )
                order by p.health_score desc, p.fail_streak asc, coalesce(p.last_used_at, '1970-01-01T00:00:00+00:00') asc
                limit 1
                """,
                (*params, now_iso),
            ).fetchone()
            if not candidate:
                self.conn.commit()
                return None
            cur = self.conn.execute(
                """
                insert into proxy_leases(proxy_id, lease_key, owner_type, owner_id, acquired_at, expires_at)
                values (?, ?, ?, ?, ?, ?)
                """,
                (candidate["id"], lease_key, owner_type, owner_id, now_iso, expires_at),
            )
            self.conn.execute(
                "update proxies set last_used_at = ?, updated_at = ? where id = ?",
                (now_iso, now_iso, candidate["id"]),
            )
            self.conn.commit()
            return ProxyLease(
                lease_id=int(cur.lastrowid),
                proxy_id=candidate["id"],
                endpoint=candidate["endpoint"],
                lease_key=lease_key,
                owner_type=owner_type,
                owner_id=owner_id,
            )
        except Exception:
            self.conn.rollback()
            raise

    def release_lease(self, *, lease_id: int) -> None:
        now_iso = utc_now()
        self.conn.execute("begin immediate")
        try:
            self.conn.execute(
                "update proxy_leases set released_at = coalesce(released_at, ?) where id = ?",
                (now_iso, lease_id),
            )
            self.conn.commit()
        except Exception:
            self.conn.rollback()
            raise

    def record_telemetry(self, *, lease: ProxyLease, request_result: RequestResult) -> None:
        telemetry = request_result.telemetry
        status = telemetry.status_code
        is_timeout = telemetry.error_class in {"Timeout", "ReadTimeout", "ConnectTimeout"}
        is_429 = status == 429
        is_403 = status == 403
        ok = telemetry.ok and not request_result.error
        now_iso = utc_now()
        self.conn.execute(
            """
            update proxies
            set request_count = request_count + 1,
                success_count = success_count + ?,
                timeout_count = timeout_count + ?,
                rate_limited_count = rate_limited_count + ?,
                forbidden_count = forbidden_count + ?,
                failure_count = failure_count + ?,
                fail_streak = case when ? then 0 else fail_streak + 1 end,
                last_success_at = case when ? then ? else last_success_at end,
                last_failure_at = case when ? then last_failure_at else ? end,
                updated_at = ?
            where id = ?
            """,
            (
                int(ok),
                int(is_timeout),
                int(is_429),
                int(is_403),
                int(not ok),
                int(ok),
                int(ok),
                now_iso,
                int(ok),
                now_iso,
                now_iso,
                lease.proxy_id,
            ),
        )
        self.conn.execute(
            """
            update proxies
            set health_score = (
                (cast(success_count as real) / nullif(request_count, 0))
                - ((cast(timeout_count as real) / nullif(request_count, 0)) * 0.45)
                - ((cast(rate_limited_count as real) / nullif(request_count, 0)) * 0.30)
                - ((cast(forbidden_count as real) / nullif(request_count, 0)) * 0.60)
                - min(0.30, fail_streak * 0.04)
            )
            where id = ? and request_count > 0
            """,
            (lease.proxy_id,),
        )
        row = self.conn.execute(
            """
            select request_count, fail_streak, health_score,
                   (cast(timeout_count + rate_limited_count + forbidden_count as real) / nullif(request_count, 0)) as severe_failure_rate
            from proxies
            where id = ?
            """,
            (lease.proxy_id,),
        ).fetchone()
        if row and (
            row["fail_streak"] >= 5
            or (row["request_count"] >= 8 and (row["health_score"] or 0) < 0.30)
            or (row["request_count"] >= 10 and (row["severe_failure_rate"] or 0) > 0.45)
        ):
            cooldown_at = datetime.now(timezone.utc).timestamp() + 15 * 60
            cooldown_until = datetime.fromtimestamp(cooldown_at, tz=timezone.utc).isoformat()
            self.conn.execute(
                """
                update proxies
                set status = 'quarantined',
                    cooldown_until = ?,
                    quarantine_reason = 'auto_health_quarantine',
                    updated_at = ?
                where id = ?
                """,
                (cooldown_until, now_iso, lease.proxy_id),
            )


def perform_request(
    *,
    task_key: str,
    method: str,
    url: str,
    workspace_id: int | None,
    proxy_url: str | None,
    behavior_policy: RequestBehaviorPolicy | None = None,
    pacing_key: str | None = None,
    throttle_signal: bool = False,
    throttle_reason: str | None = None,
    timeout: float,
    retry_total: int,
    backoff_factor: float,
    proxy_lease: ProxyLease | None = None,
    **kwargs: Any,
) -> RequestResult:
    result = session_manager.request(
        task_key=task_key,
        method=method,
        url=url,
        workspace_id=workspace_id,
        proxy_url=proxy_url,
        behavior_policy=behavior_policy,
        pacing_key=pacing_key,
        throttle_signal=throttle_signal,
        throttle_reason=throttle_reason,
        timeout=timeout,
        retry_total=retry_total,
        backoff_factor=backoff_factor,
        **kwargs,
    )
    telemetry = result.telemetry
    level = "warning" if not telemetry.ok else "info"
    log(
        f"http_request task={telemetry.task_key} method={method.upper()} status={telemetry.status_code} "
        f"latency_ms={telemetry.latency_ms} error_class={telemetry.error_class} "
        f"pacing_profile={telemetry.pacing_profile} planned_delay_ms={telemetry.planned_delay_ms} "
        f"applied_delay_ms={telemetry.applied_delay_ms} adaptive_level={telemetry.adaptive_backoff_level} "
        f"throttled={int(telemetry.throttled)} throttle_reason={telemetry.throttle_reason}",
        level=level,
        workspace_id=workspace_id,
    )
    if proxy_lease:
        conn = db()
        try:
            allocator = ProxyAllocator(conn)
            allocator.record_telemetry(lease=proxy_lease, request_result=result)
            conn.commit()
        finally:
            conn.close()
    return result


def current_workspace_id() -> int:
    workspace_id = getattr(g, "workspace_id", None)
    if workspace_id is None:
        raise RuntimeError("Missing workspace context")
    return workspace_id


def current_user_context() -> dict[str, Any]:
    user = getattr(g, "current_user", None)
    if not user:
        raise RuntimeError("Missing user context")
    return dict(user)


def get_workspace_for_user(user_id: int) -> sqlite3.Row | None:
    conn = db()
    row = conn.execute(
        """
        select w.*, wm.role as member_role from workspace_members wm
        join workspaces w on w.id = wm.workspace_id
        where wm.user_id = ?
        order by w.id asc
        limit 1
        """,
        (user_id,),
    ).fetchone()
    conn.close()
    return row


def resolve_user_from_request() -> sqlite3.Row | None:
    auth_header = request.headers.get("Authorization", "")
    if not auth_header.startswith("Bearer "):
        return None
    token = auth_header.removeprefix("Bearer ").strip()
    if not token:
        return None
    conn = db()
    user = conn.execute("select * from users where bearer_token = ?", (token,)).fetchone()
    conn.close()
    return user


def require_auth(view_func):
    @wraps(view_func)
    def wrapped(*args, **kwargs):
        user = getattr(g, "current_user", None)
        workspace_id = getattr(g, "workspace_id", None)
        if not user or workspace_id is None:
            return jsonify({"error": "Unauthorized"}), 401
        return view_func(*args, **kwargs)

    return wrapped


def get_workspace(workspace_id: int) -> sqlite3.Row:
    conn = db()
    row = conn.execute("select * from workspaces where id = ?", (workspace_id,)).fetchone()
    conn.close()
    if not row:
        raise ValueError("Workspace not found")
    return row


def get_default_workspace() -> sqlite3.Row:
    conn = db()
    row = conn.execute("select * from workspaces order by id asc limit 1").fetchone()
    conn.close()
    if not row:
        raise ValueError("Workspace not found")
    return row


def get_workspace_for_request() -> sqlite3.Row:
    workspace = getattr(g, "current_workspace", None)
    if workspace is None:
        workspace = get_default_workspace()
    return workspace


def get_workspace_id_for_request() -> int:
    return int(get_workspace_for_request()["id"])


def get_workspace_from_auth() -> sqlite3.Row:
    return get_workspace(current_workspace_id())


def _token_from_request() -> str | None:
    auth_header = (request.headers.get("Authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        token = auth_header[7:].strip()
        return token or None
    token = (request.headers.get("X-API-Token") or "").strip()
    return token or None


def _set_auth_context(user: sqlite3.Row | dict[str, Any], workspace: sqlite3.Row) -> None:
    g.current_user = dict(user)
    g.workspace_id = int(workspace["id"])
    g.current_workspace = workspace
    g.current_role = workspace["member_role"] if "member_role" in workspace.keys() else "owner"


def _extract_captcha_token() -> str | None:
    token = captcha_verifier.extract_token(request)
    return token or None


def verify_captcha_token(token: str) -> tuple[bool, str | None]:
    return captcha_verifier.verify_token(token, request)


def _is_captcha_protected_request() -> bool:
    return captcha_verifier.is_captcha_protected_request(request)


def _captcha_token_from_request() -> str:
    return captcha_verifier.extract_token(request)


@app.before_request
def require_api_auth() -> tuple[dict[str, str], int] | None:
    incoming_correlation_id = (request.headers.get(CORRELATION_ID_HEADER) or "").strip()
    g.correlation_id = incoming_correlation_id or str(uuid4())
    if request.path == "/api/billing/stripe/webhook":
        return None
    if not request.path.startswith("/api/"):
        return None
    user = resolve_user_from_request()
    if user:
        workspace = get_workspace_for_user(user["id"])
        if not workspace:
            return jsonify({"error": "No workspace membership found"}), 403
        _set_auth_context(user, workspace)
    else:
        token = _token_from_request()
        if token and token == API_AUTH_TOKEN:
            _set_auth_context(DEFAULT_USER, get_default_workspace())
        else:
            return jsonify({"error": "Unauthorized"}), 401

    captcha_ok, reason = captcha_verifier.enforce_or_error(request)
    if not captcha_ok:
        header_token_present = bool((request.headers.get("X-CAPTCHA-Token") or "").strip())
        reason_out = reason
        if header_token_present:
            if reason == "invalid_token":
                reason_out = "provider_rejected"
            elif reason == "provider_unreachable":
                reason_out = "provider_request_failed"
        log(
            f"Blocked request due to failed CAPTCHA verification ({reason_out})",
            level="warning",
            workspace_id=getattr(g, "workspace_id", None),
        )
        strict_status = CAPTCHA_VERIFY_URL == DEFAULT_CAPTCHA_VERIFY_URL
        status_code = 403 if (header_token_present or strict_status) else 400
        return jsonify({"error": "CAPTCHA verification failed", "reason": reason_out}), status_code
    return None


@app.after_request
def add_correlation_id_header(response):
    correlation_id = getattr(g, "correlation_id", None)
    if correlation_id:
        response.headers[CORRELATION_ID_HEADER] = correlation_id
    if request.path.startswith("/api/") and response.is_json:
        try:
            payload = response.get_json(silent=True)
            if payload is not None:
                response.set_data(json.dumps(redact_sensitive_payload(payload)))
        except Exception:
            pass
    return response


def user_is_workspace_owner(user_id: int, workspace_id: int) -> bool:
    conn = db()
    row = conn.execute(
        """
        select 1 from workspace_members
        where user_id = ? and workspace_id = ? and role = 'owner'
        limit 1
        """,
        (user_id, workspace_id),
    ).fetchone()
    conn.close()
    return row is not None


def ensure_workspace_owner() -> tuple[dict[str, str], int] | None:
    user = getattr(g, "current_user", None)
    workspace_id = current_workspace_id()
    if not user or not user_is_workspace_owner(int(user["id"]), workspace_id):
        return jsonify({"error": "Workspace owner access required"}), 403
    return None


def ensure_workspace_role(*allowed_roles: str) -> tuple[dict[str, str], int] | None:
    current_role = (getattr(g, "current_role", None) or "").lower()
    expected = {role.lower() for role in allowed_roles}
    if current_role not in expected:
        return jsonify({"error": "Insufficient role for this operation"}), 403
    return None


def enforce_plan_limits(workspace_id: int, poll_interval_seconds: int) -> None:
    workspace = get_workspace(workspace_id)
    limits = PLAN_LIMITS[workspace["plan"]]
    conn = db()
    count = conn.execute(
        "select count(*) as c from monitors where workspace_id = ?", (workspace_id,)
    ).fetchone()["c"]
    conn.close()
    if count >= limits["max_monitors"]:
        raise ValueError(f"Plan limit reached ({limits['max_monitors']} monitors)")
    if poll_interval_seconds < limits["min_poll_seconds"]:
        raise ValueError(
            f"Plan {workspace['plan']} minimum poll interval is {limits['min_poll_seconds']} seconds"
        )


def _resolve_monitor_input(
    raw_input: str,
    *,
    is_edit_flow: bool = False,
    existing_product_count: int | None = None,
) -> tuple[str, list[dict[str, Any]]]:
    parsed_products = parse_monitor_input(
        raw_input,
        is_edit_flow=is_edit_flow,
        existing_product_count=existing_product_count,
    )
    first_product = parsed_products[0]
    pid = str(first_product["pid"])
    if pid == "placeholder":
        return "placeholder", parsed_products
    canonical_url = f"https://www.pokemoncenter.com/product/{pid}"
    return canonical_url, parsed_products


def normalize_monitor_assist_pid(raw_pid: str) -> str:
    digits = re.sub(r"\D+", "", str(raw_pid or ""))
    if len(digits) != 10:
        raise ValueError("PID must contain exactly 10 digits")
    return f"{digits[:2]}-{digits[2:7]}-{digits[7:]}"


def _normalize_plan_hint(value: str | None) -> str:
    if not value:
        return ""
    return re.sub(r"[^a-z0-9]+", "_", value.strip().lower()).strip("_")


def map_subscription_to_internal_plan(
    *,
    status: str,
    plan_code: str | None,
    plan_lookup_key: str | None,
    cancel_at_period_end: bool,
) -> str:
    normalized_status = (status or "").strip().lower()
    if normalized_status in {"canceled", "incomplete_expired", "unpaid"}:
        return "basic"

    if cancel_at_period_end and normalized_status in {"canceled"}:
        return "basic"

    lookup = PLAN_LOOKUP_TO_INTERNAL_PLAN.get(_normalize_plan_hint(plan_lookup_key))
    if lookup:
        return lookup

    combined = f"{_normalize_plan_hint(plan_lookup_key)} {_normalize_plan_hint(plan_code)}"
    if "team" in combined:
        return "team"
    if "pro" in combined:
        return "pro"
    return "basic"


def sync_billing_subscription_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return sync_manual_billing_subscription_event(payload)


def sync_manual_billing_subscription_event(payload: dict[str, Any]) -> dict[str, Any]:
    provider = (payload.get("provider") or "stripe").strip().lower()
    subscription_id = (payload.get("provider_subscription_id") or "").strip()
    customer_id = (payload.get("provider_customer_id") or "").strip() or None
    status = (payload.get("status") or "incomplete").strip().lower()
    plan_code = payload.get("plan_code")
    plan_lookup_key = payload.get("plan_lookup_key")
    cancel_at_period_end = bool(payload.get("cancel_at_period_end", False))
    current_period_end = payload.get("current_period_end")
    source = (payload.get("source") or "billing_subscriptions").strip()

    if not subscription_id:
        raise ValueError("provider_subscription_id is required")

    conn = db()
    try:
        conn.execute("begin")
        existing = conn.execute(
            """
            select workspace_id, billing_customer_id
            from billing_subscriptions
            where provider = ? and provider_subscription_id = ?
            """,
            (provider, subscription_id),
        ).fetchone()
        customer = None
        if customer_id:
            customer = conn.execute(
                """
                select id, workspace_id
                from billing_customers
                where provider = ? and provider_customer_id = ?
                """,
                (provider, customer_id),
            ).fetchone()

        workspace_id: int | None = existing["workspace_id"] if existing else None
        billing_customer_id: int | None = existing["billing_customer_id"] if existing else None

        if customer:
            workspace_id = customer["workspace_id"]
            billing_customer_id = customer["id"]

        if workspace_id is None:
            raise ValueError("Unable to resolve workspace for subscription event")

        now = utc_now()
        conn.execute(
            """
            insert into billing_subscriptions(
                workspace_id,
                provider,
                provider_subscription_id,
                billing_customer_id,
                status,
                current_period_end,
                cancel_at_period_end,
                plan_code,
                plan_interval,
                plan_lookup_key,
                created_at,
                updated_at
            )
            values (?, ?, ?, ?, ?, ?, ?, ?, null, ?, ?, ?)
            on conflict(provider_subscription_id) do update set
                workspace_id = excluded.workspace_id,
                billing_customer_id = excluded.billing_customer_id,
                status = excluded.status,
                current_period_end = excluded.current_period_end,
                cancel_at_period_end = excluded.cancel_at_period_end,
                plan_code = excluded.plan_code,
                plan_lookup_key = excluded.plan_lookup_key,
                updated_at = excluded.updated_at
            """,
            (
                workspace_id,
                provider,
                subscription_id,
                billing_customer_id,
                status,
                current_period_end,
                int(cancel_at_period_end),
                plan_code,
                plan_lookup_key,
                now,
                now,
            ),
        )

        internal_plan = map_subscription_to_internal_plan(
            status=status,
            plan_code=plan_code,
            plan_lookup_key=plan_lookup_key,
            cancel_at_period_end=cancel_at_period_end,
        )
        conn.execute(
            """
            update workspaces
            set plan = ?, subscription_status = ?, subscription_source = ?, subscription_updated_at = ?
            where id = ?
            """,
            (internal_plan, status, source, now, workspace_id),
        )
        conn.commit()
    except Exception:
        conn.rollback()
        raise
    finally:
        conn.close()
    return {"workspace_id": workspace_id, "plan": internal_plan, "status": status}


def sync_billing_subscription_payload(payload: dict[str, Any]) -> dict[str, Any]:
    return sync_manual_billing_subscription_event(payload)


def get_adapter_for_retailer(retailer: str | None):
    return resolve_retailer_adapter(retailer)


def evaluate_page(
    html: str,
    keyword: str | None = None,
    retailer: str | None = None,
    category: str | None = None,
) -> MonitorResult:
    return parse_monitor_html(html=html, keyword=keyword, retailer=retailer)
    adapter = get_adapter_for_retailer(retailer)
    return run_retailer_flow(adapter, {"html": html, "keyword": keyword, "category": category})


def fetch_monitor(monitor: sqlite3.Row) -> MonitorResult:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; StockSentinel/1.0; +https://example.com)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    conn = db()
    workspace = conn.execute(
        "select proxy_url, behavior_metadata from workspaces where id = ?",
        (monitor["workspace_id"],),
    ).fetchone()
    conn.close()
    proxy_url = monitor["proxy_url"] or (workspace["proxy_url"] if workspace else None)
    lease: ProxyLease | None = None
    if not proxy_url:
        policy = {
            "residential_only": bool(monitor["proxy_residential_only"]),
            "region": monitor["proxy_region"],
            "type": monitor["proxy_type"],
            "sticky_session_seconds": monitor["proxy_sticky_session_seconds"],
        }
        lease = allocator.acquire_lease(
            owner_type="monitor",
            owner_id=monitor["id"],
            lease_key=monitor["session_task_key"] or f"monitor-{monitor['id']}",
            policy=policy,
            lease_seconds=int(monitor["proxy_sticky_session_seconds"] or 60),
        )
        if lease:
            proxy_url = lease.endpoint
    task_key = monitor["session_task_key"] or f"monitor-{monitor['id']}"
    behavior_policy = _build_request_behavior_policy(monitor, workspace)
    req = perform_request(
        task_key=task_key,
        method="GET",
        url=monitor["product_url"],
        workspace_id=monitor["workspace_id"],
        proxy_url=proxy_url,
        behavior_policy=behavior_policy,
        pacing_key=f"{monitor['retailer']}:{monitor['id']}",
        timeout=15,
        retry_total=2,
        backoff_factor=0.35,
        headers=headers,
    )
    if req.error:
        raise req.error
    assert req.response is not None
    r = req.response
    r.raise_for_status()
    keyword = (monitor["keyword"] or "").strip() or None
    category = (monitor["category"] or "").strip() or "pokemon"
    return evaluate_page(r.text, keyword=keyword, retailer=monitor["retailer"], category=category)


def alert_eligibility(monitor: sqlite3.Row, result: MonitorResult) -> bool:
    if not result.in_stock:
        return False

    keyword_ok = result.keyword_matched in (True, None)

    max_price_cents = monitor["max_price_cents"]
    if max_price_cents is None:
        result.price_within_limit = None
        price_ok = True
    else:
        result.price_within_limit = result.price_cents is not None and result.price_cents <= max_price_cents
        price_ok = bool(result.price_within_limit)

    keyword = (monitor["keyword"] or "").strip().lower()
    msrp_cents = monitor["msrp_cents"]
    if "pokemon" in keyword and msrp_cents is not None:
        result.within_msrp_delta = (
            result.price_cents is not None
            and result.price_cents <= (msrp_cents + POKEMON_MSRP_BUFFER_CENTS)
        )
        msrp_ok = bool(result.within_msrp_delta)
    else:
        result.within_msrp_delta = None
        msrp_ok = True

    return keyword_ok and price_ok and msrp_ok


def dedupe_key(monitor: sqlite3.Row, result: MonitorResult) -> str:
    bucket = (result.price_cents or -1) // 100
    minute = datetime.now(timezone.utc).strftime("%Y%m%d%H%M")
    return f"{monitor['id']}:{result.in_stock}:{bucket}:{minute}"


def create_event_and_deliver(
    monitor: sqlite3.Row,
    result: MonitorResult,
    eligible: bool | None = None,
) -> None:
    if eligible is None:
        eligible = alert_eligibility(monitor, result)
    if not eligible:
        return

    key = dedupe_key(monitor, result)
    conn = db()
    existing = conn.execute("select id from events where dedupe_key = ?", (key,)).fetchone()
    if existing:
        conn.close()
        return

    ev = (
        monitor["id"],
        "in_stock",
        result.title,
        monitor["product_url"],
        monitor["retailer"],
        result.price_cents,
        utc_now(),
        key,
    )
    cur = conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        ev,
    )
    event_id = cur.lastrowid

    webhooks = conn.execute(
        "select * from webhooks where workspace_id = ? and enabled = 1",
        (monitor["workspace_id"],),
    ).fetchall()

    payload = {
        "username": "Stock Sentinel",
        "content": "@here In-stock alert",
        "embeds": [
            {
                "title": f"In Stock: {result.title}",
                "url": monitor["product_url"],
                "description": f"Retailer: {monitor['retailer']}",
                "color": 5763719,
                "fields": [
                    {"name": "Price", "value": cents_to_dollars(result.price_cents), "inline": True},
                    {"name": "Status", "value": "IN STOCK", "inline": True},
                    {"name": "Detected", "value": utc_now(), "inline": False},
                ],
                "footer": {"text": f"Monitor ID: {monitor['id']}"},
                "timestamp": datetime.now(timezone.utc).isoformat(),
            }
        ],
    }

    for hook in webhooks:
        if not should_send_to_webhook(monitor, hook, eligible):
            continue
        status, code, body = "queued", None, ""
        try:
            target_url = resolve_webhook_url(conn, hook)
            req = perform_request(
                task_key=f"webhook-{hook['id']}",
                method="POST",
                url=target_url,
                workspace_id=monitor["workspace_id"],
                proxy_url=None,
                timeout=8,
                retry_total=1,
                backoff_factor=0.2,
                json=payload,
            )
            if req.error:
                raise req.error
            assert req.response is not None
            resp = req.response
            code = resp.status_code
            body = (resp.text or "")[:1000]
            status = "sent" if 200 <= resp.status_code < 300 else "failed"
        except Exception as exc:  # noqa: BLE001
            status = "failed"
            body = str(exc)

        conn.execute(
            """
            insert into deliveries(event_id, webhook_id, status, response_code, response_body, delivered_at)
            values (?, ?, ?, ?, ?, ?)
            """,
            (event_id, hook["id"], status, code, body, utc_now()),
        )
        update_webhook_health(conn, hook["id"], status=status, status_code=code, error_text=body)

    conn.commit()
    conn.close()
    log(
        f"In-stock event emitted for monitor {monitor['id']} ({monitor['retailer']})",
        level="warning",
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
    )


def normalize_checkout_state(raw_state: Any, *, allow_control_states: bool = True) -> str:
    state = str(raw_state or "").strip().lower()
    compatibility = {
        "queued": "idle",
        "monitoring": "monitoring_product",
        "carting": "adding_to_cart",
        "shipping": "checking_out",
        "payment": "checking_out",
        "submitting": "checking_out",
        "failed": "decline",
    }
    state = compatibility.get(state, state)
    valid_states = CHECKOUT_TASK_STATES if allow_control_states else CHECKOUT_TASK_STATES - {"paused", "stopped"}
    if state not in valid_states:
        expected = ", ".join(sorted(valid_states))
        raise ValueError(f"Invalid checkout state. Expected one of: {expected}")
    return state


def record_checkout_attempt(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    workspace_id: int,
    monitor_id: int,
    state: str,
    status: str,
    details: dict[str, Any] | None = None,
    error_text: str | None = None,
) -> None:
    now_iso = utc_now()
    next_attempt_number = conn.execute(
        """
        select coalesce(max(attempt_number), 0) + 1 as next_attempt_number
        from checkout_attempts
        where task_id = ? and workspace_id = ?
        """,
        (task_id, workspace_id),
    ).fetchone()["next_attempt_number"]
    truncated_error = (error_text or "")[:500] if error_text else None
    conn.execute(
        """
        insert into checkout_attempts(
            task_id,
            workspace_id,
            monitor_id,
            attempt_number,
            state,
            step,
            error,
            status,
            details,
            error_text,
            created_at,
            updated_at
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            workspace_id,
            monitor_id,
            next_attempt_number,
            state,
            status,
            truncated_error,
            status,
            json.dumps(details or {}),
            truncated_error,
            now_iso,
            now_iso,
        ),
    )


def record_task_log(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    workspace_id: int,
    monitor_id: int,
    level: str,
    event_type: str,
    message: str,
    payload: dict[str, Any] | None = None,
) -> None:
    conn.execute(
        """
        insert into task_logs(task_id, workspace_id, monitor_id, level, event_type, message, payload, created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            workspace_id,
            monitor_id,
            level.lower(),
            event_type,
            message,
            json.dumps(payload or {}),
            utc_now(),
        ),
    )


def _json_list(value: Any) -> list[str]:
    if isinstance(value, list):
        return [str(item).strip() for item in value if str(item).strip()]
    if isinstance(value, str) and value.strip():
        try:
            parsed = json.loads(value)
            if isinstance(parsed, list):
                return [str(item).strip() for item in parsed if str(item).strip()]
        except json.JSONDecodeError:
            return [value.strip()]
    return []


def enqueue_alert_monitor_check_job(
    conn: sqlite3.Connection,
    *,
    monitor_id: int,
    reason: str,
    source_event_id: str,
    now_iso: str,
) -> int:
    payload = json.dumps({"step_attempts": {}, "reason": reason, "source_event_id": source_event_id})
    cur = conn.execute(
        """
        insert into jobs(job_type, monitor_id, status, attempt_count, next_run_at, payload_json, created_at, updated_at)
        values ('monitor_check', ?, 'queued', 0, ?, ?, ?, ?)
        """,
        (monitor_id, now_iso, payload, now_iso, now_iso),
    )
    return int(cur.lastrowid)


def create_checkout_task_for_alert(
    conn: sqlite3.Connection,
    *,
    monitor: sqlite3.Row,
    event_payload: dict[str, Any],
    reason: str,
) -> int:
    task = create_checkout_task(
        conn,
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
        task_name=f"Alert event checkout for monitor {monitor['id']}",
        task_config={
            "retailer": monitor["retailer"],
            "product_url": monitor["product_url"],
            "reason": reason,
            "alert_event": event_payload,
        },
        initial_state="queued",
    )
    record_task_log(
        conn,
        task_id=task["id"],
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
        level="info",
        event_type="discord_alert_match",
        message=f"Discord alert matched monitor {monitor['id']}",
        payload={"reason": reason, "source_event_id": event_payload.get("source_event_id")},
    )
    return int(task["id"])


def process_discord_alert_job(queue: SQLiteJobQueue, job: Job, *, now_iso: str) -> None:
    payload = json.loads(job.payload_json or "{}")
    subscription_id = int(payload.get("subscription_id") or 0)
    raw_event = payload.get("raw_event") if isinstance(payload.get("raw_event"), dict) else {}
    source_name = str(payload.get("source_name") or "discord").strip() or "discord"
    subscription = queue.conn.execute(
        "select * from alert_subscriptions where id = ? and enabled = 1",
        (subscription_id,),
    ).fetchone()
    if not subscription:
        queue.fail_job(
            job.id,
            now_iso=now_iso,
            status="failed",
            next_run_at=now_iso,
            payload_json=job.payload_json or "{}",
            error_text=f"subscription_not_found:{subscription_id}",
        )
        return
    workspace_id = int(subscription["workspace_id"])
    try:
        event = normalize_discord_alert_event(raw_event, fallback_source=source_name)
        accepted = subscription_accepts_event(
            event,
            retailer_filter=subscription["retailer_filter"],
            url_patterns=_json_list(subscription["url_patterns"]),
            sku_patterns=_json_list(subscription["sku_patterns"]),
            keyword_patterns=_json_list(subscription["keyword_patterns"]),
        )
        parse_status = "accepted" if accepted else "filtered"
        parse_error = None
    except Exception as exc:  # noqa: BLE001
        parse_status = "parse_failed"
        parse_error = str(exc)[:500]
        event = normalize_discord_alert_event({}, fallback_source=source_name)
        accepted = False

    event_cur = queue.conn.execute(
        """
        insert into alert_events(
            workspace_id, subscription_id, source_event_id, source, parse_status, event_time,
            retailer, product_url, sku, title, message, payload_json, normalized_json, parse_error, created_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        on conflict(subscription_id, source_event_id) do update set
            parse_status = excluded.parse_status,
            parse_error = excluded.parse_error
        """,
        (
            workspace_id,
            subscription_id,
            event.source_event_id,
            event.source,
            parse_status,
            event.event_time,
            event.retailer,
            event.product_url,
            event.sku,
            event.title,
            event.message,
            json.dumps(raw_event),
            json.dumps(
                {
                    "retailer": event.retailer,
                    "product_url": event.product_url,
                    "sku": event.sku,
                    "title": event.title,
                    "message": event.message,
                }
            ),
            parse_error,
            now_iso,
        ),
    )
    event_id = int(event_cur.lastrowid) if event_cur.lastrowid else int(
        queue.conn.execute(
            "select id from alert_events where subscription_id = ? and source_event_id = ?",
            (subscription_id, event.source_event_id),
        ).fetchone()["id"]
    )
    queue.conn.execute(
        "update alert_subscriptions set last_ingested_at = ?, updated_at = ? where id = ?",
        (now_iso, now_iso, subscription_id),
    )
    if not accepted:
        queue.complete_job(job.id, now_iso=now_iso)
        return

    monitors = queue.conn.execute(
        "select * from monitors where workspace_id = ? and enabled = 1",
        (workspace_id,),
    ).fetchall()
    matched = [monitor for monitor in monitors if monitor_matches_alert(dict(monitor), event)]
    for monitor in matched:
        monitor_id = int(monitor["id"])
        task_key = action_dedupe_key(
            workspace_id=workspace_id, monitor_id=monitor_id, event_id=event_id, action_type="checkout"
        )
        existing_action = queue.conn.execute(
            "select id from alert_event_actions where dedupe_key = ?",
            (task_key,),
        ).fetchone()
        if existing_action:
            continue
        task_id = create_checkout_task_for_alert(
            queue.conn,
            monitor=monitor,
            event_payload={"source_event_id": event.source_event_id, "title": event.title},
            reason="discord_ingestion_match",
        )
        job_id = enqueue_alert_monitor_check_job(
            queue.conn,
            monitor_id=monitor_id,
            reason="discord_ingestion_match",
            source_event_id=event.source_event_id,
            now_iso=now_iso,
        )
        queue.conn.execute(
            """
            insert into alert_event_actions(
                event_id, workspace_id, monitor_id, action_type, status, dedupe_key, task_id, job_id, details, created_at
            ) values (?, ?, ?, 'checkout_and_monitor', 'enqueued', ?, ?, ?, ?, ?)
            """,
            (event_id, workspace_id, monitor_id, task_key, task_id, job_id, json.dumps({"title": event.title}), now_iso),
        )
    queue.complete_job(job.id, now_iso=now_iso)

def create_checkout_task(
    conn: sqlite3.Connection,
    *,
    workspace_id: int,
    monitor_id: int,
    task_name: str | None = None,
    task_config: dict[str, Any] | None = None,
    initial_state: str = "idle",
) -> sqlite3.Row:
    task_config = dict(task_config or {})
    monitor_row = conn.execute(
        "select retailer, product_url from monitors where id = ? and workspace_id = ?",
        (monitor_id, workspace_id),
    ).fetchone()
    task_config = normalize_task_config_for_monitor(task_config, monitor_row=monitor_row)
    if "proxy_policy" in task_config:
        task_config["proxy_policy"] = normalize_proxy_policy(task_config.get("proxy_policy"))
    normalized_state = normalize_checkout_state(initial_state, allow_control_states=False)
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into checkout_tasks(
            workspace_id,
            monitor_id,
            task_name,
            task_config,
            current_state,
            enabled,
            is_paused,
            status_timestamps_json,
            created_at,
            updated_at,
            last_transition_at
        )
        values (?, ?, ?, ?, ?, 0, 0, ?, ?, ?, ?)
        """,
        (
            workspace_id,
            monitor_id,
            task_name,
            json.dumps(task_config or {}),
            normalized_state,
            json.dumps({normalized_state: now_iso}),
            now_iso,
            now_iso,
            now_iso,
        ),
    )
    task_id = cur.lastrowid
    record_checkout_attempt(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        monitor_id=monitor_id,
        state=normalized_state,
        status="created",
        details={"reason": "api_create"},
    )
    record_task_log(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        monitor_id=monitor_id,
        level="info",
        event_type="task_created",
        message=f"Checkout task {task_id} created",
        payload={"initial_state": normalized_state},
    )
    return conn.execute("select * from checkout_tasks where id = ?", (task_id,)).fetchone()


def get_checkout_task_for_workspace(
    conn: sqlite3.Connection, task_id: int, workspace_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        "select * from checkout_tasks where id = ? and workspace_id = ?",
        (task_id, workspace_id),
    ).fetchone()


def _build_transition_timestamp_map(row: sqlite3.Row, next_state: str, now_iso: str) -> dict[str, str]:
    raw_map = row["status_timestamps_json"] if "status_timestamps_json" in row.keys() else None
    existing = parse_json_object(raw_map)
    timeline = {str(key): str(value) for key, value in existing.items() if str(key) and str(value)}
    timeline[next_state] = now_iso
    return timeline


def serialize_checkout_task_summary(row: sqlite3.Row | None) -> dict[str, Any] | None:
    payload = serialize_checkout_task(row)
    if payload is None:
        return None
    state = str(payload.get("current_state") or "idle")
    timestamps = parse_json_object(payload.get("status_timestamps_json"))
    return {
        "id": payload["id"],
        "workspace_id": payload["workspace_id"],
        "monitor_id": payload["monitor_id"],
        "task_name": payload.get("task_name"),
        "retailer": (payload.get("task_config") or {}).get("retailer"),
        "current_state": state,
        "status_label": TASK_STATUS_LABELS.get(state, state.replace("_", " ").title()),
        "status_timestamps": timestamps,
        "last_transition_at": payload.get("last_transition_at"),
        "last_error": payload.get("last_error"),
    }


def transition_checkout_task(
    conn: sqlite3.Connection,
    *,
    task_id: int,
    workspace_id: int,
    requested_state: str,
    reason: str,
    error_text: str | None = None,
) -> sqlite3.Row | None:
    row = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    if not row:
        return None

    normalized_state = normalize_checkout_state(requested_state)
    task_config = parse_json_object(row["task_config"])
    active_lease_key = row["active_proxy_lease_key"]
    active_proxy_id = row["active_proxy_id"]
    allocator = ProxyAllocator(conn)
    if normalized_state == "monitoring_product" and not active_lease_key:
        policy = task_config.get("proxy_policy") if isinstance(task_config.get("proxy_policy"), dict) else {}
        sticky = int(policy.get("sticky_session_seconds") or 300)
        lease = allocator.acquire_lease(
            owner_type="checkout_task",
            owner_id=task_id,
            lease_key=f"checkout-task-{task_id}",
            policy=policy,
            lease_seconds=sticky,
        )
        if lease:
            active_lease_key = lease.lease_key
            active_proxy_id = lease.proxy_id
    if normalized_state in CHECKOUT_TERMINAL_STATES.union({"paused"}) and active_lease_key:
        lease_row = conn.execute(
            "select id from proxy_leases where owner_type = 'checkout_task' and owner_id = ? and lease_key = ? and released_at is null",
            (task_id, active_lease_key),
        ).fetchone()
        if lease_row:
            allocator.release_lease(lease_id=lease_row["id"])
        active_lease_key = None
        active_proxy_id = None
    enabled = int(normalized_state in CHECKOUT_ACTIVE_STATES)
    is_paused = int(normalized_state == "paused")
    now_iso = utc_now()
    timestamp_map = _build_transition_timestamp_map(row, normalized_state, now_iso)
    transition_error = (error_text or "")[:500] if error_text else None
    conn.execute(
        """
        update checkout_tasks
        set current_state = ?,
            enabled = ?,
            is_paused = ?,
            active_proxy_id = ?,
            active_proxy_lease_key = ?,
            status_timestamps_json = ?,
            last_error = ?,
            updated_at = ?,
            last_transition_at = ?
        where id = ? and workspace_id = ?
        """,
        (
            normalized_state,
            enabled,
            is_paused,
            active_proxy_id,
            active_lease_key,
            json.dumps(timestamp_map),
            transition_error,
            now_iso,
            now_iso,
            task_id,
            workspace_id,
        ),
    )
    record_checkout_attempt(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        monitor_id=row["monitor_id"],
        state=normalized_state,
        status="transition",
        details={"reason": reason},
        error_text=error_text,
    )
    record_task_log(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        monitor_id=row["monitor_id"],
        level="info" if not error_text else "error",
        event_type="state_transition",
        message=f"Task transitioned to {normalized_state}",
        payload={"reason": reason},
    )
    return get_checkout_task_for_workspace(conn, task_id, workspace_id)


class CheckoutRetryableError(RuntimeError):
    pass


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def serialize_checkout_task(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    config_raw = payload.get("task_config")
    try:
        parsed_config = json.loads(config_raw) if config_raw else {}
    except (TypeError, json.JSONDecodeError):
        parsed_config = {}
    payload["task_config"] = normalize_task_config_for_monitor(
        parsed_config if isinstance(parsed_config, dict) else {},
    )
    return payload


def serialize_checkout_task_summary(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return serialize_checkout_task(row)


def redact_webhook_url(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 12:
        return "***"
    return f"{value[:8]}...{value[-4:]}"


def serialize_webhook(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    payload["webhook_url"] = redact_webhook_url(payload.get("webhook_url") or "")
    payload = redact_sensitive_payload(payload)
    return payload
def _classify_checkout_step_failure(step: str, exc: Exception, attempt_number: int) -> tuple[bool, str]:
    policy = CHECKOUT_STEP_RETRY_POLICY[step]
    if isinstance(exc, (ValueError, PermissionError)):
        return False, "terminal_validation_error"
    if attempt_number >= policy["max_attempts"]:
        return False, "terminal_max_attempts_exceeded"
    if isinstance(exc, (CheckoutRetryableError, requests.RequestException, TimeoutError, ConnectionError, sqlite3.OperationalError)):
        return True, "retryable_exception"
    return False, "terminal_unclassified"


def _derive_status_signal(exc: Exception) -> str:
    message = str(exc).strip().lower()
    if "datadome" in message:
        return "antibot_datadome_challenge"
    if "incapsula" in message:
        return "antibot_incapsula_challenge"
    if "queue_reentry" in message or "session_ip_change" in message:
        return "queue_reentry_session_or_ip_change"
    if "decline" in message or "payment_declined" in message:
        return "decline"
    return "generic_failure"


def _failure_class_from_signal(signal: str, retryable: bool) -> str:
    if signal.startswith("antibot_") or signal == "queue_reentry_session_or_ip_change":
        return "antibot"
    if signal == "decline":
        return "decline"
    if retryable:
        return "network"
    return "other"


def _status_hint_for_signal(signal: str) -> str | None:
    if signal.startswith("antibot_"):
        return "likely proxy reputation issue"
    if signal == "queue_reentry_session_or_ip_change":
        return "session expired, task requeued"
    return None


def _compute_retry_preset(
    *,
    step: str,
    failure_class: str,
    task_config: dict[str, Any],
) -> dict[str, float | int]:
    step_policy = CHECKOUT_STEP_RETRY_POLICY[step]
    preset = dict(CHECKOUT_RETRY_PRESETS.get(failure_class, CHECKOUT_RETRY_PRESETS["other"]))
    max_attempts = min(int(step_policy["max_attempts"]), int(preset["max_attempts"]))
    group_limits = task_config.get("group_limits") if isinstance(task_config.get("group_limits"), dict) else {}
    group_max = _coerce_optional_int(group_limits.get("max_retries"))
    if group_max is not None and group_max > 0:
        max_attempts = min(max_attempts, group_max)
    return {"max_attempts": max(1, max_attempts), "base_backoff_seconds": float(preset["base_backoff_seconds"])}


def _checkout_step_monitoring(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    monitor: sqlite3.Row | None,
    _config: dict[str, Any],
    _attempt_number: int,
) -> None:
    if monitor is None:
        raise ValueError("monitor_missing")


def _checkout_step_carting(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    _monitor: sqlite3.Row | None,
    _config: dict[str, Any],
    _attempt_number: int,
) -> None:
    return None


def _checkout_step_shipping(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    _monitor: sqlite3.Row | None,
    config: dict[str, Any],
    _attempt_number: int,
) -> None:
    if not config.get("profile"):
        raise ValueError("shipping_profile_missing")


def _checkout_step_payment(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    _monitor: sqlite3.Row | None,
    config: dict[str, Any],
    attempt_number: int,
) -> None:
    if not config.get("payment"):
        raise ValueError("payment_method_missing")
    fail_step = str(config.get("simulate_fail_step") or "")
    fail_times = int(config.get("simulate_fail_times") or 0)
    if fail_step in {"payment", "checking_out"} and attempt_number <= fail_times:
        if bool(config.get("simulate_retryable", True)):
            raise CheckoutRetryableError(fail_error)
        raise ValueError(fail_error or "simulated_payment_terminal_failure")


def _checkout_step_submitting(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    _monitor: sqlite3.Row | None,
    _config: dict[str, Any],
    _attempt_number: int,
) -> None:
    return None


CHECKOUT_STEP_HANDLERS = {
    "monitoring_product": _checkout_step_monitoring,
    "adding_to_cart": _checkout_step_carting,
    "checking_out": _checkout_step_payment,
}


def _resolve_task_binding_context(
    conn: sqlite3.Connection,
    *,
    workspace_id: int,
    monitor_id: int,
    config: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    context = {
        "profile": config.get("profile"),
        "account": config.get("account"),
        "payment": config.get("payment"),
    }
    errors: list[str] = []
    binding = conn.execute(
        """
        select * from task_profile_bindings
        where workspace_id = ? and monitor_id = ?
        """,
        (workspace_id, monitor_id),
    ).fetchone()
    if not binding:
        return context, errors

    if binding["checkout_profile_id"] is None:
        errors.append("binding_profile_missing")
    else:
        profile = conn.execute(
            "select * from checkout_profiles where id = ? and workspace_id = ?",
            (binding["checkout_profile_id"], workspace_id),
        ).fetchone()
        if not profile:
            errors.append("binding_profile_not_found")
        else:
            context["profile"] = profile["name"]

    if binding["retailer_account_id"] is None:
        errors.append("binding_account_missing")
    else:
        account = conn.execute(
            "select * from retailer_accounts where id = ? and workspace_id = ?",
            (binding["retailer_account_id"], workspace_id),
        ).fetchone()
        if not account:
            errors.append("binding_account_not_found")
        else:
            context["account"] = account["email"] or account["username"] or f"account-{account['id']}"

    if binding["payment_method_id"] is None:
        errors.append("binding_payment_missing")
    else:
        payment = conn.execute(
            "select * from payment_methods where id = ? and workspace_id = ?",
            (binding["payment_method_id"], workspace_id),
        ).fetchone()
        if not payment:
            errors.append("binding_payment_not_found")
        else:
            context["payment"] = payment["label"]

    return context, errors


def _validate_checkout_context_for_step(step: str, context: dict[str, Any], binding_errors: list[str]) -> str | None:
    if step != "checking_out":
        return None
    if binding_errors:
        return binding_errors[0]
    if not context.get("profile"):
        return "missing_profile_binding_or_config"
    if not context.get("account"):
        return "missing_account_binding_or_config"
    if not context.get("payment"):
        return "missing_payment_binding_or_config"
    return None


def _run_account_mode_pipeline(
    conn: sqlite3.Connection,
    *,
    task: sqlite3.Row,
    workspace_id: int,
    mode: str,
) -> sqlite3.Row:
    mode_steps = ["monitoring", "submitting"] if mode == "newsletter_subscribe" else ["monitoring", "shipping", "submitting"]
    for step in mode_steps:
        transitioned = transition_checkout_task(
            conn,
            task_id=task["id"],
            workspace_id=workspace_id,
            requested_state=step,
            reason=f"{mode}_step_start",
        )
        if transitioned:
            record_task_log(
                conn,
                task_id=task["id"],
                workspace_id=workspace_id,
                monitor_id=task["monitor_id"],
                level="info",
                event_type=f"{mode}_{step}",
                message=f"{mode} step '{step}' completed",
                payload={"mode": mode},
            )
    transition_checkout_task(
        conn,
        task_id=task["id"],
        workspace_id=workspace_id,
        requested_state="success",
        reason=f"{mode}_complete",
    )
    conn.commit()
    done = get_checkout_task_for_workspace(conn, task["id"], workspace_id)
    assert done is not None
    return done


def execute_checkout_task_state_machine(task_id: int, workspace_id: int) -> sqlite3.Row | None:
    conn = db()
    task = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    if not task:
        conn.close()
        return None

    config_raw = task["task_config"] or "{}"
    try:
        config = json.loads(config_raw)
    except (TypeError, json.JSONDecodeError):
        config = {}
    mode = _pokemon_center_mode(config) if str((monitor["retailer"] if monitor else "") or "").strip().lower() == "pokemoncenter" else "default"
    if monitor and monitor["retailer"] == "pokemoncenter":
        site_error = validate_pokemon_center_mode_site(mode, str(config.get("site") or "").strip().lower())
        if site_error:
            transition_checkout_task(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                requested_state="failed",
                reason=f"{mode}:validation",
                error_text=site_error,
            )
            conn.commit()
            failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
            conn.close()
            return failed
        requirement_error = validate_pokemon_center_mode_requirements(config)
        if requirement_error:
            transition_checkout_task(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                requested_state="failed",
                reason=f"{mode}:validation",
                error_text=requirement_error,
            )
            conn.commit()
            failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
            conn.close()
            return failed
    if mode in {"create_account", "newsletter_subscribe"}:
        done = _run_account_mode_pipeline(conn, task=task, workspace_id=workspace_id, mode=mode)
        conn.close()
        return done

    monitor = conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (task["monitor_id"], workspace_id),
    ).fetchone()
    mode = _pokemon_center_mode(config) if str((monitor["retailer"] if monitor else "") or "").strip().lower() == "pokemoncenter" else "default"
    if monitor and monitor["retailer"] == "pokemoncenter":
        site_error = validate_pokemon_center_mode_site(mode, str(config.get("site") or "").strip().lower())
        if site_error:
            transition_checkout_task(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                requested_state="failed",
                reason=f"{mode}:validation",
                error_text=site_error,
            )
            conn.commit()
            failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
            conn.close()
            return failed
        requirement_error = validate_pokemon_center_mode_requirements(config)
        if requirement_error:
            transition_checkout_task(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                requested_state="failed",
                reason=f"{mode}:validation",
                error_text=requirement_error,
            )
            conn.commit()
            failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
            conn.close()
            return failed
    if mode in {"create_account", "newsletter_subscribe"}:
        done = _run_account_mode_pipeline(conn, task=task, workspace_id=workspace_id, mode=mode)
        conn.close()
        return done

    binding_context, binding_errors = _resolve_task_binding_context(
        conn,
        workspace_id=workspace_id,
        monitor_id=task["monitor_id"],
        config=config,
    )
    config.update(binding_context)

    transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        requested_state="starting",
        reason="checkout_run_start",
    )
    for step in CHECKOUT_STEP_SEQUENCE:
        context_error = _validate_checkout_context_for_step(step, config, binding_errors)
        if context_error:
            transition_checkout_task(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                requested_state="error",
                reason=f"{step}:validation",
                error_text=context_error,
            )
            record_task_log(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                monitor_id=task["monitor_id"],
                level="error",
                event_type="binding_validation_failed",
                message=f"Checkout validation failed before {step}",
                payload={"error_code": context_error},
            )
            conn.commit()
            failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
            conn.close()
            return failed

        transitioned = transition_checkout_task(
            conn,
            task_id=task_id,
            workspace_id=workspace_id,
            requested_state=step,
            reason="checkout_step_start",
        )
        if not transitioned:
            conn.close()
            return None
        step_policy = CHECKOUT_STEP_RETRY_POLICY[step]
        for attempt_number in range(1, step_policy["max_attempts"] + 1):
            try:
                CHECKOUT_STEP_HANDLERS[step](conn, transitioned, monitor, config, attempt_number)
                record_checkout_attempt(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    monitor_id=task["monitor_id"],
                    state=step,
                    status="step_success",
                    details={"attempt_number": attempt_number},
                )
                record_task_log(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    monitor_id=task["monitor_id"],
                    level="info",
                    event_type="step_success",
                    message=f"{step} succeeded",
                    payload={"attempt_number": attempt_number},
                )
                break
            except Exception as exc:  # noqa: BLE001
                retryable, taxonomy = _classify_checkout_step_failure(step, exc, attempt_number)
                status_signal = _derive_status_signal(exc)
                failure_class = _failure_class_from_signal(status_signal, retryable)
                retry_preset = _compute_retry_preset(step=step, failure_class=failure_class, task_config=config)
                hint = _status_hint_for_signal(status_signal)
                should_retry = retryable and attempt_number < int(retry_preset["max_attempts"])
                backoff_seconds = float(retry_preset["base_backoff_seconds"]) * (2 ** max(attempt_number - 1, 0)) if should_retry else 0.0
                if failure_class == "antibot":
                    antibot_events += 1
                record_checkout_attempt(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    monitor_id=task["monitor_id"],
                    state=step,
                    status="step_failure",
                    details={
                        "attempt_number": attempt_number,
                        "taxonomy": taxonomy,
                        "retryable": should_retry,
                        "status_signal": status_signal,
                        "failure_class": failure_class,
                        "status_hint": hint,
                        "retry_preset": retry_preset,
                        "scheduled_backoff_seconds": round(backoff_seconds, 2),
                        "failure_reason": {
                            "step": step,
                            "taxonomy": taxonomy,
                            "status_signal": status_signal,
                            "failure_class": failure_class,
                            "exception_type": type(exc).__name__,
                        },
                    },
                    error_text=f"{taxonomy}:{exc}",
                )
                record_task_log(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    monitor_id=task["monitor_id"],
                    level="warning" if should_retry else "error",
                    event_type="step_failure",
                    message=f"{step} failed ({status_signal})",
                    payload={
                        "attempt_number": attempt_number,
                        "retryable": should_retry,
                        "failure_class": failure_class,
                        "status_hint": hint,
                    },
                )
                message = str(exc).lower()
                if "queue_session_expired" in message or "queue expired" in message:
                    transition_checkout_task(
                        conn,
                        task_id=task_id,
                        workspace_id=workspace_id,
                        requested_state="requeued",
                        reason=f"{step}:queue_session_expired",
                        error_text=f"{taxonomy}:{exc}",
                    )
                    transition_checkout_task(
                        conn,
                        task_id=task_id,
                        workspace_id=workspace_id,
                        requested_state="waiting_for_queue",
                        reason=f"{step}:waiting_for_new_queue",
                    )
                    continue
                if retryable:
                    continue
                transition_checkout_task(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    requested_state="decline",
                    reason=f"{step}:{taxonomy}",
                    error_text=f"{taxonomy}:{exc}",
                )
                conn.commit()
                failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
                conn.close()
                return failed

    transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        requested_state="success",
        reason="checkout_complete",
    )
    conn.commit()
    done = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    conn.close()
    return done


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def serialize_checkout_task(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    config_raw = payload.get("task_config")
    try:
        parsed_config = json.loads(config_raw) if config_raw else {}
    except (TypeError, json.JSONDecodeError):
        parsed_config = {}
    payload["task_config"] = normalize_task_config_for_monitor(
        parsed_config if isinstance(parsed_config, dict) else {},
    )
    return payload


def redact_webhook_url(value: str) -> str:
    if not value:
        return ""
    if len(value) <= 12:
        return "***"
    return f"{value[:8]}...{value[-4:]}"


def serialize_webhook(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    payload["webhook_url"] = redact_webhook_url(payload.get("webhook_url") or "")
    payload = redact_sensitive_payload(payload)
    return payload
def _classify_checkout_step_failure(step: str, exc: Exception, attempt_number: int) -> tuple[bool, str]:
    policy = CHECKOUT_STEP_RETRY_POLICY[step]
    if isinstance(exc, (ValueError, PermissionError)):
        return False, "terminal_validation_error"
    if attempt_number >= policy["max_attempts"]:
        return False, "terminal_max_attempts_exceeded"
    if isinstance(exc, (CheckoutRetryableError, requests.RequestException, TimeoutError, ConnectionError, sqlite3.OperationalError)):
        return True, "retryable_exception"
    return False, "terminal_unclassified"


def _checkout_step_monitoring(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    monitor: sqlite3.Row | None,
    _config: dict[str, Any],
    _attempt_number: int,
) -> None:
    if monitor is None:
        raise ValueError("monitor_missing")


def _checkout_step_carting(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    _monitor: sqlite3.Row | None,
    _config: dict[str, Any],
    _attempt_number: int,
) -> None:
    return None


def _checkout_step_shipping(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    _monitor: sqlite3.Row | None,
    config: dict[str, Any],
    _attempt_number: int,
) -> None:
    if not config.get("profile"):
        raise ValueError("shipping_profile_missing")


def _checkout_step_payment(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    _monitor: sqlite3.Row | None,
    config: dict[str, Any],
    attempt_number: int,
) -> None:
    if not config.get("payment"):
        raise ValueError("payment_method_missing")
    fail_step = str(config.get("simulate_fail_step") or "")
    fail_times = int(config.get("simulate_fail_times") or 0)
    if fail_step in {"payment", "checking_out"} and attempt_number <= fail_times:
        if bool(config.get("simulate_retryable", True)):
            raise CheckoutRetryableError(fail_error)
        raise ValueError(fail_error or "simulated_payment_terminal_failure")


def _checkout_step_submitting(
    _conn: sqlite3.Connection,
    _task: sqlite3.Row,
    _monitor: sqlite3.Row | None,
    _config: dict[str, Any],
    _attempt_number: int,
) -> None:
    return None


CHECKOUT_STEP_HANDLERS = {
    "monitoring_product": _checkout_step_monitoring,
    "adding_to_cart": _checkout_step_carting,
    "checking_out": _checkout_step_payment,
}


def _resolve_task_binding_context(
    conn: sqlite3.Connection,
    *,
    workspace_id: int,
    monitor_id: int,
    config: dict[str, Any],
) -> tuple[dict[str, Any], list[str]]:
    context = {
        "profile": config.get("profile"),
        "account": config.get("account"),
        "payment": config.get("payment"),
    }
    errors: list[str] = []
    binding = conn.execute(
        """
        select * from task_profile_bindings
        where workspace_id = ? and monitor_id = ?
        """,
        (workspace_id, monitor_id),
    ).fetchone()
    if not binding:
        return context, errors

    if binding["checkout_profile_id"] is None:
        errors.append("binding_profile_missing")
    else:
        profile = conn.execute(
            "select * from checkout_profiles where id = ? and workspace_id = ?",
            (binding["checkout_profile_id"], workspace_id),
        ).fetchone()
        if not profile:
            errors.append("binding_profile_not_found")
        else:
            context["profile"] = profile["name"]

    if binding["retailer_account_id"] is None:
        errors.append("binding_account_missing")
    else:
        account = conn.execute(
            "select * from retailer_accounts where id = ? and workspace_id = ?",
            (binding["retailer_account_id"], workspace_id),
        ).fetchone()
        if not account:
            errors.append("binding_account_not_found")
        else:
            context["account"] = account["email"] or account["username"] or f"account-{account['id']}"

    if binding["payment_method_id"] is None:
        errors.append("binding_payment_missing")
    else:
        payment = conn.execute(
            "select * from payment_methods where id = ? and workspace_id = ?",
            (binding["payment_method_id"], workspace_id),
        ).fetchone()
        if not payment:
            errors.append("binding_payment_not_found")
        else:
            context["payment"] = payment["label"]

    return context, errors


def _validate_checkout_context_for_step(step: str, context: dict[str, Any], binding_errors: list[str]) -> str | None:
    if step != "checking_out":
        return None
    if binding_errors:
        return binding_errors[0]
    if not context.get("profile"):
        return "missing_profile_binding_or_config"
    if not context.get("account"):
        return "missing_account_binding_or_config"
    if not context.get("payment"):
        return "missing_payment_binding_or_config"
    return None


def execute_checkout_task_state_machine(task_id: int, workspace_id: int) -> sqlite3.Row | None:
    conn = db()
    task = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    if not task:
        conn.close()
        return None

    config_raw = task["task_config"] or "{}"
    try:
        config = json.loads(config_raw)
    except (TypeError, json.JSONDecodeError):
        config = {}
    monitor = conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (task["monitor_id"], workspace_id),
    ).fetchone()
    mode = _pokemon_center_mode(config) if str((monitor["retailer"] if monitor else "") or "").strip().lower() == "pokemoncenter" else "default"
    if monitor and monitor["retailer"] == "pokemoncenter":
        site_error = validate_pokemon_center_mode_site(mode, str(config.get("site") or "").strip().lower())
        if site_error:
            transition_checkout_task(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                requested_state="failed",
                reason=f"{mode}:validation",
                error_text=site_error,
            )
            conn.commit()
            failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
            conn.close()
            return failed
        requirement_error = validate_pokemon_center_mode_requirements(config)
        if requirement_error:
            transition_checkout_task(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                requested_state="failed",
                reason=f"{mode}:validation",
                error_text=requirement_error,
            )
            conn.commit()
            failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
            conn.close()
            return failed
    if mode in {"create_account", "newsletter_subscribe"}:
        done = _run_account_mode_pipeline(conn, task=task, workspace_id=workspace_id, mode=mode)
        conn.close()
        return done

    binding_context, binding_errors = _resolve_task_binding_context(
        conn,
        workspace_id=workspace_id,
        monitor_id=task["monitor_id"],
        config=config,
    )
    config.update(binding_context)

    transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        requested_state="starting",
        reason="checkout_run_start",
    )
    for step in CHECKOUT_STEP_SEQUENCE:
        context_error = _validate_checkout_context_for_step(step, config, binding_errors)
        if context_error:
            transition_checkout_task(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                requested_state="decline",
                reason=f"{step}:validation",
                error_text=context_error,
            )
            record_task_log(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                monitor_id=task["monitor_id"],
                level="error",
                event_type="binding_validation_failed",
                message=f"Checkout validation failed before {step}",
                payload={"error_code": context_error},
            )
            conn.commit()
            failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
            conn.close()
            return failed

        transitioned = transition_checkout_task(
            conn,
            task_id=task_id,
            workspace_id=workspace_id,
            requested_state=step,
            reason="checkout_step_start",
        )
        if not transitioned:
            conn.close()
            return None
        step_policy = CHECKOUT_STEP_RETRY_POLICY[step]
        for attempt_number in range(1, step_policy["max_attempts"] + 1):
            try:
                CHECKOUT_STEP_HANDLERS[step](conn, transitioned, monitor, config, attempt_number)
                record_checkout_attempt(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    monitor_id=task["monitor_id"],
                    state=step,
                    status="step_success",
                    details={"attempt_number": attempt_number},
                )
                record_task_log(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    monitor_id=task["monitor_id"],
                    level="info",
                    event_type="step_success",
                    message=f"{step} succeeded",
                    payload={"attempt_number": attempt_number},
                )
                break
            except Exception as exc:  # noqa: BLE001
                retryable, taxonomy = _classify_checkout_step_failure(step, exc, attempt_number)
                status_signal = _derive_status_signal(exc)
                failure_class = _failure_class_from_signal(status_signal, retryable)
                retry_preset = _compute_retry_preset(step=step, failure_class=failure_class, task_config=config)
                hint = _status_hint_for_signal(status_signal)
                should_retry = retryable and attempt_number < int(retry_preset["max_attempts"])
                backoff_seconds = float(retry_preset["base_backoff_seconds"]) * (2 ** max(attempt_number - 1, 0)) if should_retry else 0.0
                if failure_class == "antibot":
                    antibot_events += 1
                record_checkout_attempt(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    monitor_id=task["monitor_id"],
                    state=step,
                    status="step_failure",
                    details={
                        "attempt_number": attempt_number,
                        "taxonomy": taxonomy,
                        "retryable": should_retry,
                        "status_signal": status_signal,
                        "failure_class": failure_class,
                        "status_hint": hint,
                        "retry_preset": retry_preset,
                        "scheduled_backoff_seconds": round(backoff_seconds, 2),
                        "failure_reason": {
                            "step": step,
                            "taxonomy": taxonomy,
                            "status_signal": status_signal,
                            "failure_class": failure_class,
                            "exception_type": type(exc).__name__,
                        },
                    },
                    error_text=f"{taxonomy}:{exc}",
                )
                record_task_log(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    monitor_id=task["monitor_id"],
                    level="warning" if should_retry else "error",
                    event_type="step_failure",
                    message=f"{step} failed ({status_signal})",
                    payload={
                        "attempt_number": attempt_number,
                        "retryable": should_retry,
                        "failure_class": failure_class,
                        "status_hint": hint,
                    },
                )
                message = str(exc).lower()
                if "queue_session_expired" in message or "queue expired" in message:
                    transition_checkout_task(
                        conn,
                        task_id=task_id,
                        workspace_id=workspace_id,
                        requested_state="requeued",
                        reason=f"{step}:queue_session_expired",
                        error_text=f"{taxonomy}:{exc}",
                    )
                    transition_checkout_task(
                        conn,
                        task_id=task_id,
                        workspace_id=workspace_id,
                        requested_state="waiting_for_queue",
                        reason=f"{step}:waiting_for_new_queue",
                    )
                    continue
                if retryable:
                    continue
                transition_checkout_task(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    requested_state="decline",
                    reason=f"{step}:{taxonomy}",
                    error_text=f"{taxonomy}:{exc}",
                )
                conn.commit()
                failed = get_checkout_task_for_workspace(conn, task_id, workspace_id)
                conn.close()
                return failed

    transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        requested_state="success",
        reason="checkout_complete",
    )
    conn.commit()
    done = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    conn.close()
    return done


def enqueue_checkout_for_monitor(
    monitor: sqlite3.Row, result: MonitorResult, *, reason: str = "in_stock_detected"
) -> int | None:
    if not result.in_stock:
        return None
    conn = db()
    binding_ok, binding_error = _require_checkout_binding(
        conn,
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
        mode="default",
    )
    if not binding_ok:
        log(
            f"Skipping checkout enqueue for monitor {monitor['id']}: {binding_error}",
            level="warning",
            workspace_id=monitor["workspace_id"],
            monitor_id=monitor["id"],
        )
        conn.close()
        return None
    existing = conn.execute(
        """
        select * from checkout_tasks
        where workspace_id = ? and monitor_id = ?
          and current_state not in ('success', 'decline', 'error', 'stopped')
        order by id desc
        limit 1
        """,
        (monitor["workspace_id"], monitor["id"]),
    ).fetchone()
    if existing:
        record_task_log(
            conn,
            task_id=existing["id"],
            workspace_id=monitor["workspace_id"],
            monitor_id=monitor["id"],
            level="info",
            event_type="enqueue_skipped_existing",
            message=f"Existing active checkout task {existing['id']} detected",
            payload={"reason": reason},
        )
        conn.commit()
        conn.close()
        return existing["id"]

    task_config = {
        "retailer": monitor["retailer"],
        "category": monitor["category"],
        "product_url": monitor["product_url"],
    }
    initial_state = "waiting_for_queue" if bool(task_config.get("wait_for_queue")) else "queued"
    task = create_checkout_task(
        conn,
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
        task_name=f"Monitor {monitor['id']} checkout",
        task_config=task_config,
        initial_state=initial_state,
    )
    record_checkout_attempt(
        conn,
        task_id=task["id"],
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
        state=initial_state,
        status="enqueued",
        details={
            "reason": reason,
            "title": result.title,
            "price_cents": result.price_cents,
        },
    )
    record_task_log(
        conn,
        task_id=task["id"],
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
        level="warning",
        event_type="task_enqueued",
        message=f"Checkout task {task['id']} enqueued from monitor {monitor['id']}",
        payload={"price_cents": result.price_cents, "title": result.title},
    )
    conn.commit()
    conn.close()
    return int(task["id"])


def _checkout_binding_for_monitor(
    conn: sqlite3.Connection, *, workspace_id: int, monitor_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        """
        select b.*, a.proxy_url as account_proxy_url
        from task_profile_bindings b
        left join retailer_accounts a
          on a.id = b.retailer_account_id
         and a.workspace_id = b.workspace_id
        where b.workspace_id = ? and b.monitor_id = ?
        """,
        (workspace_id, monitor_id),
    ).fetchone()


def _require_checkout_binding(
    conn: sqlite3.Connection, *, workspace_id: int, monitor_id: int, mode: str
) -> tuple[bool, str | None]:
    if mode in {"create_account", "newsletter_subscribe"}:
        return True, None
    binding = _checkout_binding_for_monitor(conn, workspace_id=workspace_id, monitor_id=monitor_id)
    if not binding:
        return False, "Task binding is required before creating checkout tasks"
    if binding["retailer_account_id"] is None:
        return False, "Task binding must include retailer_account_id for checkout-capable tasks"
    account = conn.execute(
        "select id from retailer_accounts where id = ? and workspace_id = ?",
        (binding["retailer_account_id"], workspace_id),
    ).fetchone()
    if not account:
        return False, "retailer_account_id in task binding does not exist"
    return True, None


def cents_to_dollars(cents: int | None) -> str:
    if cents is None:
        return "unknown"
    return f"${cents / 100:.2f}"


def create_secret(
    conn: sqlite3.Connection,
    workspace_id: int,
    secret_type: str,
    plaintext: str,
    user_id: int | None = None,
) -> int:
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, created_at, updated_at)
        values (?, ?, ?, ?, ?, ?)
        """,
        (workspace_id, user_id, secret_type, encrypt_secret_value(plaintext), now_iso, now_iso),
    )
    return int(cur.lastrowid)


def resolve_webhook_url(conn: sqlite3.Connection, webhook: sqlite3.Row) -> str:
    secret_id = webhook["webhook_secret_id"]
    if secret_id:
        row = conn.execute(
            "select ciphertext from account_secrets where id = ? and workspace_id = ?",
            (secret_id, webhook["workspace_id"]),
        ).fetchone()
        if row and row["ciphertext"]:
            try:
                return decrypt_secret_value(row["ciphertext"])
            except ValueError:
                pass
    return str(webhook["webhook_url"] or "")


def serialize_checkout_task(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    config_raw = payload.get("task_config")
    try:
        parsed_config = json.loads(config_raw) if config_raw else {}
    except (TypeError, json.JSONDecodeError):
        parsed_config = {}
    payload["task_config"] = normalize_task_config_for_monitor(
        parsed_config if isinstance(parsed_config, dict) else {},
    )
    return payload


def serialize_checkout_task_summary(row: sqlite3.Row | None) -> dict[str, Any] | None:
    return serialize_task_ui(row)


def serialize_task_ui(row: sqlite3.Row | None) -> dict[str, Any] | None:
    payload = serialize_checkout_task_summary(row)
    if payload is None:
        return None
    payload["state"] = payload["current_state"]
    payload["retries"] = 0
    payload["last_step"] = payload["current_state"]
    return payload


def serialize_webhook(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    config = payload.get("task_config") or {}
    current_state = str(payload.get("current_state") or "queued")
    state = "running" if current_state == "monitoring" else ("idle" if current_state in {"queued", "waiting_for_queue"} else current_state)
    return {
        "id": payload["id"],
        "state": state,
        "retries": 0,
        "last_step": current_state,
        "last_error": payload.get("last_error"),
        "retailer": config.get("retailer"),
        "product_url": config.get("product_url"),
        "profile": config.get("profile"),
        "account": config.get("account"),
        "payment": config.get("payment"),
    }


def serialize_webhook(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    payload["webhook_url"] = redact_webhook_url(payload.get("webhook_url") or "")
    payload = redact_sensitive_payload(payload)
    return payload


def serialize_checkout_profile(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    payload["shipping_address"] = json.loads(payload.pop("shipping_address_json"))
    payload["billing_address"] = json.loads(payload.pop("billing_address_json"))
    return payload


def serialize_payment_method(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def serialize_retailer_account(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


def serialize_task_profile_binding(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    return dict(row)


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^[0-9+\-().\s]{7,24}$")
SESSION_STATUSES = {"active", "expired", "locked", "logged_out"}
BEHAVIOR_PROFILE_NAME_RE = re.compile(r"^[a-z0-9][a-z0-9_\-]{0,31}$")

DEFAULT_REQUEST_BEHAVIOR_POLICY: dict[str, Any] = {
    "profile": "default",
    "base_delay_seconds": 0.2,
    "jitter_ratio": 0.2,
    "min_delay_seconds": 0.05,
    "max_delay_seconds": 2.5,
    "adaptive_backoff_enabled": True,
    "adaptive_backoff_step_seconds": 0.4,
    "adaptive_backoff_cap_seconds": 5.0,
    "retailer_profiles": {
        "walmart": {"profile": "walmart", "base_delay_seconds": 0.3, "max_delay_seconds": 3.0},
        "target": {"profile": "target", "base_delay_seconds": 0.25, "max_delay_seconds": 2.5},
        "bestbuy": {"profile": "bestbuy", "base_delay_seconds": 0.2, "max_delay_seconds": 2.0},
        "pokemoncenter": {"profile": "pokemoncenter", "base_delay_seconds": 0.35, "max_delay_seconds": 3.5},
    },
}


def _validate_email(email: str) -> str:
    normalized = email.strip().lower()
    if not EMAIL_RE.match(normalized):
        raise ValueError("Invalid email")
    return normalized


def _validate_phone(phone: str | None) -> str | None:
    if phone is None:
        return None
    normalized = phone.strip()
    if not normalized:
        return None
    if not PHONE_RE.match(normalized):
        raise ValueError("Invalid phone")
    return normalized


def _validate_address(value: Any, field_name: str) -> dict[str, Any]:
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    required = ("line1", "city", "state", "postal_code", "country")
    for key in required:
        raw = value.get(key)
        if not isinstance(raw, str) or not raw.strip():
            raise ValueError(f"{field_name}.{key} is required")
    return {k: (v.strip() if isinstance(v, str) else v) for k, v in value.items()}


def _validate_json_object(value: Any, *, field_name: str, max_length: int = 4000) -> dict[str, Any] | None:
    if value is None:
        return None
    if not isinstance(value, dict):
        raise ValueError(f"{field_name} must be an object")
    serialized = json.dumps(value)
    if len(serialized) > max_length:
        raise ValueError(f"{field_name} exceeds {max_length} bytes")
    return value


def _validate_behavior_policy(value: Any, *, field_name: str = "behavior_metadata") -> dict[str, Any] | None:
    metadata = _validate_json_object(value, field_name=field_name)
    if metadata is None:
        return None
    for numeric_key in (
        "base_delay_seconds",
        "jitter_ratio",
        "min_delay_seconds",
        "max_delay_seconds",
        "adaptive_backoff_step_seconds",
        "adaptive_backoff_cap_seconds",
    ):
        if numeric_key in metadata:
            numeric_value = float(metadata[numeric_key])
            if numeric_value < 0:
                raise ValueError(f"{field_name}.{numeric_key} must be >= 0")
            metadata[numeric_key] = numeric_value
    if "jitter_ratio" in metadata and not 0 <= float(metadata["jitter_ratio"]) <= 1:
        raise ValueError(f"{field_name}.jitter_ratio must be between 0 and 1")
    if "adaptive_backoff_enabled" in metadata:
        metadata["adaptive_backoff_enabled"] = bool(metadata["adaptive_backoff_enabled"])
    if "profile" in metadata:
        profile = str(metadata["profile"]).strip().lower()
        if not BEHAVIOR_PROFILE_NAME_RE.match(profile):
            raise ValueError(f"{field_name}.profile has invalid format")
        metadata["profile"] = profile
    if "retailer_profiles" in metadata:
        retailer_profiles = metadata["retailer_profiles"]
        if not isinstance(retailer_profiles, dict):
            raise ValueError(f"{field_name}.retailer_profiles must be an object")
        normalized_profiles: dict[str, dict[str, Any]] = {}
        for retailer, profile_cfg in retailer_profiles.items():
            normalized_retailer = canonical_retailer(str(retailer).strip())
            if normalized_retailer not in SUPPORTED_RETAILERS:
                raise ValueError(f"{field_name}.retailer_profiles has unsupported retailer '{retailer}'")
            if not isinstance(profile_cfg, dict):
                raise ValueError(f"{field_name}.retailer_profiles.{retailer} must be an object")
            normalized_profiles[normalized_retailer] = _validate_behavior_policy(
                profile_cfg, field_name=f"{field_name}.retailer_profiles.{normalized_retailer}"
            ) or {}
        metadata["retailer_profiles"] = normalized_profiles
    if "min_delay_seconds" in metadata and "max_delay_seconds" in metadata:
        if float(metadata["max_delay_seconds"]) < float(metadata["min_delay_seconds"]):
            raise ValueError(f"{field_name}.max_delay_seconds must be >= min_delay_seconds")
    return metadata


def _build_request_behavior_policy(monitor: sqlite3.Row, workspace: sqlite3.Row | None) -> RequestBehaviorPolicy:
    merged: dict[str, Any] = json.loads(json.dumps(DEFAULT_REQUEST_BEHAVIOR_POLICY))
    for raw in (workspace["behavior_metadata"] if workspace else None, monitor["behavior_metadata"]):
        if not raw:
            continue
        try:
            candidate = json.loads(raw)
        except (TypeError, json.JSONDecodeError):
            continue
        if isinstance(candidate, dict):
            merged.update(candidate)
    return RequestBehaviorPolicy.from_mapping(merged).for_retailer(monitor["retailer"])


def should_send_to_webhook(monitor: sqlite3.Row, hook: sqlite3.Row, eligible: bool) -> bool:
    if not hook["enabled"]:
        return False
    if eligible and not hook["notify_success"]:
        return False
    if not eligible and not hook["notify_failures"]:
        return False
    if hook["notify_restock_only"] and monitor["last_in_stock"]:
        return False
    return True


def update_webhook_health(
    conn: sqlite3.Connection,
    webhook_id: int,
    status: str,
    status_code: int | None = None,
    error_text: str | None = None,
    tested: bool = False,
) -> None:
    now_iso = utc_now()
    tested_at = now_iso if tested else None
    conn.execute(
        """
        update webhooks
        set last_tested_at = coalesce(?, last_tested_at),
            last_test_status = coalesce(?, last_test_status),
            last_delivery_status = ?,
            last_delivery_at = ?,
            fail_streak = case when ? = 'failed' then fail_streak + 1 else 0 end,
            last_error = ?,
            last_status_code = ?
        where id = ?
        """,
        (
            tested_at,
            status if tested else None,
            status,
            now_iso,
            status,
            (error_text or "")[:500] if error_text else None,
            status_code,
            webhook_id,
        ),
    )


def create_monitor_error_events(monitor: sqlite3.Row, error_text: str) -> None:
    log(
        f"Monitor {monitor['id']} error event: {error_text[:200]}",
        level="warning",
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
    )


def emit_monitor_events(monitor: sqlite3.Row, result: MonitorResult, eligible: bool) -> None:
    payload = {
        "monitor_id": monitor["id"],
        "workspace_id": monitor["workspace_id"],
        "retailer": monitor["retailer"],
        "status_text": result.status_text,
        "eligible_for_alert": bool(eligible),
        "in_stock": bool(result.in_stock),
        "price_cents": result.price_cents,
        "availability_reason": result.availability_reason,
        "parser_confidence": result.parser_confidence,
        "queue_detected": bool(getattr(result, "queue_detected", False)),
        "checked_at": utc_now(),
    }
    try:
        socketio.emit("monitor_update", payload)
    except Exception as exc:  # noqa: BLE001
        # Best-effort telemetry; never fail monitor execution because of socket emit issues.
        print(json.dumps(format_log_entry("warning", f"monitor_update_emit_failed: {exc}", workspace_id=monitor["workspace_id"], monitor_id=monitor["id"])))
    try:
        log(
            f"Monitor {monitor['id']} telemetry emitted ({result.status_text}, eligible={int(bool(eligible))})",
            level="info",
            workspace_id=monitor["workspace_id"],
            monitor_id=monitor["id"],
        )
    except Exception as exc:  # noqa: BLE001
        print(json.dumps(format_log_entry("warning", f"monitor_telemetry_log_failed: {exc}", workspace_id=monitor["workspace_id"], monitor_id=monitor["id"])))


STEP_RETRY_POLICY = {
    "fetch": {"max_attempts": 5, "base_seconds": 5},
    "persist": {"max_attempts": 4, "base_seconds": 2},
    "notify": {"max_attempts": 4, "base_seconds": 3},
}
RETRYABLE_EXCEPTIONS = (requests.RequestException, TimeoutError, ConnectionError, sqlite3.OperationalError)


def _exponential_backoff_seconds(base_seconds: int, attempt_count: int) -> int:
    return base_seconds * (2 ** max(attempt_count - 1, 0))


def _classify_step_failure(step: str, exc: Exception, step_attempts: int) -> tuple[bool, str]:
    if isinstance(exc, (PermissionError, ValueError)):
        return False, "terminal_non_retryable_error"
    policy = STEP_RETRY_POLICY[step]
    if step_attempts >= policy["max_attempts"]:
        return False, "terminal_max_attempts_exceeded"
    return isinstance(exc, RETRYABLE_EXCEPTIONS), "retryable_exception" if isinstance(exc, RETRYABLE_EXCEPTIONS) else "terminal_unclassified"


def persist_monitor_state(monitor: sqlite3.Row, result: MonitorResult, eligible: bool) -> None:
    conn = db()
    conn.execute(
        """
        update monitors
        set last_checked_at = ?, last_in_stock = ?, last_price_cents = ?, failure_streak = 0, last_error = NULL
        where id = ?
        """,
        (utc_now(), int(eligible), result.price_cents, monitor["id"]),
    )
    conn.commit()
    conn.close()

def _handle_queue_detected_for_waiting_tasks(monitor: sqlite3.Row, result: MonitorResult) -> None:
    if monitor["retailer"] != "pokemoncenter" or not bool(getattr(result, "queue_detected", False)):
        return
    conn = db()
    waiting_tasks = conn.execute(
        """
        select *
        from checkout_tasks
        where workspace_id = ?
          and monitor_id = ?
          and current_state = 'waiting_for_queue'
          and is_paused = 0
        order by id asc
        """,
        (monitor["workspace_id"], monitor["id"]),
    ).fetchall()
    transitioned = 0
    for task in waiting_tasks:
        task_config = parse_json_object(task["task_config"])
        if not bool(task_config.get("wait_for_queue")):
            continue
        delay_ms = _coerce_optional_int(task_config.get("queue_entry_delay_ms"))
        if delay_ms and delay_ms > 0:
            time.sleep(delay_ms / 1000)
        updated = transition_checkout_task(
            conn,
            task_id=task["id"],
            workspace_id=task["workspace_id"],
            requested_state="monitoring",
            reason="queue_detected_event",
        )
        if updated:
            transitioned += 1
    if transitioned:
        conn.commit()
    conn.close()


def process_post_persist_actions(
    monitor: sqlite3.Row,
    result: MonitorResult,
    eligible: bool,
    *,
    strict: bool,
) -> None:
    _handle_queue_detected_for_waiting_tasks(monitor, result)
    if eligible:
        try:
            enqueue_checkout_for_monitor(monitor, result)
        except Exception as exc:  # noqa: BLE001
            if strict:
                raise
            log(
                f"Checkout enqueue failed for monitor {monitor['id']}: {exc}",
                level="warning",
                workspace_id=monitor["workspace_id"],
                monitor_id=monitor["id"],
            )

    try:
        create_event_and_deliver(monitor, result, eligible)
    except Exception as exc:  # noqa: BLE001
        if strict:
            raise
        log(
            f"Event/webhook notify failed for monitor {monitor['id']}: {exc}",
            level="warning",
            workspace_id=monitor["workspace_id"],
            monitor_id=monitor["id"],
        )

    try:
        emit_monitor_events(monitor, result, eligible)
    except Exception as exc:  # noqa: BLE001
        if strict:
            raise
        log(
            f"Telemetry emit failed for monitor {monitor['id']}: {exc}",
            level="warning",
            workspace_id=monitor["workspace_id"],
            monitor_id=monitor["id"],
        )


def run_monitor_pipeline_once(monitor: sqlite3.Row) -> dict[str, Any]:
    result = fetch_monitor(monitor)
    eligible = alert_eligibility(monitor, result)
    persist_monitor_state(monitor, result, eligible)
    process_post_persist_actions(monitor, result, eligible, strict=False)

    log(
        f"Checked monitor {monitor['id']} | {monitor['retailer']} | {result.status_text} | {cents_to_dollars(result.price_cents)}",
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
    )
    return {
        "ok": True,
        "in_stock": result.in_stock,
        "eligible_for_alert": eligible,
        "price_cents": result.price_cents,
        "availability_reason": result.availability_reason,
        "parser_confidence": result.parser_confidence,
        "keyword_matched": result.keyword_matched,
        "price_within_limit": result.price_within_limit,
        "within_msrp_delta": result.within_msrp_delta,
        "title": result.title,
    }


def check_monitor_once(monitor: sqlite3.Row) -> dict[str, Any]:
    try:
        return run_monitor_pipeline_once(monitor)
    except Exception as exc:  # noqa: BLE001
        conn = db()
        conn.execute(
            """
            insert into monitor_failures(monitor_id, workspace_id, error_text, failed_at)
            values (?, ?, ?, ?)
            """,
            (monitor["id"], monitor["workspace_id"], str(exc)[:500], utc_now()),
        )
        conn.execute(
            """
            update monitors
            set last_checked_at = ?, failure_streak = failure_streak + 1, last_error = ?
            where id = ?
            """,
            (utc_now(), str(exc)[:500], monitor["id"]),
        )
        conn.commit()
        conn.close()
        create_monitor_error_events(monitor, str(exc))
        log(
            f"Monitor {monitor['id']} fetch failed: {exc}",
            level="error",
            workspace_id=monitor["workspace_id"],
            monitor_id=monitor["id"],
        )
        return {"ok": False, "error": str(exc)}


def apply_due_schedules(conn: sqlite3.Connection) -> None:
    now_iso = utc_now()
    rows = conn.execute(
        """
        select * from monitor_schedules
        where applied_at is null and run_at <= ?
        order by id asc
        """,
        (now_iso,),
    ).fetchall()
    for row in rows:
        conn.execute(
            "update monitors set poll_interval_seconds = ? where id = ?",
            (row["new_poll_interval_seconds"], row["monitor_id"]),
        )
        conn.execute("update monitor_schedules set applied_at = ? where id = ?", (now_iso, row["id"]))
        log(
            f"Applied schedule {row['id']} for monitor {row['monitor_id']} (poll={row['new_poll_interval_seconds']}s)"
        )


class SQLiteJobQueue:
    def __init__(self, conn: sqlite3.Connection, worker_id: str):
        self.conn = conn
        self.worker_id = worker_id

    def enqueue_monitor_check_if_due(self, monitor: sqlite3.Row, *, now_iso: str) -> None:
        now_ts = datetime.now(timezone.utc).timestamp()
        if monitor["last_checked_at"]:
            elapsed = now_ts - datetime.fromisoformat(monitor["last_checked_at"]).timestamp()
            if elapsed < monitor["poll_interval_seconds"]:
                return
        exists = self.conn.execute(
            """
            select id
            from jobs
            where monitor_id = ?
              and job_type = 'monitor_check'
              and status in ('queued', 'retrying', 'running')
            limit 1
            """,
            (monitor["id"],),
        ).fetchone()
        if exists:
            return
        enqueue_jitter = random.uniform(0.0, max(QUEUE_ENQUEUE_JITTER_SECONDS, 0.0))
        scheduled_ts = now_ts + enqueue_jitter
        scheduled_iso = datetime.fromtimestamp(scheduled_ts, tz=timezone.utc).isoformat()
        payload = json.dumps(
            {
                "step_attempts": {},
                "pacing": {
                    "enqueue_jitter_ms": int(enqueue_jitter * 1000),
                    "scheduled_from_iso": now_iso,
                    "scheduled_for_iso": scheduled_iso,
                },
            }
        )
        self.conn.execute(
            """
            insert into jobs(job_type, monitor_id, status, attempt_count, next_run_at, payload_json, created_at, updated_at)
            values ('monitor_check', ?, 'queued', 0, ?, ?, ?, ?)
            """,
            (monitor["id"], scheduled_iso, payload, now_iso, now_iso),
        )

    def claim_due_job(self, *, now_iso: str) -> Job | None:
        stale_cutoff = datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() - WORKER_LOCK_TIMEOUT_SECONDS, tz=timezone.utc
        ).isoformat()
        self.conn.execute("begin immediate")
        row = self.conn.execute(
            """
            select *
            from jobs
            where status in ('queued', 'retrying')
              and next_run_at <= ?
              and (
                locked_at is null
                or locked_at < ?
              )
            order by next_run_at asc, id asc
            limit 1
            """,
            (now_iso, stale_cutoff),
        ).fetchone()
        if not row:
            self.conn.commit()
            return None
        self.conn.execute(
            """
            update jobs
            set status = 'running', locked_by = ?, locked_at = ?, updated_at = ?
            where id = ?
            """,
            (self.worker_id, now_iso, now_iso, row["id"]),
        )
        self.conn.commit()
        return Job(**dict(row))

    def complete_job(self, job_id: int, *, now_iso: str) -> None:
        self.conn.execute(
            """
            update jobs
            set status = 'completed', locked_by = null, locked_at = null, updated_at = ?
            where id = ?
            """,
            (now_iso, job_id),
        )

    def fail_job(self, job_id: int, *, now_iso: str, status: str, next_run_at: str, payload_json: str, error_text: str) -> None:
        self.conn.execute(
            """
            update jobs
            set status = ?,
                attempt_count = attempt_count + 1,
                next_run_at = ?,
                payload_json = ?,
                last_error = ?,
                locked_by = null,
                locked_at = null,
                updated_at = ?
            where id = ?
            """,
            (status, next_run_at, payload_json, error_text[:500], now_iso, job_id),
        )


def execute_monitor_job(queue: SQLiteJobQueue, job: Job, *, now_iso: str) -> None:
    monitor = queue.conn.execute("select * from monitors where id = ?", (job.monitor_id,)).fetchone()
    if not monitor:
        queue.fail_job(
            job.id,
            now_iso=now_iso,
            status="failed",
            next_run_at=now_iso,
            payload_json=job.payload_json or "{}",
            error_text=f"monitor_not_found:{job.monitor_id}",
        )
        return
    payload = json.loads(job.payload_json or "{}")
    step_attempts: dict[str, int] = payload.get("step_attempts") or {}
    result: MonitorResult | None = None
    eligible: bool | None = None
    try:
        result = fetch_monitor(monitor)
    except Exception as exc:  # noqa: BLE001
        step = "fetch"
        step_attempts[step] = int(step_attempts.get(step, 0)) + 1
        retryable, reason = _classify_step_failure(step, exc, step_attempts[step])
        if retryable:
            delay = _exponential_backoff_seconds(STEP_RETRY_POLICY[step]["base_seconds"], step_attempts[step])
            next_run = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + delay, tz=timezone.utc).isoformat()
            queue.fail_job(
                job.id,
                now_iso=now_iso,
                status="retrying",
                next_run_at=next_run,
                payload_json=json.dumps({"step_attempts": step_attempts}),
                error_text=f"{step}:{reason}:{exc}",
            )
            return
        queue.fail_job(
            job.id,
            now_iso=now_iso,
            status="failed",
            next_run_at=now_iso,
            payload_json=json.dumps({"step_attempts": step_attempts}),
            error_text=f"{step}:{reason}:{exc}",
        )
        return
    try:
        eligible = alert_eligibility(monitor, result)
        persist_monitor_state(monitor, result, eligible)
    except Exception as exc:  # noqa: BLE001
        step = "persist"
        step_attempts[step] = int(step_attempts.get(step, 0)) + 1
        retryable, reason = _classify_step_failure(step, exc, step_attempts[step])
        if retryable:
            delay = _exponential_backoff_seconds(STEP_RETRY_POLICY[step]["base_seconds"], step_attempts[step])
            next_run = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + delay, tz=timezone.utc).isoformat()
            queue.fail_job(
                job.id,
                now_iso=now_iso,
                status="retrying",
                next_run_at=next_run,
                payload_json=json.dumps({"step_attempts": step_attempts}),
                error_text=f"{step}:{reason}:{exc}",
            )
            return
        queue.fail_job(
            job.id,
            now_iso=now_iso,
            status="failed",
            next_run_at=now_iso,
            payload_json=json.dumps({"step_attempts": step_attempts}),
            error_text=f"{step}:{reason}:{exc}\n{traceback.format_exc()}",
        )
        return
    try:
        process_post_persist_actions(monitor, result, eligible, strict=True)
        log(
            f"Checked monitor {monitor['id']} | {monitor['retailer']} | {result.status_text} | {cents_to_dollars(result.price_cents)}",
            workspace_id=monitor["workspace_id"],
            monitor_id=monitor["id"],
        )
        queue.complete_job(job.id, now_iso=now_iso)
    except Exception as exc:  # noqa: BLE001
        step = "notify"
        step_attempts[step] = int(step_attempts.get(step, 0)) + 1
        retryable, reason = _classify_step_failure(step, exc, step_attempts[step])
        if retryable:
            delay = _exponential_backoff_seconds(STEP_RETRY_POLICY[step]["base_seconds"], step_attempts[step])
            next_run = datetime.fromtimestamp(datetime.now(timezone.utc).timestamp() + delay, tz=timezone.utc).isoformat()
            queue.fail_job(
                job.id,
                now_iso=now_iso,
                status="retrying",
                next_run_at=next_run,
                payload_json=json.dumps({"step_attempts": step_attempts}),
                error_text=f"{step}:{reason}:{exc}",
            )
            return
        queue.fail_job(
            job.id,
            now_iso=now_iso,
            status="failed",
            next_run_at=now_iso,
            payload_json=json.dumps({"step_attempts": step_attempts}),
            error_text=f"{step}:{reason}:{exc}\n{traceback.format_exc()}",
        )


def _active_checkout_states() -> tuple[str, ...]:
    return tuple(sorted(CHECKOUT_ACTIVE_STATES))


def _release_proxy_lock_if_owned(conn: sqlite3.Connection, account_id: int) -> None:
    conn.execute(
        """
        update retailer_accounts
        set proxy_lock_state = 'unlocked',
            proxy_lock_owner = null,
            proxy_lock_acquired_at = null,
            updated_at = ?
        where id = ?
        """,
        (utc_now(), account_id),
    )


def _try_acquire_proxy_lock(conn: sqlite3.Connection, account: sqlite3.Row, *, now_iso: str) -> bool:
    proxy_url = (account["proxy_url"] or "").strip()
    if not proxy_url:
        conn.execute(
            """
            update retailer_accounts
            set proxy_lock_state = 'unlocked',
                proxy_lock_owner = null,
                proxy_lock_acquired_at = null,
                updated_at = ?
            where id = ?
            """,
            (now_iso, account["id"]),
        )
        return True
    lock_owner = f"account:{account['id']}"
    conflict = conn.execute(
        """
        select id
        from retailer_accounts
        where workspace_id = ?
          and proxy_url = ?
          and id != ?
          and proxy_lock_state = 'locked'
        limit 1
        """,
        (account["workspace_id"], proxy_url, account["id"]),
    ).fetchone()
    if conflict:
        return False
    conn.execute(
        """
        update retailer_accounts
        set proxy_lock_state = 'locked',
            proxy_lock_owner = ?,
            proxy_lock_acquired_at = coalesce(proxy_lock_acquired_at, ?),
            last_used_at = ?,
            updated_at = ?
        where id = ?
        """,
        (lock_owner, now_iso, now_iso, now_iso, account["id"]),
    )
    return True


def _deterministic_account_delay_seconds(account_id: int) -> int:
    floor = min(ACCOUNT_START_DELAY_MIN_SECONDS, ACCOUNT_START_DELAY_MAX_SECONDS)
    ceiling = max(ACCOUNT_START_DELAY_MIN_SECONDS, ACCOUNT_START_DELAY_MAX_SECONDS)
    if floor == ceiling:
        return floor
    span = ceiling - floor + 1
    bucket = int(datetime.now(timezone.utc).timestamp() // 60)
    digest = hashlib.sha256(f"{account_id}:{bucket}".encode("utf-8")).hexdigest()
    return floor + (int(digest[:8], 16) % span)


def run_checkout_account_scheduler(conn: sqlite3.Connection, *, now_iso: str) -> None:
    rows = conn.execute(
        """
        select
            a.*,
            t.id as task_id,
            t.current_state as task_state,
            t.created_at as task_created_at
        from retailer_accounts a
        left join task_profile_bindings b
          on b.workspace_id = a.workspace_id
         and b.retailer_account_id = a.id
        left join checkout_tasks t
          on t.workspace_id = a.workspace_id
         and t.monitor_id = b.monitor_id
         and t.is_paused = 0
         and t.current_state in ('starting', 'waiting_for_queue', 'solving_hcaptcha', 'in_queue', 'passed_queue', 'waiting_for_monitor_input', 'monitoring_product', 'adding_to_cart', 'checking_out', 'requeued')
        order by a.id asc, t.id asc
        """
    ).fetchall()
    if not rows:
        return
    by_account: dict[int, dict[str, Any]] = {}
    for row in rows:
        payload = by_account.setdefault(
            row["id"],
            {"account": row, "tasks": []},
        )
        if row["task_id"] is not None:
            payload["tasks"].append(row)

    for account_id, payload in by_account.items():
        account = payload["account"]
        tasks = payload["tasks"]
        if not tasks:
            _release_proxy_lock_if_owned(conn, account_id)
            continue

        lock_ok = _try_acquire_proxy_lock(conn, account, now_iso=now_iso)
        if not lock_ok:
            continue
        active_now = next((t for t in tasks if t["task_state"] != "queued"), None)
        if active_now:
            continue

        next_start_after = account["next_start_after"]
        if next_start_after:
            try:
                if datetime.fromisoformat(next_start_after) > datetime.fromisoformat(now_iso):
                    continue
            except ValueError:
                pass
        next_task = tasks[0]
        transition_checkout_task(
            conn,
            task_id=next_task["task_id"],
            workspace_id=account["workspace_id"],
            requested_state="monitoring",
            reason="account_scheduler_start",
        )
        delay_seconds = _deterministic_account_delay_seconds(account_id)
        next_start_ts = datetime.now(timezone.utc).timestamp() + delay_seconds
        conn.execute(
            """
            update retailer_accounts
            set next_start_after = ?, last_used_at = ?, updated_at = ?
            where id = ?
            """,
            (
                datetime.fromtimestamp(next_start_ts, tz=timezone.utc).isoformat(),
                now_iso,
                now_iso,
                account_id,
            ),
        )


def worker_loop() -> None:
    log(f"Worker loop started ({WORKER_ID})")
    while worker_running:
        with worker_lock:
            conn = db()
            now_iso = utc_now()
            queue = SQLiteJobQueue(conn, worker_id=WORKER_ID)
            run_checkout_account_scheduler(conn, now_iso=now_iso)
            apply_due_schedules(conn)
            monitors = conn.execute("select * from monitors where enabled = 1").fetchall()
            for monitor in monitors:
                queue.enqueue_monitor_check_if_due(monitor, now_iso=now_iso)
            job = queue.claim_due_job(now_iso=now_iso)
            if job:
                if job.job_type == "monitor_check":
                    execute_monitor_job(queue, job, now_iso=now_iso)
                elif job.job_type == "discord_ingest_event":
                    process_discord_alert_job(queue, job, now_iso=now_iso)
                else:
                    queue.fail_job(
                        job.id,
                        now_iso=now_iso,
                        status="failed",
                        next_run_at=now_iso,
                        payload_json=job.payload_json or "{}",
                        error_text=f"unsupported_job_type:{job.job_type}",
                    )
            conn.commit()
            conn.close()
        if not job:
            time.sleep(WORKER_IDLE_SLEEP_SECONDS)
            idle_jitter = random.uniform(0.0, max(WORKER_IDLE_SLEEP_JITTER_SECONDS, 0.0))
            time.sleep(WORKER_IDLE_SLEEP_SECONDS + idle_jitter)
        else:
            active_jitter = random.uniform(0.0, max(WORKER_ACTIVE_JITTER_SECONDS, 0.0))
            if active_jitter > 0:
                time.sleep(active_jitter)
    log(f"Worker loop stopped ({WORKER_ID})")


@app.route("/")
def index():
    return render_template(
        "index.html",
        captcha_provider=CAPTCHA_PROVIDER,
        captcha_site_key=CAPTCHA_SITE_KEY,
        captcha_script_url=CAPTCHA_SCRIPT_URL,
        api_auth_token=API_AUTH_TOKEN,
    )


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "worker_running": worker_running, "app_role": APP_ROLE})


@app.get("/api/meta")
def api_meta():
    return jsonify(
        {
            "app_version": APP_VERSION,
            "release_channel": RELEASE_CHANNEL,
            "python_version": os.sys.version.split()[0],
            "app_role": APP_ROLE,
            "embedded_worker_enabled": ENABLE_EMBEDDED_WORKER,
        }
    )


def normalize_version(value: str) -> tuple[int, ...]:
    parts = [int(part) for part in re.findall(r"\d+", value or "")]
    while parts and parts[-1] == 0:
        parts.pop()
    return tuple(parts or [0])


def is_version_newer(current_version: str, latest_version: str) -> bool:
    current_parts = list(normalize_version(current_version))
    latest_parts = list(normalize_version(latest_version))
    max_len = max(len(current_parts), len(latest_parts))
    current_parts.extend([0] * (max_len - len(current_parts)))
    latest_parts.extend([0] * (max_len - len(latest_parts)))
    return tuple(latest_parts) > tuple(current_parts)


def parse_latest_version_from_response(resp: requests.Response) -> str:
    content_type = (resp.headers.get("Content-Type") or "").lower()
    if "application/json" in content_type:
        data = resp.json()
        if isinstance(data, str):
            latest_version = data
        elif isinstance(data, dict):
            latest_version = data.get("latest_version") or data.get("version") or data.get("tag_name")
        else:
            latest_version = None
    else:
        latest_version = resp.text

    if not isinstance(latest_version, str) or not latest_version.strip():
        raise ValueError("Missing latest version in upstream response")
    return latest_version.strip()


def resolve_latest_version() -> tuple[str, str | None]:
    fallback_version = APP_VERSION
    if not UPDATE_CHECK_URL:
        return fallback_version, "update_check_url_not_configured"

    try:
        req = perform_request(
            task_key="meta-update-check",
            method="GET",
            url=UPDATE_CHECK_URL,
            workspace_id=None,
            proxy_url=None,
            timeout=UPDATE_CHECK_TIMEOUT_SECONDS,
            retry_total=1,
            backoff_factor=0.1,
        )
        if req.error:
            raise req.error
        assert req.response is not None
        resp = req.response
        resp.raise_for_status()
        latest = parse_latest_version_from_response(resp)
        return latest, None
    except Exception as exc:
        log(f"Update check failed: {exc}", level="warning")
        return fallback_version, str(exc)


@app.get("/api/meta/check-update")
def api_meta_check_update():
    latest, source_error = resolve_latest_version()
    payload = {
        "ok": True,
        "current_version": APP_VERSION,
        "latest_version": latest,
        "update_available": is_version_newer(APP_VERSION, latest),
        "release_channel": RELEASE_CHANNEL,
    }
    if source_error:
        payload["source_error"] = source_error
    return jsonify(payload)


@app.post("/api/billing/stripe/webhook")
def api_billing_stripe_webhook():
    payload = request.get_data(cache=False, as_text=False)
    signature_header = request.headers.get("Stripe-Signature")
    try:
        verify_stripe_webhook_signature(payload, signature_header)
    except PermissionError as exc:
        return jsonify({"error": str(exc)}), 401

    try:
        event = json.loads(payload.decode("utf-8"))
    except (UnicodeDecodeError, json.JSONDecodeError):
        return jsonify({"error": "Invalid JSON payload"}), 400

    event_id = event.get("id")
    event_type = event.get("type", "")
    if not event_id:
        return jsonify({"error": "Missing Stripe event id"}), 400

    subscription = ((event.get("data") or {}).get("object") or {})
    workspace_id = None
    if isinstance(subscription, dict):
        workspace_id = _workspace_id_from_subscription_object(subscription)

    supported_types = {
        "customer.subscription.created",
        "customer.subscription.updated",
        "customer.subscription.deleted",
    }
    conn = db()
    try:
        conn.execute("begin")
        insert_result = conn.execute(
            """
            insert or ignore into billing_webhook_events(event_id, processed_at, event_type, workspace_id)
            values (?, ?, ?, ?)
            """,
            (event_id, utc_now(), event_type, workspace_id),
        )
        if insert_result.rowcount == 0:
            conn.rollback()
            conn.close()
            return jsonify({"ok": True, "noop": True}), 200
        if event_type in supported_types:
            sync_billing_subscription_event(conn, event)
        conn.commit()
    except sqlite3.DatabaseError as exc:
        conn.rollback()
        conn.close()
        return jsonify({"error": f"Database error: {exc}"}), 400
    conn.close()
    return jsonify({"ok": True, "noop": False}), 200


@app.get("/api/workspace")
@require_auth
def api_workspace():
    row = get_workspace(current_workspace_id())
    return jsonify({"workspace": dict(row), "user": current_user_context()})


@app.patch("/api/workspace")
@require_auth
def api_update_workspace():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    body = request.json or {}
    updates: dict[str, Any] = {}
    try:
        if "proxy_url" in body:
            updates["proxy_url"] = (body.get("proxy_url") or "").strip() or None
        if "session_metadata" in body:
            session_metadata = _validate_json_object(body.get("session_metadata"), field_name="session_metadata")
            updates["session_metadata"] = json.dumps(session_metadata) if session_metadata is not None else None
        if "behavior_metadata" in body:
            behavior_metadata = _validate_behavior_policy(body.get("behavior_metadata"))
            updates["behavior_metadata"] = json.dumps(behavior_metadata) if behavior_metadata is not None else None
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not updates:
        return jsonify({"error": "No updatable fields provided"}), 400
    workspace_id = current_workspace_id()
    conn = db()
    set_clause = ", ".join(f"{column} = ?" for column in updates)
    conn.execute(f"update workspaces set {set_clause} where id = ?", (*updates.values(), workspace_id))
    conn.commit()
    row = conn.execute("select * from workspaces where id = ?", (workspace_id,)).fetchone()
    conn.close()
    return jsonify({"workspace": dict(row)})


@app.post("/api/workspace/plan")
@require_auth
def api_update_plan():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    plan = (request.json or {}).get("plan", "basic")
    if plan not in PLAN_LIMITS:
        return jsonify({"error": "Invalid plan"}), 400
    workspace_id = get_workspace_id_for_request()
    conn = db()
    conn.execute("update workspaces set plan = ? where id = ?", (plan, workspace_id))
    conn.execute("update workspaces set plan = ? where id = ?", (plan, current_workspace_id()))
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "plan": plan})


@app.post("/api/billing/subscription-events")
@require_auth
def api_sync_billing_subscription_event():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    body = request.json or {}
    subscription = body.get("subscription") if isinstance(body.get("subscription"), dict) else {}
    payload = {
        "provider": body.get("provider", "stripe"),
        "provider_subscription_id": body.get("provider_subscription_id") or subscription.get("id"),
        "provider_customer_id": body.get("provider_customer_id") or subscription.get("customer_id"),
        "status": body.get("status") or subscription.get("status"),
        "plan_code": body.get("plan_code") or subscription.get("plan_code"),
        "plan_lookup_key": body.get("plan_lookup_key") or subscription.get("plan_lookup_key"),
        "cancel_at_period_end": body.get("cancel_at_period_end", subscription.get("cancel_at_period_end")),
        "current_period_end": body.get("current_period_end") or subscription.get("current_period_end"),
        "source": body.get("source") or "billing_subscriptions",
    }
    try:
        result = sync_billing_subscription_payload(payload)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    return jsonify({"ok": True, **result})


@app.get("/api/monitors")
@require_auth
def api_list_monitors():
    workspace_id = get_workspace_id_for_request()
    conn = db()
    rows = conn.execute(
        "select * from monitors where workspace_id = ? order by id desc",
        (workspace_id,),
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.post("/api/monitors/monitor-assist/apply")
@require_auth
def api_apply_monitor_assist_pid():
    body = request.json or {}
    raw_monitor_ids = body.get("monitor_ids")
    if not isinstance(raw_monitor_ids, list) or not raw_monitor_ids:
        return jsonify({"error": "monitor_ids must be a non-empty list"}), 400
    try:
        monitor_ids = sorted({int(value) for value in raw_monitor_ids})
    except (TypeError, ValueError):
        return jsonify({"error": "monitor_ids must contain valid integers"}), 400

    try:
        pid = normalize_monitor_assist_pid(str(body.get("pid") or ""))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    product_url = f"https://www.pokemoncenter.com/product/{pid}"
    workspace_id = get_workspace_id_for_request()
    conn = db()
    placeholders = ",".join(["?"] * len(monitor_ids))
    monitor_rows = conn.execute(
        f"""
        select id, retailer, product_url
        from monitors
        where workspace_id = ?
          and id in ({placeholders})
        """,
        (workspace_id, *monitor_ids),
    ).fetchall()
    if not monitor_rows:
        conn.close()
        return jsonify({"error": "No matching monitors found"}), 404

    eligible_monitor_ids = [row["id"] for row in monitor_rows if (row["retailer"] or "").strip().lower() == "pokemoncenter"]
    if not eligible_monitor_ids:
        conn.close()
        return jsonify({"error": "Monitor assist currently supports pokemoncenter monitors only"}), 400

    eligible_placeholders = ",".join(["?"] * len(eligible_monitor_ids))
    conn.execute(
        f"""
        update monitors
        set product_url = ?
        where workspace_id = ?
          and id in ({eligible_placeholders})
        """,
        (product_url, workspace_id, *eligible_monitor_ids),
    )

    task_rows = conn.execute(
        f"""
        select id, monitor_id, current_state
        from checkout_tasks
        where workspace_id = ?
          and monitor_id in ({eligible_placeholders})
        """,
        (workspace_id, *eligible_monitor_ids),
    ).fetchall()
    for task in task_rows:
        details = {
            "event": "monitor_assist_pid_apply",
            "pid": pid,
            "product_url": product_url,
            "source": "monitor_assist",
        }
        record_checkout_attempt(
            conn,
            task_id=int(task["id"]),
            workspace_id=workspace_id,
            monitor_id=int(task["monitor_id"]),
            state=str(task["current_state"] or "queued"),
            status="PID updated from monitor assist",
            details=details,
            error_text="PID updated from monitor assist",
        )
        record_task_log(
            conn,
            task_id=int(task["id"]),
            workspace_id=workspace_id,
            monitor_id=int(task["monitor_id"]),
            level="info",
            event_type="monitor_assist",
            message="PID updated from monitor assist",
            payload=details,
        )

    conn.commit()
    conn.close()
    return jsonify(
        {
            "ok": True,
            "pid": pid,
            "product_url": product_url,
            "updated_monitors": len(eligible_monitor_ids),
            "updated_tasks": len(task_rows),
            "monitor_ids": eligible_monitor_ids,
        }
    )


def get_monitor_for_workspace(
    conn: sqlite3.Connection, monitor_id: int, workspace_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (monitor_id, workspace_id),
    ).fetchone()


@app.get("/api/monitors/<int:monitor_id>")
@require_auth
def api_get_monitor(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    conn.close()
    if not row:
        return jsonify({"error": "Monitor not found"}), 404
    return jsonify(dict(row))


@app.post("/api/tasks")
@require_auth
def api_create_task():
    body = request.json or {}
    try:
        retailer = canonical_retailer((body.get("retailer") or "").strip())
        category = (body.get("category") or "pokemon").strip().lower()
        product_url = (body.get("url") or body.get("product_url") or "").strip()
        normalized_products: list[dict[str, Any]] | None = None
        profile = (body.get("profile") or "").strip()
        account = (body.get("account") or "").strip()
        payment = (body.get("payment") or "").strip()
        if retailer not in SUPPORTED_RETAILERS:
            raise ValueError(f"Unsupported retailer '{retailer}'")
        if category not in SUPPORTED_MONITOR_CATEGORIES:
            raise ValueError(f"Unsupported category '{category}'")
        if category not in RETAILER_CATEGORY_SUPPORT.get(retailer, set()):
            raise ValueError(f"Retailer '{retailer}' does not support category '{category}'")
        if retailer == "pokemoncenter":
            product_url, normalized_products = _resolve_monitor_input(product_url)
            if product_url == "placeholder":
                raise ValueError("placeholder is not allowed when creating monitors")
        elif not (product_url.startswith("http://") or product_url.startswith("https://")):
            raise ValueError("url must be http(s)")
        if not profile:
            raise ValueError("profile is required")
        if not account:
            raise ValueError("account is required")
        if not payment:
            raise ValueError("payment is required")
    except (ValueError, MonitorInputValidationError) as exc:
        return jsonify({"error": str(exc)}), 400

    workspace_id = current_workspace_id()
    conn = db()
    enforce_plan_limits(workspace_id, 20)
    monitor_cur = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, enabled, created_at)
        values (?, ?, ?, 20, 0, ?)
        """,
        (workspace_id, retailer, product_url, utc_now()),
        """,
        (workspace_id, retailer, product_url, utc_now()),
        insert into monitors(workspace_id, retailer, category, product_url, poll_interval_seconds, enabled, created_at)
        values (?, ?, ?, ?, 20, 0, ?)
        """,
        (workspace_id, retailer, category, product_url, utc_now()),
    )
    monitor_id = int(monitor_cur.lastrowid)
    task = create_checkout_task(
        conn,
        workspace_id=workspace_id,
        monitor_id=monitor_id,
        task_name=f"{retailer} task",
        task_config={
            "retailer": retailer,
            "category": category,
            "product_url": product_url,
            "products": normalized_products or [],
            "profile": profile,
            "account": account,
            "payment": payment,
        },
        initial_state="queued",
    )
    conn.commit()
    conn.close()

    payload = {
        "id": task["id"],
        "workspace_id": workspace_id,
        "retailer": retailer,
        "category": category,
        "product_url": product_url,
        "profile": profile,
        "account": account,
        "payment": payment,
        "state": "idle",
        "retries": 0,
        "last_step": None,
        "last_error": None,
    }
    socketio.emit("task_update", payload)
    return jsonify(payload), 201


@app.get("/api/tasks")
@require_auth
def api_list_tasks():
    conn = db()
    rows = conn.execute(
        "select * from checkout_tasks where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    payloads = []
    for row in rows:
        config = {}
        raw = row["task_config"]
        if raw:
            try:
                config = json.loads(raw)
            except (TypeError, json.JSONDecodeError):
                config = {}
        state = normalize_checkout_state(row["current_state"])
        compat_state = "running" if state in CHECKOUT_ACTIVE_STATES else ("idle" if state == "idle" else state)
        payloads.append(
            {
                "id": row["id"],
                "workspace_id": row["workspace_id"],
                "retailer": config.get("retailer"),
                "product_url": config.get("product_url"),
                "profile": config.get("profile"),
                "account": config.get("account"),
                "payment": config.get("payment"),
                "state": compat_state,
                "retries": 0,
                "last_step": row["current_state"],
                "last_error": row["last_error"],
            }
        )
    conn.close()
    return jsonify(payloads)


@app.post("/api/tasks/<int:task_id>/start")
@require_auth
def api_start_task(task_id: int):
    workspace_id = current_workspace_id()
    conn = db()
    task = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    if not task:
        conn.close()
        return jsonify({"error": "Task not found"}), 404
    transitioned = transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        requested_state="monitoring",
        reason="compat_api_start_task",
    )
    conn.commit()
    conn.close()
    assert transitioned is not None
    config = json.loads(transitioned["task_config"] or "{}")
    payload = {
        "id": transitioned["id"],
        "workspace_id": workspace_id,
        "retailer": config.get("retailer"),
        "category": config.get("category"),
        "product_url": config.get("product_url"),
        "profile": config.get("profile"),
        "account": config.get("account"),
        "payment": config.get("payment"),
        "state": "running",
        "retries": 0,
        "last_step": transitioned["current_state"],
        "last_error": transitioned["last_error"],
    }
    socketio.emit("task_update", payload)
    return jsonify({"ok": True, "task": payload, "already_running": False})


@app.post("/api/tasks/<int:task_id>/stop")
@require_auth
def api_stop_task(task_id: int):
    workspace_id = current_workspace_id()
    conn = db()
    task = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    if not task:
        conn.close()
        return jsonify({"error": "Task not found"}), 404
    transitioned = transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=workspace_id,
        requested_state="stopped",
        reason="compat_api_stop_task",
    )
    conn.commit()
    conn.close()
    assert transitioned is not None
    config = json.loads(transitioned["task_config"] or "{}")
    payload = {
        "id": transitioned["id"],
        "workspace_id": workspace_id,
        "retailer": config.get("retailer"),
        "category": config.get("category"),
        "product_url": config.get("product_url"),
        "profile": config.get("profile"),
        "account": config.get("account"),
        "payment": config.get("payment"),
        "state": "stopped",
        "retries": 0,
        "last_step": transitioned["current_state"],
        "last_error": transitioned["last_error"],
    }
    socketio.emit("task_update", payload)
    return jsonify({"ok": True, "task": payload})


@app.get("/api/monitors/<int:monitor_id>")
@require_auth
def api_get_monitor_dup1(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    conn.close()
    if not row:
        return jsonify({"error": "Monitor not found"}), 404
    return jsonify(dict(row))


@app.get("/api/dashboard/summary")
@require_auth
def api_dashboard_summary():
    workspace_id = get_workspace_id_for_request()
    conn = db()
    total_monitors = conn.execute(
        "select count(*) as c from monitors where workspace_id = ?", (workspace_id,)
    ).fetchone()["c"]
    enabled_monitors = conn.execute(
        "select count(*) as c from monitors where workspace_id = ? and enabled = 1", (workspace_id,)
    ).fetchone()["c"]
    checks_last_24h = conn.execute(
        """
        select count(*) as c from monitors
        where workspace_id = ?
          and last_checked_at is not null
          and datetime(last_checked_at) >= datetime('now', '-1 day')
        """,
        (workspace_id,),
    ).fetchone()["c"]
    latest_check = conn.execute(
        "select max(last_checked_at) as latest_check from monitors where workspace_id = ?",
        (workspace_id,),
    ).fetchone()["latest_check"]
    events_24h = conn.execute(
        """
        select count(*) as c from events e
        join monitors m on m.id = e.monitor_id
        where m.workspace_id = ?
          and datetime(e.event_time) >= datetime('now', '-1 day')
        """,
        (workspace_id,),
    ).fetchone()["c"]
    events_7d = conn.execute(
        """
        select count(*) as c from events e
        join monitors m on m.id = e.monitor_id
        where m.workspace_id = ?
          and datetime(e.event_time) >= datetime('now', '-7 day')
        """,
        (workspace_id,),
    ).fetchone()["c"]
    deliveries_total = conn.execute(
        """
        select count(*) as c from deliveries d
        join events e on e.id = d.event_id
        join monitors m on m.id = e.monitor_id
        where m.workspace_id = ?
        """,
        (workspace_id,),
    ).fetchone()["c"]
    deliveries_sent = conn.execute(
        """
        select count(*) as c from deliveries d
        join events e on e.id = d.event_id
        join monitors m on m.id = e.monitor_id
        where m.workspace_id = ? and d.status = 'sent'
        """,
        (workspace_id,),
    ).fetchone()["c"]
    conn.close()

    success_rate = 0.0 if deliveries_total == 0 else (deliveries_sent / deliveries_total)
    return jsonify(
        {
            "total_monitors": total_monitors,
            "enabled_monitors": enabled_monitors,
            "checks_last_24h": checks_last_24h,
            "latest_check_at": latest_check,
            "events_last_24h": events_24h,
            "events_last_7d": events_7d,
            "delivery_success_rate": round(success_rate, 4),
            "worker_running": worker_running,
            "app_role": APP_ROLE,
        }
    )


@app.get("/api/ops/metrics")
@require_auth
def api_ops_metrics():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    workspace_id = get_workspace_id_for_request()
    conn = db()
    metrics = {
        "checks_total": conn.execute(
            "select count(*) as c from monitors where workspace_id = ? and last_checked_at is not null",
            (workspace_id,),
        ).fetchone()["c"],
        "checks_failed_total": conn.execute(
            "select count(*) as c from monitors where workspace_id = ? and failure_streak > 0",
            (workspace_id,),
        ).fetchone()["c"],
        "alerts_created_total": conn.execute(
            """
            select count(*) as c from events e
            join monitors m on m.id = e.monitor_id
            where m.workspace_id = ?
            """,
            (workspace_id,),
        ).fetchone()["c"],
        "webhook_sent_total": conn.execute(
            """
            select count(*) as c from deliveries d
            join webhooks w on w.id = d.webhook_id
            where w.workspace_id = ? and d.status = 'sent'
            """,
            (workspace_id,),
        ).fetchone()["c"],
        "webhook_failed_total": conn.execute(
            """
            select count(*) as c from deliveries d
            join webhooks w on w.id = d.webhook_id
            where w.workspace_id = ? and d.status = 'failed'
            """,
            (workspace_id,),
        ).fetchone()["c"],
    }
    conn.close()
    return jsonify(metrics)


@app.get("/api/ops/monitor-failure-trends")
@require_auth
def api_monitor_failure_trends():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    workspace_id = get_workspace_id_for_request()
    conn = db()
    rows = conn.execute(
        """
        select m.id as monitor_id,
               coalesce(sum(case when datetime(mf.failed_at) >= datetime('now', '-1 day') then 1 else 0 end), 0) as failures_last_24h,
               coalesce(sum(case when datetime(mf.failed_at) >= datetime('now', '-7 day') then 1 else 0 end), 0) as failures_last_7d
        from monitors m
        left join monitor_failures mf on mf.monitor_id = m.id
        where m.workspace_id = ?
        group by m.id
        order by m.id asc
        """,
        (workspace_id,),
    ).fetchall()
    conn.close()
    return jsonify({"trends": [dict(r) for r in rows]})


@app.get("/api/ops/webhook-health-trends")
@require_auth
def api_webhook_health_trends():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    workspace_id = get_workspace_id_for_request()
    conn = db()
    rows = conn.execute(
        """
        select w.id as webhook_id,
               w.fail_streak,
               w.last_status_code,
               w.last_delivery_status,
               w.last_delivery_at,
               coalesce(sum(case when d.status = 'failed' and datetime(d.delivered_at) >= datetime('now', '-1 day') then 1 else 0 end), 0) as recent_failures_24h,
               coalesce(sum(case when d.status = 'failed' and datetime(d.delivered_at) >= datetime('now', '-7 day') then 1 else 0 end), 0) as recent_failures_7d
        from webhooks w
        left join deliveries d on d.webhook_id = w.id
        where w.workspace_id = ?
        group by w.id
        order by w.id asc
        """,
        (workspace_id,),
    ).fetchall()
    conn.close()
    return jsonify({"webhooks": [dict(r) for r in rows]})


@app.post("/api/monitors")
@require_auth
def api_create_monitor():
    body = request.json or {}
    try:
        retailer = canonical_retailer(body["retailer"])
        category = (body.get("category") or "pokemon").strip().lower()
        url = body["product_url"].strip()
        normalized_products: list[dict[str, Any]] | None = None
        poll_interval = int(body.get("poll_interval_seconds", 20))
        keyword = (body.get("keyword") or "").strip() or None
        max_price_cents = body.get("max_price_cents")
        if max_price_cents is not None:
            max_price_cents = int(max_price_cents)
        msrp_cents = body.get("msrp_cents")
        if msrp_cents is not None:
            msrp_cents = int(msrp_cents)
        proxy_url = (body.get("proxy_url") or "").strip() or None
        session_task_key = (body.get("session_task_key") or "").strip() or None
        session_metadata = _validate_json_object(body.get("session_metadata"), field_name="session_metadata")
        behavior_metadata = _validate_behavior_policy(body.get("behavior_metadata"))
        if retailer not in SUPPORTED_RETAILERS:
            raise ValueError(f"Unsupported retailer '{retailer}'")
        if category not in SUPPORTED_MONITOR_CATEGORIES:
            raise ValueError(f"Unsupported category '{category}'")
        if category not in RETAILER_CATEGORY_SUPPORT.get(retailer, set()):
            raise ValueError(f"Retailer '{retailer}' does not support category '{category}'")
        if retailer == "pokemoncenter":
            url, normalized_products = _resolve_monitor_input(url)
            if url == "placeholder":
                raise ValueError("placeholder is not allowed when creating monitors")
        elif not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("product_url must be http(s)")
        if session_task_key and len(session_task_key) > 80:
            raise ValueError("session_task_key must be <= 80 chars")

        enforce_plan_limits(current_workspace_id(), poll_interval)
    except (KeyError, ValueError, MonitorInputValidationError) as exc:
        return jsonify({"error": str(exc)}), 400

    conn = db()
    cur = conn.execute(
        """
        insert into monitors(workspace_id, retailer, category, product_url, keyword, max_price_cents, msrp_cents, poll_interval_seconds, created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            current_workspace_id(),
            retailer,
            category,
            url,
            keyword,
            max_price_cents,
            msrp_cents,
            poll_interval,
            utc_now(),
        ),
    )
    conn.commit()
    monitor_id = cur.lastrowid
    row = conn.execute("select * from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    payload = dict(row)
    if normalized_products is not None:
        payload["products"] = normalized_products
    return jsonify(payload), 201


@app.patch("/api/monitors/<int:monitor_id>")
@require_auth
def api_update_monitor(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    body = request.json or {}
    enabled = body.get("enabled")
    if enabled is None:
        return jsonify({"error": "enabled is required"}), 400

    conn = db()
    conn.execute("update monitors set enabled = ? where id = ? and workspace_id = ?", (int(bool(enabled)), monitor_id, workspace_id))
    conn.commit()
    row = conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (monitor_id, workspace_id),
    ).fetchone()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    if not row:
        conn.close()
        return jsonify({"error": "Monitor not found"}), 404
    conn.close()
    return jsonify(dict(row))


@app.delete("/api/monitors/<int:monitor_id>")
@require_auth
def api_workspace_dup1():
    row = get_workspace(current_workspace_id())
    return jsonify({"workspace": dict(row), "user": current_user_context()})
def api_delete_monitor(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    if not row:
        conn.close()
        return jsonify({"error": "Monitor not found"}), 404
    conn.execute("delete from monitors where id = ? and workspace_id = ?", (monitor_id, workspace_id))
    conn.execute(
        "delete from monitors where id = ? and workspace_id = ?",
        (monitor_id, workspace_id),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.post("/api/monitors/<int:monitor_id>/check")
@require_auth
def api_check_monitor(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    row = conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (monitor_id, workspace_id),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Monitor not found"}), 404
    return jsonify(check_monitor_once(row))


@app.post("/api/checkout/tasks")
@require_auth
def api_create_checkout_task():
    body = request.json or {}
    monitor_id = body.get("monitor_id")
    if monitor_id is None:
        return jsonify({"error": "monitor_id is required"}), 400

    workspace_id = current_workspace_id()
    conn = db()
    monitor = conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (int(monitor_id), workspace_id),
    ).fetchone()
    if not monitor:
        conn.close()
        return jsonify({"error": "Monitor not found"}), 404
    initial_state = body.get("initial_state", "queued")
    raw_task_config = body.get("task_config") if isinstance(body.get("task_config"), dict) else None
    normalized_task_config = normalize_task_config_for_monitor(raw_task_config, monitor_row=monitor)
    if monitor["retailer"] != "pokemoncenter" and _pokemon_center_mode(normalized_task_config) != "default":
        conn.close()
        return jsonify({"error": "Selected mode is only supported for pokemoncenter monitors"}), 400
    if monitor["retailer"] == "pokemoncenter":
        mode = _pokemon_center_mode(normalized_task_config)
        site = str(normalized_task_config.get("site") or "").strip().lower()
        site_validation_error = validate_pokemon_center_mode_site(mode, site)
        if site_validation_error:
            conn.close()
            return jsonify({"error": site_validation_error}), 400
        field_validation_error = validate_pokemon_center_mode_requirements(normalized_task_config)
        if field_validation_error:
            conn.close()
            return jsonify({"error": field_validation_error}), 400

    binding_ok, binding_error = _require_checkout_binding(
        conn,
        workspace_id=workspace_id,
        monitor_id=int(monitor_id),
        mode=_pokemon_center_mode(normalized_task_config),
    )
    if not binding_ok:
        conn.close()
        return jsonify({"error": binding_error}), 400
    try:
        task = create_checkout_task(
            conn,
            workspace_id=workspace_id,
            monitor_id=int(monitor_id),
            task_name=(body.get("task_name") or "").strip() or None,
            task_config=normalized_task_config,
            initial_state=initial_state,
        )
    except ValueError as exc:
        conn.close()
        return jsonify({"error": str(exc)}), 400

    conn.commit()
    conn.close()
    payload = serialize_checkout_task_summary(task)
    socketio.emit("task_update", payload)
    return jsonify(payload), 201


@app.get("/api/checkout/tasks")
@require_auth
def api_list_checkout_tasks():
    conn = db()
    rows = conn.execute(
        """
        select *
        from checkout_tasks
        where workspace_id = ?
        order by id desc
        """,
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_checkout_task_summary(row) for row in rows])


@app.post("/api/checkout/task-groups/products/preview")
@require_auth
def api_preview_checkout_task_group_products():
    body = request.json if isinstance(request.json, dict) else {}
    monitor_ids = body.get("monitor_ids")
    if not isinstance(monitor_ids, list) or not monitor_ids:
        return jsonify({"error": "monitor_ids must be a non-empty list"}), 400
    normalized_ids = [int(mid) for mid in monitor_ids if isinstance(mid, int) or str(mid).isdigit()]
    if not normalized_ids:
        return jsonify({"error": "monitor_ids must include numeric ids"}), 400

    workspace_id = current_workspace_id()
    conn = db()
    placeholders = ",".join(["?"] * len(normalized_ids))
    state_placeholders = ",".join(["?"] * len(RUNNING_TASK_STATES))
    rows = conn.execute(
        f"""
        select ct.id, ct.task_config
        from checkout_tasks ct
        where ct.workspace_id = ?
          and ct.monitor_id in ({placeholders})
          and ct.current_state in ({state_placeholders})
        order by ct.id asc
        """,
        (workspace_id, *normalized_ids, *RUNNING_TASK_STATES),
    ).fetchall()
    conn.close()
    if not rows:
        return jsonify({"tasks_found": 0, "products": []})

    first = serialize_checkout_task(rows[0]) or {}
    config = first.get("task_config") if isinstance(first.get("task_config"), dict) else {}
    products = _coerce_product_rows(config.get("products"))
    return jsonify({"tasks_found": len(rows), "products": products})


@app.post("/api/checkout/task-groups/products")
@require_auth
def api_update_checkout_task_group_products():
    body = request.json if isinstance(request.json, dict) else {}
    monitor_ids = body.get("monitor_ids")
    operation = body.get("operation") if isinstance(body.get("operation"), dict) else {}
    if not isinstance(monitor_ids, list) or not monitor_ids:
        return jsonify({"error": "monitor_ids must be a non-empty list"}), 400

    normalized_ids = [int(mid) for mid in monitor_ids if isinstance(mid, int) or str(mid).isdigit()]
    if not normalized_ids:
        return jsonify({"error": "monitor_ids must include numeric ids"}), 400

    workspace_id = current_workspace_id()
    conn = db()
    placeholders = ",".join(["?"] * len(normalized_ids))
    state_placeholders = ",".join(["?"] * len(RUNNING_TASK_STATES))
    rows = conn.execute(
        f"""
        select ct.*, m.retailer, m.product_url
        from checkout_tasks ct
        join monitors m on m.id = ct.monitor_id
        where ct.workspace_id = ?
          and ct.monitor_id in ({placeholders})
          and ct.current_state in ({state_placeholders})
        order by ct.id asc
        """,
        (workspace_id, *normalized_ids, *RUNNING_TASK_STATES),
    ).fetchall()
    if not rows:
        conn.close()
        return jsonify({"updated": 0, "rejected": 0, "tasks_found": 0})

    updated = 0
    rejected = 0
    errors: list[dict[str, Any]] = []
    for row in rows:
        task_payload = serialize_checkout_task(row) or {}
        task_config = task_payload.get("task_config") if isinstance(task_payload.get("task_config"), dict) else {}
        try:
            next_config = apply_product_group_operation(task_config, operation)
        except ValueError as exc:
            rejected += 1
            errors.append({"task_id": row["id"], "error": str(exc)})
            continue
        conn.execute(
            "update checkout_tasks set task_config = ?, updated_at = ? where id = ? and workspace_id = ?",
            (json.dumps(next_config), utc_now(), row["id"], workspace_id),
        )
        refreshed = conn.execute(
            "select * from checkout_tasks where id = ? and workspace_id = ?",
            (row["id"], workspace_id),
        ).fetchone()
        if refreshed:
            socketio.emit("task_update", serialize_task_ui(refreshed))
        updated += 1

    conn.commit()
    conn.close()
    return jsonify(
        {
            "updated": updated,
            "rejected": rejected,
            "tasks_found": len(rows),
            "errors": errors[:25],
        }
    )


def get_monitor_for_workspace(
    conn: sqlite3.Connection, monitor_id: int, workspace_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (monitor_id, workspace_id),
    ).fetchone()


@app.get("/api/monitors/<int:monitor_id>")
@require_auth
def api_get_monitor_dup2(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    conn.close()
    if not row:
        return jsonify({"error": "Monitor not found"}), 404
    return jsonify(dict(row))


@app.get("/api/monitors/<int:monitor_id>")
@require_auth
def api_get_monitor_dup3(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    conn.close()
    if not row:
        return jsonify({"error": "Monitor not found"}), 404
    return jsonify(dict(row))
@app.get("/api/checkout/tasks/<int:task_id>/attempts")
@require_auth
def api_checkout_task_attempts(task_id: int):
    conn = db()
    task = get_checkout_task_for_workspace(conn, task_id, current_workspace_id())
    if not task:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    attempts = conn.execute(
        """
        select id, task_id, state, status, details, error_text, created_at
        from checkout_attempts
        where task_id = ? and workspace_id = ?
        order by id desc
        """,
        (task_id, current_workspace_id()),
    ).fetchall()
    conn.close()
    return jsonify(
        [
            {
                "id": row["id"],
                "task_id": row["task_id"],
                "state": row["state"],
                "step": row["status"],
                "details": parse_json_object(row["details"]),
                "error": row["error_text"],
                "created_at": row["created_at"],
            }
            for row in attempts
        ]
    )




@app.get("/api/checkout/tasks")
@require_auth
def api_list_checkout_tasks_dup1():
    conn = db()
    rows = conn.execute(
        "select * from checkout_tasks where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_task_ui(row) for row in rows])


@app.get("/api/checkout/tasks")
@require_auth
def api_list_checkout_tasks_dup2():
    conn = db()
    rows = conn.execute(
        "select * from checkout_tasks where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_task_ui(row) for row in rows])


@app.get("/api/checkout/tasks")
@require_auth
def api_list_checkout_tasks_dup3():
    conn = db()
    rows = conn.execute(
        "select * from checkout_tasks where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_task_ui(row) for row in rows])


@app.get("/api/checkout/tasks")
@require_auth
def api_list_checkout_tasks_dup4():
    conn = db()
    rows = conn.execute(
        "select * from checkout_tasks where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_task_ui(row) for row in rows])


@app.post("/api/checkout/tasks/<int:task_id>/start")
@require_auth
def api_start_checkout_task(task_id: int):
    conn = db()
    row = transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=current_workspace_id(),
        requested_state="monitoring",
        reason="api_start",
    )
    if not row:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    conn.commit()
    conn.close()
    payload = serialize_checkout_task_summary(row)
    socketio.emit("task_update", payload)
    return jsonify({"ok": True, "task": payload})


@app.post("/api/checkout/tasks/start-now")
@require_auth
def api_start_checkout_tasks_now():
    body = request.json or {}
    task_ids = body.get("task_ids") if isinstance(body.get("task_ids"), list) else []
    normalized_ids = [int(task_id) for task_id in task_ids if str(task_id).isdigit()]
    if not normalized_ids:
        return jsonify({"error": "task_ids is required"}), 400
    conn = db()
    workspace_id = current_workspace_id()
    updated_tasks: list[dict[str, Any]] = []
    for task_id in normalized_ids:
        task = get_checkout_task_for_workspace(conn, task_id, workspace_id)
        if not task:
            continue
        row = transition_checkout_task(
            conn,
            task_id=task_id,
            workspace_id=workspace_id,
            requested_state="monitoring",
            reason="api_start_now_override",
        )
        if row:
            updated_tasks.append(serialize_task_ui(row))
    conn.commit()
    conn.close()
    for payload in updated_tasks:
        socketio.emit("task_update", payload)
    return jsonify({"ok": True, "tasks": updated_tasks})


@app.post("/api/checkout/tasks/<int:task_id>/run")
@require_auth
def api_run_checkout_task(task_id: int):
    row = execute_checkout_task_state_machine(task_id, current_workspace_id())
    if not row:
        return jsonify({"error": "Checkout task not found"}), 404
    return jsonify({"ok": True, "task": serialize_checkout_task(row)})


@app.post("/api/checkout/tasks/<int:task_id>/run")
@require_auth
def api_run_checkout_task_dup1(task_id: int):
    row = execute_checkout_task_state_machine(task_id, current_workspace_id())
    if not row:
        return jsonify({"error": "Checkout task not found"}), 404
    return jsonify({"ok": True, "task": serialize_checkout_task(row)})


@app.post("/api/checkout/tasks/<int:task_id>/run")
@require_auth
def api_run_checkout_task_dup2(task_id: int):
    row = execute_checkout_task_state_machine(task_id, current_workspace_id())
    if not row:
        return jsonify({"error": "Checkout task not found"}), 404
    return jsonify({"ok": True, "task": serialize_checkout_task(row)})


@app.post("/api/checkout/tasks/<int:task_id>/pause")
@require_auth
def api_pause_checkout_task(task_id: int):
    conn = db()
    row = transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=current_workspace_id(),
        requested_state="paused",
        reason="api_pause",
    )
    if not row:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    conn.commit()
    conn.close()
    payload = serialize_checkout_task_summary(row)
    socketio.emit("task_update", payload)
    return jsonify({"ok": True, "task": payload})


@app.post("/api/checkout/tasks/<int:task_id>/stop")
@require_auth
def api_stop_checkout_task(task_id: int):
    conn = db()
    row = transition_checkout_task(
        conn,
        task_id=task_id,
        workspace_id=current_workspace_id(),
        requested_state="stopped",
        reason="api_stop",
    )
    if not row:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    conn.commit()
    conn.close()
    payload = serialize_checkout_task_summary(row)
    socketio.emit("task_update", payload)
    return jsonify({"ok": True, "task": payload})




@app.get("/api/checkout/tasks/<int:task_id>/attempts")
@require_auth
def api_checkout_task_attempts_dup1(task_id: int):
    conn = db()
    row = get_checkout_task_for_workspace(conn, task_id, current_workspace_id())
    if not row:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    attempts = conn.execute(
        """
        select id, task_id, state, status, details, error_text, created_at
        from checkout_attempts
        where task_id = ?
        order by id desc
        """,
        (task_id,),
    ).fetchall()
    conn.close()
    return jsonify(
        [
            {
                "id": a["id"],
                "task_id": a["task_id"],
                "state": a["state"],
                "step": a["status"],
                "details": parse_json_object(a["details"]),
                "error": a["error_text"],
                "created_at": a["created_at"],
            }
            for a in attempts
        ]
    )


@app.get("/api/checkout/tasks/<int:task_id>/attempts")
@require_auth
def api_checkout_task_attempts_dup2(task_id: int):
    conn = db()
    row = get_checkout_task_for_workspace(conn, task_id, current_workspace_id())
    if not row:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    attempts = conn.execute(
        """
        select id, task_id, state, status, details, error_text, created_at
        from checkout_attempts
        where task_id = ?
        order by id desc
        """,
        (task_id,),
    ).fetchall()
    conn.close()
    return jsonify(
        [
            {
                "id": a["id"],
                "task_id": a["task_id"],
                "state": a["state"],
                "step": a["status"],
                "details": parse_json_object(a["details"]),
                "error": a["error_text"],
                "created_at": a["created_at"],
            }
            for a in attempts
        ]
    )


@app.get("/api/checkout/tasks/<int:task_id>/attempts")
@require_auth
def api_checkout_task_attempts_dup3(task_id: int):
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    if not row:
        conn.close()
        return jsonify({"error": "Monitor not found"}), 404
    conn.execute(
        "update monitors set enabled = ? where id = ? and workspace_id = ?",
        (int(bool(enabled)), monitor_id, workspace_id),
    )
    conn.commit()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    conn.close()
    return jsonify(dict(row))


@app.delete("/api/monitors/<int:monitor_id>")
@require_auth
def api_delete_monitor(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    if not row:
        conn.close()
        return jsonify({"error": "Monitor not found"}), 404
    conn.execute("delete from monitors where id = ? and workspace_id = ?", (monitor_id, workspace_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})
    row = get_checkout_task_for_workspace(conn, task_id, current_workspace_id())
    if not row:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    attempts = conn.execute(
        """
        select id, task_id, state, status, error_text, created_at
        from checkout_attempts
        where task_id = ?
        order by id desc
        """,
        (task_id,),
    ).fetchall()
    conn.close()
    return jsonify(
        [
            {
                "id": a["id"],
                "task_id": a["task_id"],
                "state": a["state"],
                "step": a["status"],
                "error": a["error_text"],
                "created_at": a["created_at"],
            }
            for a in attempts
        ]
    )


@app.get("/api/checkout/tasks/<int:task_id>/state")
@require_auth
def api_checkout_task_state(task_id: int):
    conn = db()
    row = get_monitor_for_workspace(conn, monitor_id, workspace_id)
    row = get_checkout_task_for_workspace(conn, task_id, current_workspace_id())
    if not row:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    last_attempt = conn.execute(
        """
        select * from checkout_attempts
        where task_id = ?
        order by id desc
        limit 1
        """,
        (task_id,),
    ).fetchone()
    conn.close()
    return jsonify(
        {
            "task_id": task_id,
            "current_state": row["current_state"],
            "last_error": row["last_error"],
            "last_transition_at": row["last_transition_at"],
            "last_attempt": dict(last_attempt) if last_attempt else None,
        }
    )


@app.post("/api/checkout/tasks/<int:task_id>/captcha-challenges")
@require_auth
def api_create_checkout_captcha_challenge(task_id: int):
    conn = db()
    workspace_id = current_workspace_id()
    task = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    if not task:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404

    checkout_captcha_service.expire_stale_challenges(conn)
    retailer_account_id = get_retailer_account_id_for_task(
        conn,
        workspace_id=workspace_id,
        monitor_id=task["monitor_id"],
    )
    challenge = checkout_captcha_service.create_challenge(
        conn,
        workspace_id=workspace_id,
        task_id=task_id,
        retailer_account_id=retailer_account_id,
        provider_name=checkout_solve_provider.name,
    )
    attempt = checkout_solve_provider.attempt_solve(dict(challenge))
    checkout_captcha_service.mark_attempt_result(conn, challenge_id=challenge["id"], attempt=attempt)
    updated = conn.execute("select * from captcha_challenges where id = ?", (challenge["id"],)).fetchone()
    conn.commit()
    conn.close()
    assert updated is not None
    emit_captcha_challenge_update(updated)
    return jsonify(serialize_challenge(updated)), 201


@app.get("/api/checkout/tasks")
@require_auth
def api_list_checkout_tasks_dup5():
    conn = db()
    rows = conn.execute(
        "select * from checkout_tasks where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_task_ui(row) for row in rows])


@app.post("/api/checkout/tasks/<int:task_id>/start")
@app.get("/api/checkout/tasks/<int:task_id>/captcha-challenges")
@require_auth
def api_list_checkout_captcha_challenges(task_id: int):
    conn = db()
    workspace_id = current_workspace_id()
    task = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    if not task:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    checkout_captcha_service.expire_stale_challenges(conn)
    rows = conn.execute(
        """
        select * from captcha_challenges
        where workspace_id = ? and task_id = ?
        order by id desc
        """,
        (workspace_id, task_id),
    ).fetchall()
    conn.commit()
    conn.close()
    return jsonify([serialize_challenge(row) for row in rows])


@app.post("/api/checkout/tasks/<int:task_id>/run")
@require_auth
def api_run_checkout_task_dup3(task_id: int):
    row = execute_checkout_task_state_machine(task_id, current_workspace_id())
    if not row:
        return jsonify({"error": "Checkout task not found"}), 404
    return jsonify({"ok": True, "task": serialize_checkout_task(row)})


@app.post("/api/checkout/tasks/<int:task_id>/pause")
@app.post("/api/checkout/captcha-challenges/<int:challenge_id>/manual-solve")
@require_auth
def api_submit_manual_captcha_solution(challenge_id: int):
    body = request.json or {}
    solved_token = (body.get("solved_token") or "").strip()
    if not solved_token:
        return jsonify({"error": "solved_token is required"}), 400

    conn = db()
    row = conn.execute(
        """
        select cc.* from captcha_challenges cc
        where cc.id = ? and cc.workspace_id = ?
        """,
        (challenge_id, current_workspace_id()),
    ).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Captcha challenge not found"}), 404
    checkout_captcha_service.mark_manual_solution(
        conn,
        challenge_id=challenge_id,
        solved_token=solved_token,
        operator_note=(body.get("operator_note") or "").strip() or None,
    )
    updated = conn.execute("select * from captcha_challenges where id = ?", (challenge_id,)).fetchone()
    conn.commit()
    conn.close()
    assert updated is not None
    emit_captcha_challenge_update(updated)
    return jsonify({"ok": True, "challenge": serialize_challenge(updated)})


@app.post("/api/checkout/captcha-challenges/<int:challenge_id>/handoff-token")
@require_auth
def api_issue_captcha_handoff_token(challenge_id: int):
    conn = db()
    row = conn.execute(
        """
        select * from captcha_challenges
        where id = ? and workspace_id = ?
        """,
        (challenge_id, current_workspace_id()),
    ).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Captcha challenge not found"}), 404
    try:
        token = checkout_captcha_service.issue_worker_handoff_token(conn, challenge_id=challenge_id)
    except ValueError as exc:
        conn.close()
        return jsonify({"error": str(exc)}), 400
    updated = conn.execute("select * from captcha_challenges where id = ?", (challenge_id,)).fetchone()
    conn.commit()
    conn.close()
    if updated:
        emit_captcha_challenge_update(updated)
    return jsonify({"ok": True, "challenge_id": challenge_id, "handoff_token": token})


@app.get("/api/checkout/tasks/<int:task_id>/attempts")
@require_auth
def api_checkout_task_attempts_dup4(task_id: int):
    conn = db()
    row = get_checkout_task_for_workspace(conn, task_id, current_workspace_id())
    if not row:
        conn.close()
        return jsonify({"error": "Checkout task not found"}), 404
    include_created = request.args.get("include_created", "").strip().lower() in {"1", "true", "yes"}
    if include_created:
        attempts = conn.execute(
            """
            select id, task_id, state, status, details, error_text, created_at
            from checkout_attempts
            where task_id = ?
            order by id desc
            """,
            (task_id,),
        ).fetchall()
    else:
        attempts = conn.execute(
            """
            select id, task_id, state, status, details, error_text, created_at
            from checkout_attempts
            where task_id = ?
              and status != 'created'
            order by id desc
            """,
            (task_id,),
        ).fetchall()
    conn.close()
    return jsonify(
        [
            {
                "id": a["id"],
                "task_id": a["task_id"],
                "state": a["state"],
                "step": a["status"],
                "details": parse_json_object(a["details"]),
                "error": a["error_text"],
                "created_at": a["created_at"],
            }
            for a in attempts
        ]
    )


@app.get("/api/checkout/tasks/<int:task_id>/state")
@app.post("/api/internal/checkout/captcha-handoffs/consume")
@require_auth
def api_consume_captcha_handoff_token():
    body = request.json or {}
    token = (body.get("handoff_token") or "").strip()
    if not token:
        return jsonify({"error": "handoff_token is required"}), 400
    conn = db()
    try:
        payload = checkout_captcha_service.consume_worker_handoff_token(conn, token=token)
    except ValueError as exc:
        conn.close()
        return jsonify({"error": str(exc)}), 400
    updated = conn.execute("select * from captcha_challenges where id = ?", (payload["challenge_id"],)).fetchone()
    conn.commit()
    conn.close()
    if updated:
        emit_captcha_challenge_update(updated)
    return jsonify({"ok": True, **payload})


@app.get("/api/events")
@require_auth
def api_events():
    workspace_id = get_workspace_id_for_request()
    conn = db()
    rows = conn.execute(
        """
        select e.*, m.retailer as monitor_retailer from events e
        join monitors m on m.id = e.monitor_id
        where m.workspace_id = ?
        order by e.id desc limit 100
        """
        ,
        (workspace_id,),
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.get("/api/alert-subscriptions")
@require_auth
def api_list_alert_subscriptions():
    conn = db()
    rows = conn.execute(
        "select * from alert_subscriptions where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])


@app.post("/api/alert-subscriptions")
@require_auth
def api_create_alert_subscription():
    body = request.get_json(force=True) or {}
    guild_id = str(body.get("guild_id") or "").strip()
    channel_id = str(body.get("channel_id") or "").strip()
    source = str(body.get("source") or "discord").strip().lower() or "discord"
    if not guild_id or not channel_id:
        return jsonify({"error": "guild_id and channel_id are required"}), 400
    now_iso = utc_now()
    conn = db()
    cur = conn.execute(
        """
        insert into alert_subscriptions(
            workspace_id, guild_id, channel_id, source, source_name, retailer_filter,
            url_patterns, sku_patterns, keyword_patterns, enabled, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            current_workspace_id(),
            guild_id,
            channel_id,
            source,
            body.get("source_name"),
            body.get("retailer_filter"),
            json.dumps(_json_list(body.get("url_patterns"))),
            json.dumps(_json_list(body.get("sku_patterns"))),
            json.dumps(_json_list(body.get("keyword_patterns"))),
            int(bool(body.get("enabled", True))),
            now_iso,
            now_iso,
        ),
    )
    row = conn.execute("select * from alert_subscriptions where id = ?", (cur.lastrowid,)).fetchone()
    conn.commit()
    conn.close()
    return jsonify(dict(row)), 201


@app.post("/api/alerts/discord/ingest")
@require_auth
def api_ingest_discord_alert():
    body = request.get_json(force=True) or {}
    subscription_id = int(body.get("subscription_id") or 0)
    raw_event = body.get("event") if isinstance(body.get("event"), dict) else {}
    if not subscription_id or not raw_event:
        return jsonify({"error": "subscription_id and event object are required"}), 400
    conn = db()
    subscription = conn.execute(
        "select * from alert_subscriptions where id = ? and workspace_id = ?",
        (subscription_id, current_workspace_id()),
    ).fetchone()
    if not subscription:
        conn.close()
        return jsonify({"error": "subscription not found"}), 404
    now_iso = utc_now()
    cur = conn.execute(
        """
        insert into jobs(job_type, monitor_id, status, attempt_count, next_run_at, payload_json, created_at, updated_at)
        values ('discord_ingest_event', null, 'queued', 0, ?, ?, ?, ?)
        """,
        (
            now_iso,
            json.dumps(
                {
                    "subscription_id": subscription_id,
                    "source_name": subscription["source_name"] or subscription["source"],
                    "raw_event": raw_event,
                }
            ),
            now_iso,
            now_iso,
        ),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True, "job_id": cur.lastrowid}), 202


@app.get("/api/alerts/events")
@require_auth
def api_list_alert_events():
    conn = db()
    rows = conn.execute(
        """
        select ae.*, s.guild_id, s.channel_id, s.source_name
        from alert_events ae
        join alert_subscriptions s on s.id = ae.subscription_id
        where ae.workspace_id = ?
        order by ae.id desc
        limit 200
        """,
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([dict(row) for row in rows])


@app.post("/api/webhooks")
@require_auth
def api_add_webhook():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    body = request.json or {}
    name = (body.get("name") or "Discord").strip()
    url = (body.get("webhook_url") or "").strip()
    notify_success = int(bool(body.get("notify_success", True)))
    notify_failures = int(bool(body.get("notify_failures", False)))
    notify_restock_only = int(bool(body.get("notify_restock_only", True)))
    if not url.startswith("https://discord.com/api/webhooks/"):
        return jsonify({"error": "Invalid Discord webhook URL"}), 400

    workspace_id = get_workspace_id_for_request()
    conn = db()
    secret_id = create_secret(conn, workspace_id, "webhook_url", url, int(g.current_user["id"]))
    cur = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, webhook_secret_id, notify_success, notify_failures, notify_restock_only, created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            workspace_id,
            name,
            redact_webhook_url(url),
            secret_id,
            notify_success,
            notify_failures,
            notify_restock_only,
            utc_now(),
        ),
    )
    conn.commit()
    row = conn.execute("select * from webhooks where id = ?", (cur.lastrowid,)).fetchone()
    conn.close()
    return jsonify(serialize_webhook(row)), 201


def get_webhook_for_workspace(
    conn: sqlite3.Connection, webhook_id: int, workspace_id: int
) -> sqlite3.Row | None:
    return conn.execute(
        "select * from webhooks where id = ? and workspace_id = ?",
        (webhook_id, workspace_id),
    ).fetchone()


@app.get("/api/webhooks")
@require_auth
def api_list_webhooks():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    workspace_id = get_workspace_id_for_request()
    conn = db()
    rows = conn.execute(
        "select * from webhooks where workspace_id = ? order by id desc",
        (workspace_id,),
    ).fetchall()
    conn.close()
    return jsonify([serialize_webhook(r) for r in rows])


@app.post("/api/webhooks/<int:webhook_id>/test")
@require_auth
def api_test_webhook(webhook_id: int):
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    workspace_id = get_workspace_id_for_request()
    conn = db()
    hook = get_webhook_for_workspace(conn, webhook_id, workspace_id)
    conn.close()
    if not hook:
        return jsonify({"error": "Webhook not found"}), 404

    payload = {
        "username": "Stock Sentinel",
        "content": "🧪 Test alert from Stock Sentinel",
    }
    started = time.perf_counter()
    conn = db()
    try:
        target_url = resolve_webhook_url(conn, hook)
        req = perform_request(
            task_key=f"webhook-test-{webhook_id}",
            method="POST",
            url=target_url,
            workspace_id=workspace_id,
            proxy_url=None,
            timeout=8,
            retry_total=1,
            backoff_factor=0.2,
            json=payload,
        )
        if req.error:
            raise req.error
        assert req.response is not None
        resp = req.response
        ok = 200 <= resp.status_code < 300
        status = "sent" if ok else "failed"
        body = (resp.text or "")[:500]
        update_webhook_health(conn, webhook_id, status=status, status_code=resp.status_code, error_text=body, tested=True)
        conn.commit()
        conn.close()
        return jsonify(
            {
                "ok": ok,
                "status_code": resp.status_code,
                "response_body": body,
                "latency_ms": int((time.perf_counter() - started) * 1000),
            }
        )
    except Exception as exc:  # noqa: BLE001
        update_webhook_health(conn, webhook_id, status="failed", error_text=str(exc), tested=True)
        conn.commit()
        conn.close()
        return jsonify({"ok": False, "error": str(exc), "latency_ms": int((time.perf_counter() - started) * 1000)}), 500


@app.patch("/api/webhooks/<int:webhook_id>")
@require_auth
def api_update_webhook(webhook_id: int):
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    body = request.json or {}
    fields: list[tuple[str, Any]] = []
    if "enabled" in body:
        fields.append(("enabled", int(bool(body["enabled"]))))
    if "notify_success" in body:
        fields.append(("notify_success", int(bool(body["notify_success"]))))
    if "notify_failures" in body:
        fields.append(("notify_failures", int(bool(body["notify_failures"]))))
    if "notify_restock_only" in body:
        fields.append(("notify_restock_only", int(bool(body["notify_restock_only"]))))
    if not fields:
        return jsonify({"error": "No mutable fields provided"}), 400
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_webhook_for_workspace(conn, webhook_id, workspace_id)
    if not row:
        conn.close()
        return jsonify({"error": "Webhook not found"}), 404
    for key, value in fields:
        conn.execute(
            f"update webhooks set {key} = ? where id = ? and workspace_id = ?",
            (value, webhook_id, workspace_id),
        )
    conn.commit()
    row = get_webhook_for_workspace(conn, webhook_id, workspace_id)
    conn.close()
    return jsonify(dict(row))


@app.delete("/api/webhooks/<int:webhook_id>")
@require_auth
def api_delete_webhook(webhook_id: int):
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = get_webhook_for_workspace(conn, webhook_id, workspace_id)
    if not row:
        conn.close()
        return jsonify({"error": "Webhook not found"}), 404
    conn.execute("delete from webhooks where id = ? and workspace_id = ?", (webhook_id, workspace_id))
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.post("/api/profiles")
@require_auth
def api_create_profile():
    body = request.json or {}
    try:
        name = (body.get("name") or "").strip()
        if not name:
            raise ValueError("name is required")
        email = _validate_email(body.get("email") or "")
        phone = _validate_phone(body.get("phone"))
        shipping = _validate_address(body.get("shipping_address"), "shipping_address")
        billing = _validate_address(body.get("billing_address"), "billing_address")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    now_iso = utc_now()
    conn = db()
    cur = conn.execute(
        """
        insert into checkout_profiles(
            workspace_id, name, email, phone, shipping_address_json, billing_address_json, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (current_workspace_id(), name, email, phone, json.dumps(shipping), json.dumps(billing), now_iso, now_iso),
    )
    conn.commit()
    row = conn.execute(
        "select * from checkout_profiles where id = ? and workspace_id = ?",
        (cur.lastrowid, current_workspace_id()),
    ).fetchone()
    conn.close()
    return jsonify(serialize_checkout_profile(row)), 201


@app.get("/api/profiles")
@require_auth
def api_list_profiles():
    conn = db()
    rows = conn.execute(
        "select * from checkout_profiles where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_checkout_profile(r) for r in rows])


@app.patch("/api/profiles/<int:profile_id>")
@require_auth
def api_update_profile(profile_id: int):
    body = request.json or {}
    fields: list[tuple[str, Any]] = []
    try:
        if "name" in body:
            name = (body.get("name") or "").strip()
            if not name:
                raise ValueError("name cannot be empty")
            fields.append(("name", name))
        if "email" in body:
            fields.append(("email", _validate_email(body.get("email") or "")))
        if "phone" in body:
            fields.append(("phone", _validate_phone(body.get("phone"))))
        if "shipping_address" in body:
            fields.append(("shipping_address_json", json.dumps(_validate_address(body.get("shipping_address"), "shipping_address"))))
        if "billing_address" in body:
            fields.append(("billing_address_json", json.dumps(_validate_address(body.get("billing_address"), "billing_address"))))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not fields:
        return jsonify({"error": "No mutable fields provided"}), 400
    fields.append(("updated_at", utc_now()))
    conn = db()
    for key, value in fields:
        conn.execute(
            f"update checkout_profiles set {key} = ? where id = ? and workspace_id = ?",
            (value, profile_id, current_workspace_id()),
        )
    conn.commit()
    row = conn.execute(
        "select * from checkout_profiles where id = ? and workspace_id = ?",
        (profile_id, current_workspace_id()),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Profile not found"}), 404
    return jsonify(serialize_checkout_profile(row))


@app.delete("/api/profiles/<int:profile_id>")
@require_auth
def api_delete_profile(profile_id: int):
    conn = db()
    cur = conn.execute(
        "delete from checkout_profiles where id = ? and workspace_id = ?",
        (profile_id, current_workspace_id()),
    )
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        return jsonify({"error": "Profile not found"}), 404
    return jsonify({"ok": True})


@app.post("/api/accounts")
@require_auth
def api_create_account():
    body = request.json or {}
    try:
        retailer = canonical_retailer(body.get("retailer") or "")
        if retailer not in SUPPORTED_RETAILERS:
            raise ValueError(f"Unsupported retailer '{retailer}'")
        username = (body.get("username") or "").strip() or None
        email = body.get("email")
        email = _validate_email(email) if email else None
        if not username and not email:
            raise ValueError("username or email is required")
        credential_ref = (body.get("encrypted_credential_ref") or "").strip()
        if not credential_ref:
            raise ValueError("encrypted_credential_ref is required")
        proxy_url = (body.get("proxy_url") or "").strip() or None
        session_status = (body.get("session_status") or "logged_out").strip().lower()
        if session_status not in SESSION_STATUSES:
            raise ValueError("Invalid session_status")
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400

    now_iso = utc_now()
    conn = db()
    cur = conn.execute(
        """
        insert into retailer_accounts(
            workspace_id, retailer, username, email, encrypted_credential_ref, proxy_url, session_status, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (current_workspace_id(), retailer, username, email, credential_ref, proxy_url, session_status, now_iso, now_iso),
    )
    conn.commit()
    row = conn.execute(
        "select * from retailer_accounts where id = ? and workspace_id = ?",
        (cur.lastrowid, current_workspace_id()),
    ).fetchone()
    conn.close()
    return jsonify(serialize_retailer_account(row)), 201


@app.get("/api/accounts")
@require_auth
def api_list_accounts():
    conn = db()
    rows = conn.execute(
        "select * from retailer_accounts where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_retailer_account(r) for r in rows])


@app.patch("/api/accounts/<int:account_id>")
@require_auth
def api_update_account(account_id: int):
    body = request.json or {}
    fields: list[tuple[str, Any]] = []
    try:
        if "retailer" in body:
            retailer = canonical_retailer(body.get("retailer") or "")
            if retailer not in SUPPORTED_RETAILERS:
                raise ValueError(f"Unsupported retailer '{retailer}'")
            fields.append(("retailer", retailer))
        if "username" in body:
            fields.append(("username", (body.get("username") or "").strip() or None))
        if "email" in body:
            email = body.get("email")
            fields.append(("email", _validate_email(email) if email else None))
        if "encrypted_credential_ref" in body:
            credential_ref = (body.get("encrypted_credential_ref") or "").strip()
            if not credential_ref:
                raise ValueError("encrypted_credential_ref cannot be empty")
            fields.append(("encrypted_credential_ref", credential_ref))
        if "proxy_url" in body:
            fields.append(("proxy_url", (body.get("proxy_url") or "").strip() or None))
        if "session_status" in body:
            session_status = (body.get("session_status") or "").strip().lower()
            if session_status not in SESSION_STATUSES:
                raise ValueError("Invalid session_status")
            fields.append(("session_status", session_status))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not fields:
        return jsonify({"error": "No mutable fields provided"}), 400
    fields.append(("updated_at", utc_now()))
    conn = db()
    for key, value in fields:
        conn.execute(
            f"update retailer_accounts set {key} = ? where id = ? and workspace_id = ?",
            (value, account_id, current_workspace_id()),
        )
    conn.commit()
    row = conn.execute(
        "select * from retailer_accounts where id = ? and workspace_id = ?",
        (account_id, current_workspace_id()),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Account not found"}), 404
    return jsonify(serialize_retailer_account(row))


@app.delete("/api/accounts/<int:account_id>")
@require_auth
def api_delete_account(account_id: int):
    conn = db()
    cur = conn.execute(
        "delete from retailer_accounts where id = ? and workspace_id = ?",
        (account_id, current_workspace_id()),
    )
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        return jsonify({"error": "Account not found"}), 404
    return jsonify({"ok": True})


@app.post("/api/task-profile-bindings")
@require_auth
def api_upsert_task_profile_binding():
    body = request.json or {}
    try:
        monitor_id = int(body.get("monitor_id"))
    except (TypeError, ValueError):
        return jsonify({"error": "monitor_id is required"}), 400
    workspace_id = current_workspace_id()
    checkout_profile_id = body.get("checkout_profile_id")
    payment_method_id = body.get("payment_method_id")
    retailer_account_id = body.get("retailer_account_id")
    if retailer_account_id is None:
        return jsonify({"error": "retailer_account_id is required for checkout-capable task bindings"}), 400
    try:
        retailer_account_id = int(retailer_account_id)
        checkout_profile_id = int(checkout_profile_id) if checkout_profile_id is not None else None
        payment_method_id = int(payment_method_id) if payment_method_id is not None else None
    except ValueError:
        return jsonify({"error": "Invalid numeric identifier in binding payload"}), 400

    now_iso = utc_now()
    conn = db()
    monitor = conn.execute(
        "select id from monitors where id = ? and workspace_id = ?",
        (monitor_id, workspace_id),
    ).fetchone()
    if not monitor:
        conn.close()
        return jsonify({"error": "Monitor not found"}), 404
    account = conn.execute(
        "select id from retailer_accounts where id = ? and workspace_id = ?",
        (retailer_account_id, workspace_id),
    ).fetchone()
    if not account:
        conn.close()
        return jsonify({"error": "retailer_account_id not found"}), 400
    if checkout_profile_id is not None:
        profile = conn.execute(
            "select id from checkout_profiles where id = ? and workspace_id = ?",
            (checkout_profile_id, workspace_id),
        ).fetchone()
        if not profile:
            conn.close()
            return jsonify({"error": "checkout_profile_id not found"}), 400
    if payment_method_id is not None:
        payment = conn.execute(
            "select id from payment_methods where id = ? and workspace_id = ?",
            (payment_method_id, workspace_id),
        ).fetchone()
        if not payment:
            conn.close()
            return jsonify({"error": "payment_method_id not found"}), 400
    conn.execute(
        """
        insert into task_profile_bindings(
            workspace_id, monitor_id, checkout_profile_id, retailer_account_id, payment_method_id, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?)
        on conflict(workspace_id, monitor_id) do update set
            checkout_profile_id = excluded.checkout_profile_id,
            retailer_account_id = excluded.retailer_account_id,
            payment_method_id = excluded.payment_method_id,
            updated_at = excluded.updated_at
        """,
        (workspace_id, monitor_id, checkout_profile_id, retailer_account_id, payment_method_id, now_iso, now_iso),
    )
    conn.commit()
    row = conn.execute(
        "select * from task_profile_bindings where workspace_id = ? and monitor_id = ?",
        (workspace_id, monitor_id),
    ).fetchone()
    conn.close()
    return jsonify(serialize_task_profile_binding(row)), 201


@app.get("/api/task-profile-bindings")
@require_auth
def api_list_task_profile_bindings():
    conn = db()
    rows = conn.execute(
        "select * from task_profile_bindings where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_task_profile_binding(r) for r in rows])


def _account_execution_read_model(conn: sqlite3.Connection, workspace_id: int) -> list[dict[str, Any]]:
    rows = conn.execute(
        """
        select
            a.*,
            count(t.id) as queue_depth,
            min(case when t.current_state != 'idle' then t.id end) as active_task_id,
            min(case when t.current_state != 'idle' then t.current_state end) as active_task_state
        from retailer_accounts a
        left join task_profile_bindings b
          on b.workspace_id = a.workspace_id
         and b.retailer_account_id = a.id
        left join checkout_tasks t
          on t.workspace_id = a.workspace_id
         and t.monitor_id = b.monitor_id
         and t.is_paused = 0
         and t.current_state in ('starting', 'waiting_for_queue', 'solving_hcaptcha', 'in_queue', 'passed_queue', 'waiting_for_monitor_input', 'monitoring_product', 'adding_to_cart', 'checking_out', 'requeued')
        where a.workspace_id = ?
        group by a.id
        order by a.id asc
        """,
        (workspace_id,),
    ).fetchall()
    payload: list[dict[str, Any]] = []
    for row in rows:
        item = dict(row)
        queue_depth = int(item["queue_depth"] or 0)
        if queue_depth == 0:
            execution_status = "idle"
        elif item["proxy_lock_state"] == "locked":
            execution_status = "running" if item["active_task_id"] else "queued_waiting_for_stagger"
        else:
            execution_status = "queued_waiting_for_proxy"
        item["execution_status"] = execution_status
        payload.append(item)
    return payload


@app.get("/api/accounts/execution")
@require_auth
def api_account_execution_status():
    workspace_id = current_workspace_id()
    conn = db()
    payload = _account_execution_read_model(conn, workspace_id)
    conn.close()
    return jsonify(payload)


@app.get("/api/accounts/proxy-locks")
@require_auth
def api_account_proxy_locks():
    workspace_id = current_workspace_id()
    conn = db()
    rows = conn.execute(
        """
        select id as account_id, retailer, proxy_url, proxy_lock_state, proxy_lock_owner, proxy_lock_acquired_at, last_used_at
        from retailer_accounts
        where workspace_id = ?
        order by id asc
        """,
        (workspace_id,),
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.post("/api/payments")
@require_auth
def api_create_payment():
    body = request.json or {}
    prohibited = {"pan", "card_number", "number", "cvv", "cvc"}
    if any(key in body for key in prohibited):
        return jsonify({"error": "Raw card data is not allowed; store tokenized reference only"}), 400
    try:
        label = (body.get("label") or "").strip()
        if not label:
            raise ValueError("label is required")
        provider = (body.get("provider") or "").strip() or None
        token_reference = (body.get("token_reference") or "").strip()
        if not token_reference:
            raise ValueError("token_reference is required")
        billing_profile_id = body.get("billing_profile_id")
        if billing_profile_id is not None:
            billing_profile_id = int(billing_profile_id)
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    now_iso = utc_now()
    conn = db()
    if billing_profile_id is not None:
        profile = conn.execute(
            "select id from checkout_profiles where id = ? and workspace_id = ?",
            (billing_profile_id, current_workspace_id()),
        ).fetchone()
        if not profile:
            conn.close()
            return jsonify({"error": "billing_profile_id not found"}), 400
    cur = conn.execute(
        """
        insert into payment_methods(
            workspace_id, label, provider, token_reference, billing_profile_id, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?)
        """,
        (current_workspace_id(), label, provider, token_reference, billing_profile_id, now_iso, now_iso),
    )
    conn.commit()
    row = conn.execute(
        "select * from payment_methods where id = ? and workspace_id = ?",
        (cur.lastrowid, current_workspace_id()),
    ).fetchone()
    conn.close()
    return jsonify(serialize_payment_method(row)), 201


@app.get("/api/payments")
@require_auth
def api_list_payments():
    conn = db()
    rows = conn.execute(
        "select * from payment_methods where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_payment_method(r) for r in rows])


@app.patch("/api/payments/<int:payment_id>")
@require_auth
def api_update_payment(payment_id: int):
    body = request.json or {}
    prohibited = {"pan", "card_number", "number", "cvv", "cvc"}
    if any(key in body for key in prohibited):
        return jsonify({"error": "Raw card data is not allowed; store tokenized reference only"}), 400
    fields: list[tuple[str, Any]] = []
    try:
        if "label" in body:
            label = (body.get("label") or "").strip()
            if not label:
                raise ValueError("label cannot be empty")
            fields.append(("label", label))
        if "provider" in body:
            fields.append(("provider", (body.get("provider") or "").strip() or None))
        if "token_reference" in body:
            token_reference = (body.get("token_reference") or "").strip()
            if not token_reference:
                raise ValueError("token_reference cannot be empty")
            fields.append(("token_reference", token_reference))
        if "billing_profile_id" in body:
            billing_profile_id = body.get("billing_profile_id")
            value = int(billing_profile_id) if billing_profile_id is not None else None
            fields.append(("billing_profile_id", value))
    except ValueError as exc:
        return jsonify({"error": str(exc)}), 400
    if not fields:
        return jsonify({"error": "No mutable fields provided"}), 400
    conn = db()
    for key, value in fields:
        if key == "billing_profile_id" and value is not None:
            profile = conn.execute(
                "select id from checkout_profiles where id = ? and workspace_id = ?",
                (value, current_workspace_id()),
            ).fetchone()
            if not profile:
                conn.close()
                return jsonify({"error": "billing_profile_id not found"}), 400
        conn.execute(
            f"update payment_methods set {key} = ? where id = ? and workspace_id = ?",
            (value, payment_id, current_workspace_id()),
        )
    conn.execute(
        "update payment_methods set updated_at = ? where id = ? and workspace_id = ?",
        (utc_now(), payment_id, current_workspace_id()),
    )
    conn.commit()
    row = conn.execute(
        "select * from payment_methods where id = ? and workspace_id = ?",
        (payment_id, current_workspace_id()),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Payment method not found"}), 404
    return jsonify(serialize_payment_method(row))


@app.delete("/api/payments/<int:payment_id>")
@require_auth
def api_delete_payment(payment_id: int):
    conn = db()
    cur = conn.execute(
        "delete from payment_methods where id = ? and workspace_id = ?",
        (payment_id, current_workspace_id()),
    )
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        return jsonify({"error": "Payment method not found"}), 404
    return jsonify({"ok": True})


@app.get("/api/schedules")
@require_auth
def api_list_schedules():
    workspace_id = get_workspace_id_for_request()
    conn = db()
    rows = conn.execute(
        """
        select s.*, m.retailer, m.product_url
        from monitor_schedules s
        join monitors m on m.id = s.monitor_id
        where m.workspace_id = ?
        order by s.id desc
        """
        ,
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.post("/api/schedules")
@require_auth
def api_create_schedule():
    workspace_id = get_workspace_id_for_request()
    body = request.json or {}
    monitor_ids = body.get("monitor_ids") or []
    run_at = (body.get("run_at") or "").strip()
    new_poll = body.get("new_poll_interval_seconds")
    if not monitor_ids or not isinstance(monitor_ids, list):
        return jsonify({"error": "monitor_ids array is required"}), 400
    if not run_at:
        return jsonify({"error": "run_at is required"}), 400
    try:
        datetime.fromisoformat(run_at.replace("Z", "+00:00"))
        new_poll_int = int(new_poll)
        if new_poll_int < 1:
            raise ValueError
    except ValueError:
        return jsonify({"error": "Invalid schedule payload"}), 400

    conn = db()
    created = []
    for monitor_id in monitor_ids:
        row = conn.execute(
            "select * from monitors where id = ? and workspace_id = ?",
            (int(monitor_id), current_workspace_id()),
        ).fetchone()
        if not row:
            continue
        cur = conn.execute(
            """
            insert into monitor_schedules(monitor_id, new_poll_interval_seconds, run_at, created_at)
            values (?, ?, ?, ?)
            """,
            (int(monitor_id), new_poll_int, run_at, utc_now()),
        )
        created.append(cur.lastrowid)
    conn.commit()
    rows = conn.execute("select * from monitor_schedules where id in ({})".format(",".join("?" * len(created))), created).fetchall() if created else []
    conn.close()
    return jsonify({"ok": True, "created": [dict(r) for r in rows]})


@app.delete("/api/schedules/<int:schedule_id>")
@require_auth
def api_delete_schedule(schedule_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    conn.execute(
        """
        delete from monitor_schedules
        where id = ?
          and applied_at is null
          and monitor_id in (select id from monitors where workspace_id = ?)
        """,
        (schedule_id, current_workspace_id()),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.post("/api/start")
@require_auth
def api_start():
    global worker_running, worker_thread
    if not ENABLE_EMBEDDED_WORKER:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Embedded worker disabled. Use APP_ROLE=worker and external process management.",
                }
            ),
            409,
        )
    if worker_running:
        return jsonify({"ok": True, "worker_running": True, "mode": "embedded"})
    worker_running = True
    worker_thread = threading.Thread(target=worker_loop, daemon=True)
    worker_thread.start()
    return jsonify({"ok": True, "worker_running": True, "mode": "embedded"})


@app.post("/api/stop")
@require_auth
def api_stop():
    global worker_running
    if not ENABLE_EMBEDDED_WORKER:
        return (
            jsonify(
                {
                    "ok": False,
                    "error": "Embedded worker disabled. Stop worker process via external supervisor.",
                }
            ),
            409,
        )
    worker_running = False
    return jsonify({"ok": True, "worker_running": False, "mode": "embedded"})


if __name__ == "__main__":
    init_db()
    validate_startup_configuration()
    log(
        "Legal/ethical note: this project provides stock monitoring + alerts and an experimental checkout workflow.",
        level="warning",
    )
    listen_port = int(os.getenv("PORT", "5000"))
    if APP_ROLE == "worker":
        worker_running = True
        worker_loop()
    elif APP_ROLE == "all":
        if ENABLE_EMBEDDED_WORKER:
            worker_running = True
            worker_thread = threading.Thread(target=worker_loop, daemon=True)
            worker_thread.start()
        socketio.run(app, host="0.0.0.0", port=listen_port, allow_unsafe_werkzeug=True)
    else:
        socketio.run(app, host="0.0.0.0", port=listen_port, allow_unsafe_werkzeug=True)
