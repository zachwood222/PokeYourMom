from __future__ import annotations

import base64
import json
import os
import re
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
from retailers import (
    MonitorResult,
    canonical_retailer,
    default_parser,
    resolve_retailer_adapter,
    run_retailer_flow,
)

from network.session_manager import RequestResult, SessionManager

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

DB_PATH = os.getenv("DB_PATH", "bot.db")
POLL_LOOP_SECONDS = int(os.getenv("POLL_LOOP_SECONDS", "15"))
WORKER_IDLE_SLEEP_SECONDS = float(os.getenv("WORKER_IDLE_SLEEP_SECONDS", "2.0"))
WORKER_LOCK_TIMEOUT_SECONDS = int(os.getenv("WORKER_LOCK_TIMEOUT_SECONDS", "60"))
WORKER_ID = os.getenv("WORKER_ID", f"worker-{uuid4()}")
APP_ROLE = os.getenv("APP_ROLE", "api").lower()
ENABLE_EMBEDDED_WORKER = os.getenv("ENABLE_EMBEDDED_WORKER", "0") == "1"
DEFAULT_PLAN = os.getenv("DEFAULT_PLAN", "basic")
POKEMON_MSRP_BUFFER_CENTS = int(os.getenv("POKEMON_MSRP_BUFFER_CENTS", "1000"))
APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
RELEASE_CHANNEL = os.getenv("RELEASE_CHANNEL", "stable")
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
CAPTCHA_VERIFY_URL = os.getenv(
    "CAPTCHA_VERIFY_URL",
    "https://www.google.com/recaptcha/api/siteverify",
)
CAPTCHA_VERIFY_TIMEOUT_SECONDS = float(os.getenv("CAPTCHA_VERIFY_TIMEOUT_SECONDS", "2.0"))
TASK_STEP_DELAY_SECONDS = float(os.getenv("TASK_STEP_DELAY_SECONDS", "0.5"))
STRICT_API_AUTH_TOKEN = (os.getenv("STRICT_API_AUTH_TOKEN", "1") or "").strip().lower() not in {"0", "false", "no", "off"}

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
    "queued",
    "monitoring",
    "carting",
    "shipping",
    "payment",
    "submitting",
    "success",
    "failed",
    "paused",
    "stopped",
}



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


def db() -> sqlite3.Connection:
    conn = sqlite3.connect(DB_PATH)
    conn.row_factory = sqlite3.Row
    return conn


def ensure_column(conn: sqlite3.Connection, table: str, column: str, ddl: str) -> None:
    columns = {r["name"] for r in conn.execute(f"pragma table_info({table})").fetchall()}
    if column not in columns:
        conn.execute(f"alter table {table} add column {column} {ddl}")


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
            current_state text not null default 'queued',
            enabled integer not null default 0,
            is_paused integer not null default 0,
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
            state text not null,
            status text not null,
            details text,
            error_text text,
            created_at text not null,
            foreign key(task_id) references checkout_tasks(id),
            foreign key(workspace_id) references workspaces(id),
            foreign key(monitor_id) references monitors(id)
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
            session_status text not null default 'logged_out',
            created_at text not null,
            updated_at text not null,
            foreign key(workspace_id) references workspaces(id)
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

        create index if not exists idx_proxy_status_cooldown on proxies(status, cooldown_until);
        create index if not exists idx_proxy_leases_active on proxy_leases(proxy_id) where released_at is null;
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
    ensure_column(conn, "monitors", "proxy_url", "text")
    ensure_column(conn, "monitors", "proxy_type", "text")
    ensure_column(conn, "monitors", "proxy_region", "text")
    ensure_column(conn, "monitors", "proxy_residential_only", "integer not null default 0")
    ensure_column(conn, "monitors", "proxy_sticky_session_seconds", "integer")
    ensure_column(conn, "monitors", "session_task_key", "text")
    ensure_column(conn, "monitors", "session_metadata", "text")
    ensure_column(conn, "checkout_tasks", "active_proxy_id", "integer")
    ensure_column(conn, "checkout_tasks", "active_proxy_lease_key", "text")
    ensure_column(conn, "jobs", "job_type", "text not null default 'monitor_check'")
    ensure_column(conn, "jobs", "monitor_id", "integer")
    ensure_column(conn, "jobs", "payload_json", "text")
    ensure_column(conn, "jobs", "last_error", "text")
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
        timeout=timeout,
        retry_total=retry_total,
        backoff_factor=backoff_factor,
        **kwargs,
    )
    telemetry = result.telemetry
    level = "warning" if not telemetry.ok else "info"
    log(
        f"http_request task={telemetry.task_key} method={method.upper()} status={telemetry.status_code} "
        f"latency_ms={telemetry.latency_ms} error_class={telemetry.error_class}",
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


def get_workspace_for_request() -> sqlite3.Row:
    workspace = getattr(g, "current_workspace", None)
    if workspace is None:
        workspace = get_workspace(1)
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
    header_token = (request.headers.get("X-Captcha-Token") or "").strip()
    if header_token:
        return header_token
    body = request.get_json(silent=True)
    if isinstance(body, dict):
        for key in ("captcha_token", "cf-turnstile-response", "g-recaptcha-response"):
            token = body.get(key)
            if isinstance(token, str) and token.strip():
                return token.strip()
    for key in ("captcha_token", "cf-turnstile-response", "g-recaptcha-response"):
        token = (request.form.get(key) or "").strip()
        if token:
            return token
    return None


def verify_captcha_token(token: str) -> tuple[bool, str | None]:
    if not CAPTCHA_SECRET_KEY or not CAPTCHA_VERIFY_URL:
        return True, None
    if not token:
        return False, "missing_token"
    payload = {"secret": CAPTCHA_SECRET_KEY, "response": token}
    remote_ip = (request.headers.get("X-Forwarded-For") or request.remote_addr or "").split(",")[0].strip()
    if remote_ip:
        payload["remoteip"] = remote_ip
    try:
        response = requests.post(CAPTCHA_VERIFY_URL, data=payload, timeout=CAPTCHA_TIMEOUT_SECONDS)
    except Exception as exc:  # noqa: BLE001
        log(f"CAPTCHA verification request failed: {exc}", level="warning")
        return False, "provider_unreachable"
    if response.status_code >= 500:
        log(
            f"CAPTCHA verification provider returned {response.status_code}",
            level="warning",
        )
        return False, "provider_error"
    if response.status_code >= 400:
        return False, "provider_rejected"
    try:
        result = response.json()
    except ValueError:
        log("CAPTCHA verification provider returned non-JSON response", level="warning")
        return False, "provider_invalid_response"
    if not bool(result.get("success")):
        return False, "invalid_token"
    return True, None


def _is_captcha_protected_request() -> bool:
    if request.method not in {"POST", "PUT", "PATCH", "DELETE"}:
        return False
    if request.path == "/api/billing/stripe/webhook":
        return False
    return request.path.startswith("/api/")


def _captcha_token_from_request() -> str:
    header_token = (request.headers.get("X-CAPTCHA-Token") or "").strip()
    if header_token:
        return header_token
    payload = request.get_json(silent=True)
    if isinstance(payload, dict):
        for key in ("captcha_token", "captchaToken", "captcha-response", "captchaResponse"):
            candidate = payload.get(key)
            if isinstance(candidate, str) and candidate.strip():
                return candidate.strip()
    form_token = (request.form.get("captcha_token") or "").strip()
    if form_token:
        return form_token
    return ""


def verify_captcha_token(token: str) -> tuple[bool, str]:
    if not CAPTCHA_SECRET_KEY:
        return True, "skipped_not_configured"
    if not token:
        return False, "missing_token"
    try:
        response = requests.post(
            CAPTCHA_VERIFY_URL,
            data={
                "secret": CAPTCHA_SECRET_KEY,
                "response": token,
                "remoteip": request.remote_addr,
            },
            timeout=CAPTCHA_VERIFY_TIMEOUT_SECONDS,
        )
    except requests.RequestException:
        return False, "provider_request_failed"
    if response.status_code != 200:
        return False, "provider_http_error"
    try:
        payload = response.json()
    except ValueError:
        return False, "provider_invalid_json"
    success = bool(payload.get("success"))
    if success:
        return True, "ok"
    return False, "provider_rejected"


@app.before_request
def require_api_auth() -> tuple[dict[str, str], int] | None:
    incoming_correlation_id = (request.headers.get("X-Correlation-ID") or "").strip()
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
            _set_auth_context(DEFAULT_USER, get_workspace(1))
        else:
            return jsonify({"error": "Unauthorized"}), 401

    if _is_captcha_protected_request():
        captcha_token = _captcha_token_from_request()
        is_valid, failure_reason = verify_captcha_token(captcha_token)
        if not is_valid:
            log(
                "captcha_verification_failed",
                level="warning",
                workspace_id=getattr(g, "workspace_id", None),
            )
            return (
                jsonify({"error": "CAPTCHA verification failed", "reason": failure_reason}),
                403,
            )
    if request.method in {"POST", "PATCH", "PUT", "DELETE"}:
        captcha_token = _extract_captcha_token()
        captcha_ok, reason = verify_captcha_token(captcha_token or "")
        if not captcha_ok:
            log(
                f"Blocked request due to failed CAPTCHA verification ({reason})",
                level="warning",
            )
            return jsonify({"error": "CAPTCHA verification failed", "reason": reason}), 400
    return None


@app.after_request
def add_correlation_id_header(response):
    correlation_id = getattr(g, "correlation_id", None)
    if correlation_id:
        response.headers["X-Correlation-ID"] = correlation_id
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


def extract_price_cents(text: str) -> int | None:
    matches = re.findall(r"\$\s*(\d{1,4}(?:\.\d{2})?)", text)
    if not matches:
        return None
    values = []
    for m in matches:
        try:
            v = float(m)
            if 1.0 <= v <= 2000.0:
                values.append(int(round(v * 100)))
        except ValueError:
            continue
    return min(values) if values else None


def _parse_common_title_and_text(html: str) -> tuple[str, str]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else "Product"
    text = re.sub(r"<[^>]+>", " ", html).lower()
    return title[:180], text


def default_parser(html: str, keyword: str | None = None) -> MonitorResult:
    title, text = _parse_common_title_and_text(html)

    out_markers = [
        "out of stock",
        "sold out",
        "unavailable",
        "not available",
        "coming soon",
        "temporarily out of stock",
    ]
    in_markers = [
        "in stock",
        "add to cart",
        "buy now",
        "pickup",
        "ship it",
    ]

    has_out = any(m in text for m in out_markers)
    has_in = any(m in text for m in in_markers)

    in_stock = has_in and not has_out
    availability_reason = "fallback_unknown"
    parser_confidence = 0.2
    if has_out and not has_in:
        availability_reason = "marker_out_of_stock"
        parser_confidence = 0.9
    elif has_in and not has_out:
        availability_reason = "marker_in_stock"
        parser_confidence = 0.9
    elif has_in and has_out:
        availability_reason = "marker_conflict"
        parser_confidence = 0.35
    keyword_matched: bool | None = None
    if keyword:
        keyword_matched = keyword.lower() in text
    price_cents = extract_price_cents(re.sub(r"<[^>]+>", " ", html))
    status_text = "in_stock" if in_stock else "out_or_unknown"
    return MonitorResult(
        in_stock=in_stock,
        price_cents=price_cents,
        title=title[:180],
        status_text=status_text,
        availability_reason=availability_reason,
        parser_confidence=parser_confidence,
        keyword_matched=keyword_matched,
    )


def pokemoncenter_parser(html: str, keyword: str | None = None) -> MonitorResult:
    title, text = _parse_common_title_and_text(html)
    result = default_parser(html, keyword=keyword)
    out_markers = ["notify me when available", "currently unavailable"]
    in_markers = ["add to bag"]
    has_out = any(m in text for m in out_markers)
    has_in = any(m in text for m in in_markers)
    if has_out:
        result.in_stock = False
        result.status_text = "out_or_unknown"
        result.availability_reason = "pokemoncenter_marker_out_of_stock"
        result.parser_confidence = 0.98
    elif has_in:
        result.in_stock = True
        result.status_text = "in_stock"
        result.availability_reason = "pokemoncenter_marker_in_stock"
        result.parser_confidence = 0.98
    result.title = title
    return result


def walmart_parser(html: str, keyword: str | None = None) -> MonitorResult:
    title, text = _parse_common_title_and_text(html)
    result = default_parser(html, keyword=keyword)
    if '"availability":"instock"' in text or "fulfillmentoptions" in text:
        result.in_stock = True
        result.status_text = "in_stock"
        result.availability_reason = "walmart_marker_in_stock"
        result.parser_confidence = 0.98
    if '"availability":"outofstock"' in text or "out of stock" in text:
        result.in_stock = False
        result.status_text = "out_or_unknown"
        result.availability_reason = "walmart_marker_out_of_stock"
        result.parser_confidence = 0.98
    result.price_cents = extract_price_cents(html)
    result.title = title
    return result


def target_parser(html: str, keyword: str | None = None) -> MonitorResult:
    title, text = _parse_common_title_and_text(html)
    result = default_parser(html, keyword=keyword)

    in_markers = [
        '"availability":"instock"',
        '"availability":"in_stock"',
        "add to cart",
        "ship it",
        "pick up",
    ]
    out_markers = [
        '"availability":"outofstock"',
        '"availability":"out_of_stock"',
        "out of stock",
        "sold out",
        "unavailable",
    ]
    has_in = any(marker in text for marker in in_markers)
    has_out = any(marker in text for marker in out_markers)

    if has_out:
        result.in_stock = False
        result.status_text = "out_or_unknown"
        result.availability_reason = "target_marker_out_of_stock"
        result.parser_confidence = 0.98
    elif has_in:
        result.in_stock = True
        result.status_text = "in_stock"
        result.availability_reason = "target_marker_in_stock"
        result.parser_confidence = 0.98

    result.price_cents = extract_price_cents(html)
    result.title = title
    return result


def bestbuy_parser(html: str, keyword: str | None = None) -> MonitorResult:
    title, text = _parse_common_title_and_text(html)
    result = default_parser(html, keyword=keyword)

    in_markers = [
        '"buttonstate":"add to cart"',
        '"shipping":"available"',
        "ready for pickup today",
    ]
    out_markers = [
        '"buttonstate":"sold out"',
        '"buttonstate":"coming soon"',
        '"shipping":"unavailable"',
        "sold out",
        "coming soon",
    ]
    has_in = any(marker in text for marker in in_markers)
    has_out = any(marker in text for marker in out_markers)

    if has_out:
        result.in_stock = False
        result.status_text = "out_or_unknown"
        result.availability_reason = "bestbuy_marker_out_of_stock"
        result.parser_confidence = 0.98
    elif has_in:
        result.in_stock = True
        result.status_text = "in_stock"
        result.availability_reason = "bestbuy_marker_in_stock"
        result.parser_confidence = 0.98

    result.price_cents = extract_price_cents(html)
    result.title = title
    return result


def get_adapter_for_retailer(retailer: str | None):
    return resolve_retailer_adapter(retailer)


def evaluate_page(
    html: str, keyword: str | None = None, retailer: str | None = None
) -> MonitorResult:
    adapter = get_adapter_for_retailer(retailer)
    return run_retailer_flow(adapter, {"html": html, "keyword": keyword})


def fetch_monitor(monitor: sqlite3.Row) -> MonitorResult:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; StockSentinel/1.0; +https://example.com)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    conn = db()
    workspace = conn.execute("select proxy_url from workspaces where id = ?", (monitor["workspace_id"],)).fetchone()
    allocator = ProxyAllocator(conn)
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
    try:
        req = perform_request(
            task_key=task_key,
            method="GET",
            url=monitor["product_url"],
            workspace_id=monitor["workspace_id"],
            proxy_url=proxy_url,
            timeout=15,
            retry_total=2,
            backoff_factor=0.35,
            proxy_lease=lease,
            headers=headers,
        )
        if req.error:
            raise req.error
        assert req.response is not None
        r = req.response
        r.raise_for_status()
        keyword = (monitor["keyword"] or "").strip() or None
        return evaluate_page(r.text, keyword=keyword, retailer=monitor["retailer"])
    finally:
        if lease:
            allocator.release_lease(lease_id=lease.lease_id)
            conn.commit()
        conn.close()


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
            req = perform_request(
                task_key=f"webhook-{hook['id']}",
                method="POST",
                url=hook["webhook_url"],
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
            webhook_target = resolve_webhook_url(conn, hook)
            resp = requests.post(webhook_target, json=payload, timeout=8)
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
    valid_states = CHECKOUT_TASK_STATES if allow_control_states else CHECKOUT_TASK_STATES - {"paused", "stopped"}
    if state not in valid_states:
        raise ValueError(
            "Invalid checkout state. Expected one of: queued, monitoring, carting, shipping, payment, submitting, success, failed"
        )
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
    conn.execute(
        """
        insert into checkout_attempts(task_id, workspace_id, monitor_id, state, status, details, error_text, created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            task_id,
            workspace_id,
            monitor_id,
            state,
            status,
            json.dumps(details or {}),
            (error_text or "")[:500] if error_text else None,
            utc_now(),
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


def create_checkout_task(
    conn: sqlite3.Connection,
    *,
    workspace_id: int,
    monitor_id: int,
    task_name: str | None = None,
    task_config: dict[str, Any] | None = None,
    initial_state: str = "queued",
) -> sqlite3.Row:
    task_config = dict(task_config or {})
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
            created_at,
            updated_at,
            last_transition_at
        )
        values (?, ?, ?, ?, ?, 0, 0, ?, ?, ?)
        """,
        (workspace_id, monitor_id, task_name, json.dumps(task_config or {}), normalized_state, now_iso, now_iso, now_iso),
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
    if normalized_state == "monitoring" and not active_lease_key:
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
    if normalized_state in {"paused", "stopped", "success", "failed"} and active_lease_key:
        lease_row = conn.execute(
            "select id from proxy_leases where owner_type = 'checkout_task' and owner_id = ? and lease_key = ? and released_at is null",
            (task_id, active_lease_key),
        ).fetchone()
        if lease_row:
            allocator.release_lease(lease_id=lease_row["id"])
        active_lease_key = None
        active_proxy_id = None
    enabled = int(normalized_state not in {"stopped", "success", "failed"})
    is_paused = int(normalized_state == "paused")
    now_iso = utc_now()
    conn.execute(
        """
        update checkout_tasks
        set current_state = ?,
            enabled = ?,
            is_paused = ?,
            active_proxy_id = ?,
            active_proxy_lease_key = ?,
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
            (error_text or "")[:500] if error_text else None,
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


def enqueue_checkout_for_monitor(
    monitor: sqlite3.Row, result: MonitorResult, *, reason: str = "in_stock_detected"
) -> int | None:
    if not result.in_stock:
        return None
    conn = db()
    existing = conn.execute(
        """
        select * from checkout_tasks
        where workspace_id = ? and monitor_id = ?
          and current_state not in ('success', 'failed', 'stopped')
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

    task = create_checkout_task(
        conn,
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
        task_name=f"Monitor {monitor['id']} checkout",
        task_config={
            "retailer": monitor["retailer"],
            "product_url": monitor["product_url"],
            "proxy_policy": normalize_proxy_policy(
                {
                    "residential_only": bool(monitor["proxy_residential_only"]),
                    "region": monitor["proxy_region"],
                    "type": monitor["proxy_type"],
                    "sticky_session_seconds": monitor["proxy_sticky_session_seconds"],
                }
            ),
        },
        initial_state="queued",
    )
    record_checkout_attempt(
        conn,
        task_id=task["id"],
        workspace_id=monitor["workspace_id"],
        monitor_id=monitor["id"],
        state="queued",
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


def cents_to_dollars(cents: int | None) -> str:
    if cents is None:
        return "unknown"
    return f"${cents / 100:.2f}"


def serialize_checkout_task(row: sqlite3.Row | None) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    config_raw = payload.get("task_config")
    try:
        payload["task_config"] = json.loads(config_raw) if config_raw else {}
    except (TypeError, json.JSONDecodeError):
        payload["task_config"] = {}
    return payload


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


EMAIL_RE = re.compile(r"^[^@\s]+@[^@\s]+\.[^@\s]+$")
PHONE_RE = re.compile(r"^[0-9+\-().\s]{7,24}$")
SESSION_STATUSES = {"active", "expired", "locked", "logged_out"}


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

    if eligible:
        enqueue_checkout_for_monitor(monitor, result)
    create_event_and_deliver(monitor, result, eligible)


def run_monitor_pipeline_once(monitor: sqlite3.Row) -> dict[str, Any]:
    result = fetch_monitor(monitor)
    eligible = alert_eligibility(monitor, result)
    persist_monitor_state(monitor, result, eligible)
    emit_monitor_events(monitor, result, eligible)

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
        payload = json.dumps({"step_attempts": {}})
        self.conn.execute(
            """
            insert into jobs(job_type, monitor_id, status, attempt_count, next_run_at, payload_json, created_at, updated_at)
            values ('monitor_check', ?, 'queued', 0, ?, ?, ?, ?)
            """,
            (monitor["id"], now_iso, payload, now_iso, now_iso),
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
        emit_monitor_events(monitor, result, eligible)
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


def worker_loop() -> None:
    log(f"Worker loop started ({WORKER_ID})")
    while worker_running:
        with worker_lock:
            conn = db()
            now_iso = utc_now()
            queue = SQLiteJobQueue(conn, worker_id=WORKER_ID)
            apply_due_schedules(conn)
            monitors = conn.execute("select * from monitors where enabled = 1").fetchall()
            for monitor in monitors:
                queue.enqueue_monitor_check_if_due(monitor, now_iso=now_iso)
            job = queue.claim_due_job(now_iso=now_iso)
            if job:
                execute_monitor_job(queue, job, now_iso=now_iso)
            conn.commit()
            conn.close()
        if not job:
            time.sleep(WORKER_IDLE_SLEEP_SECONDS)
    log(f"Worker loop stopped ({WORKER_ID})")


def run_task_worker(task_id: int, workspace_id: int, stop_event: threading.Event) -> None:
    steps = ["session_init", "profile_submit", "account_submit", "payment_submit"]
    max_attempts = 4
    try:
        for attempt_number in range(1, max_attempts + 1):
            if stop_event.is_set():
                break

            conn = db()
            set_task_state(
                conn,
                task_id,
                state="running",
                retries=attempt_number - 1,
                last_step="session_init",
                last_error=None,
                started_at=utc_now(),
                stopped_at=None,
            )
            conn.commit()
            conn.close()
            emit_task_update(task_id)
            log(f"Task {task_id}: started attempt {attempt_number}", workspace_id=workspace_id)

            attempt_failed = False
            attempt_error = None
            for step in steps:
                if stop_event.is_set():
                    break
                conn = db()
                set_task_state(conn, task_id, state="running", retries=attempt_number - 1, last_step=step, last_error=None)
                conn.commit()
                conn.close()
                emit_task_update(task_id)
                time.sleep(TASK_STEP_DELAY_SECONDS)

            if stop_event.is_set():
                break

            if attempt_number < 3:
                attempt_failed = True
                attempt_error = f"Transient checkout failure at attempt {attempt_number}"

            conn = db()
            if attempt_failed:
                set_task_state(
                    conn,
                    task_id,
                    state="retrying",
                    retries=attempt_number,
                    last_step="retry_backoff",
                    last_error=attempt_error,
                )
                attempt_id = insert_task_attempt(
                    conn,
                    task_id=task_id,
                    workspace_id=workspace_id,
                    attempt_number=attempt_number,
                    state="failed",
                    step="payment_submit",
                    error=attempt_error,
                )
                conn.commit()
                conn.close()
                emit_task_attempt(task_id, attempt_id)
                emit_task_update(task_id)
                log(f"Task {task_id}: attempt {attempt_number} failed ({attempt_error})", level="warning", workspace_id=workspace_id)
                time.sleep(TASK_STEP_DELAY_SECONDS)
                continue

            set_task_state(
                conn,
                task_id,
                state="completed",
                retries=attempt_number - 1,
                last_step="completed",
                last_error=None,
            )
            attempt_id = insert_task_attempt(
                conn,
                task_id=task_id,
                workspace_id=workspace_id,
                attempt_number=attempt_number,
                state="success",
                step="completed",
                error=None,
            )
            conn.commit()
            conn.close()
            emit_task_attempt(task_id, attempt_id)
            emit_task_update(task_id)
            log(f"Task {task_id}: completed successfully on attempt {attempt_number}", workspace_id=workspace_id)
            return

        conn = db()
        stop_reason = "stopped" if stop_event.is_set() else "failed"
        stop_error = None if stop_event.is_set() else "Max retries exhausted"
        set_task_state(
            conn,
            task_id,
            state=stop_reason,
            last_step="stopped" if stop_event.is_set() else "failed",
            last_error=stop_error,
            stopped_at=utc_now() if stop_event.is_set() else None,
        )
        conn.commit()
        conn.close()
        emit_task_update(task_id)
        if stop_event.is_set():
            log(f"Task {task_id}: stopped", workspace_id=workspace_id)
        else:
            log(f"Task {task_id}: max retries exhausted", level="error", workspace_id=workspace_id)
    finally:
        with task_runtime_lock:
            task_threads.pop(task_id, None)
            task_stop_events.pop(task_id, None)


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
    return jsonify({"workspace": dict(row), "user": dict(g.current_user)})


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


@app.post("/api/tasks")
@require_auth
def api_create_task():
    body = request.json or {}
    try:
        retailer = canonical_retailer((body.get("retailer") or "").strip())
        product_url = (body.get("url") or body.get("product_url") or "").strip()
        profile = (body.get("profile") or "").strip()
        account = (body.get("account") or "").strip()
        payment = (body.get("payment") or "").strip()
        if retailer not in SUPPORTED_RETAILERS:
            raise ValueError(f"Unsupported retailer '{retailer}'")
        if not (product_url.startswith("http://") or product_url.startswith("https://")):
            raise ValueError("url must be http(s)")
        if not profile:
            raise ValueError("profile is required")
        if not account:
            raise ValueError("account is required")
        if not payment:
            raise ValueError("payment is required")
    except ValueError as exc:
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
    )
    monitor_id = int(monitor_cur.lastrowid)
    task = create_checkout_task(
        conn,
        workspace_id=workspace_id,
        monitor_id=monitor_id,
        task_name=f"{retailer} task",
        task_config={
            "retailer": retailer,
            "product_url": product_url,
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
        state = row["current_state"]
        compat_state = "idle" if state == "queued" and not row["enabled"] else ("running" if state == "monitoring" else state)
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


@app.get("/api/tasks/<int:task_id>/attempts")
@require_auth
def api_task_attempts(task_id: int):
    workspace_id = current_workspace_id()
    conn = db()
    task = get_checkout_task_for_workspace(conn, task_id, workspace_id)
    if not task:
        conn.close()
        return jsonify({"error": "Task not found"}), 404
    attempts = conn.execute(
        """
        select * from checkout_attempts
        where workspace_id = ? and task_id = ?
          and status != 'created'
        order by id desc
        """,
        (workspace_id, task_id),
    ).fetchall()
    conn.close()
    return jsonify([dict(row) for row in attempts])


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
        url = body["product_url"].strip()
        poll_interval = int(body.get("poll_interval_seconds", 20))
        keyword = (body.get("keyword") or "").strip() or None
        max_price_cents = body.get("max_price_cents")
        if max_price_cents is not None:
            max_price_cents = int(max_price_cents)
        msrp_cents = body.get("msrp_cents")
        if msrp_cents is not None:
            msrp_cents = int(msrp_cents)
        proxy_policy = normalize_proxy_policy(
            {
                "residential_only": body.get("proxy_residential_only", False),
                "region": body.get("proxy_region"),
                "type": body.get("proxy_type"),
                "sticky_session_seconds": body.get("proxy_sticky_session_seconds"),
            }
        )
        if retailer not in SUPPORTED_RETAILERS:
            raise ValueError(f"Unsupported retailer '{retailer}'")
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("product_url must be http(s)")

        enforce_plan_limits(current_workspace_id(), poll_interval)
    except (KeyError, ValueError) as exc:
        return jsonify({"error": str(exc)}), 400

    conn = db()
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, keyword, max_price_cents, msrp_cents, poll_interval_seconds,
            proxy_type, proxy_region, proxy_residential_only, proxy_sticky_session_seconds, created_at
        )
        values (?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            current_workspace_id(),
            retailer,
            url,
            keyword,
            max_price_cents,
            msrp_cents,
            poll_interval,
            proxy_policy.get("type"),
            proxy_policy.get("region"),
            int(proxy_policy.get("residential_only", False)),
            proxy_policy.get("sticky_session_seconds"),
            utc_now(),
        ),
    )
    conn.commit()
    monitor_id = cur.lastrowid
    row = conn.execute("select * from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    return jsonify(dict(row)), 201


@app.patch("/api/monitors/<int:monitor_id>")
@require_auth
def api_update_monitor(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    body = request.json or {}
    enabled = body.get("enabled")
    has_proxy_policy = any(
        k in body for k in ("proxy_residential_only", "proxy_region", "proxy_type", "proxy_sticky_session_seconds")
    )
    if enabled is None and not has_proxy_policy:
        return jsonify({"error": "enabled or proxy policy fields are required"}), 400
    proxy_policy = normalize_proxy_policy(
        {
            "residential_only": body.get("proxy_residential_only", False),
            "region": body.get("proxy_region"),
            "type": body.get("proxy_type"),
            "sticky_session_seconds": body.get("proxy_sticky_session_seconds"),
        }
    ) if has_proxy_policy else None

    conn = db()
    if enabled is not None:
        conn.execute("update monitors set enabled = ? where id = ? and workspace_id = ?", (int(bool(enabled)), monitor_id, workspace_id))
    if proxy_policy is not None:
        conn.execute(
            """
            update monitors
            set proxy_type = ?, proxy_region = ?, proxy_residential_only = ?, proxy_sticky_session_seconds = ?
            where id = ? and workspace_id = ?
            """,
            (
                proxy_policy.get("type"),
                proxy_policy.get("region"),
                int(proxy_policy.get("residential_only", False)),
                proxy_policy.get("sticky_session_seconds"),
                monitor_id,
                workspace_id,
            ),
        )
    conn.commit()
    row = conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (monitor_id, workspace_id),
    ).fetchone()
    if not row:
        conn.close()
        return jsonify({"error": "Monitor not found"}), 404
    conn.close()
    return jsonify(dict(row))


@app.delete("/api/monitors/<int:monitor_id>")
@require_auth
def api_delete_monitor(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    conn.execute(
        "delete from monitors where id = ? and workspace_id = ?",
        (monitor_id, current_workspace_id()),
    )
    conn.commit()
    conn.close()
    return jsonify({"ok": True})


@app.post("/api/monitors/<int:monitor_id>/check")
@require_auth
def api_check_monitor(monitor_id: int):
    workspace_id = get_workspace_id_for_request()
    conn = db()
    row = conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (monitor_id, current_workspace_id()),
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
    try:
        task = create_checkout_task(
            conn,
            workspace_id=workspace_id,
            monitor_id=int(monitor_id),
            task_name=(body.get("task_name") or "").strip() or None,
            task_config=body.get("task_config") if isinstance(body.get("task_config"), dict) else None,
            initial_state=initial_state,
        )
    except ValueError as exc:
        conn.close()
        return jsonify({"error": str(exc)}), 400

    conn.commit()
    conn.close()
    return jsonify(serialize_checkout_task(task)), 201


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
    return jsonify({"ok": True, "task": serialize_checkout_task(row)})


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
    return jsonify({"ok": True, "task": serialize_checkout_task(row)})


@app.get("/api/checkout/tasks/<int:task_id>/state")
@require_auth
def api_checkout_task_state(task_id: int):
    conn = db()
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

    conn = db()
    secret_id = create_secret(conn, current_workspace_id(), "webhook_url", url, int(g.current_user["id"]))
    cur = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, webhook_secret_id, notify_success, notify_failures, notify_restock_only, created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (
            current_workspace_id(),
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


@app.get("/api/webhooks")
@require_auth
def api_list_webhooks():
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    conn = db()
    rows = conn.execute(
        "select * from webhooks where workspace_id = ? order by id desc",
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([serialize_webhook(r) for r in rows])


@app.post("/api/webhooks/<int:webhook_id>/test")
@require_auth
def api_test_webhook(webhook_id: int):
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    conn = db()
    hook = conn.execute(
        "select * from webhooks where id = ? and workspace_id = ?",
        (webhook_id, current_workspace_id()),
    ).fetchone()
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
        req = perform_request(
            task_key=f"webhook-test-{webhook_id}",
            method="POST",
            url=hook["webhook_url"],
            workspace_id=current_workspace_id(),
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
        webhook_target = resolve_webhook_url(conn, hook)
        resp = requests.post(webhook_target, json=payload, timeout=8)
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
    conn = db()
    for key, value in fields:
        conn.execute(
            f"update webhooks set {key} = ? where id = ? and workspace_id = ?",
            (value, webhook_id, current_workspace_id()),
        )
    conn.commit()
    row = conn.execute(
        "select * from webhooks where id = ? and workspace_id = ?",
        (webhook_id, current_workspace_id()),
    ).fetchone()
    conn.close()
    if not row:
        return jsonify({"error": "Webhook not found"}), 404
    return jsonify(dict(row))


@app.delete("/api/webhooks/<int:webhook_id>")
@require_auth
def api_delete_webhook(webhook_id: int):
    role_error = ensure_workspace_role("owner", "admin")
    if role_error:
        return role_error
    conn = db()
    cur = conn.execute(
        "delete from webhooks where id = ? and workspace_id = ?",
        (webhook_id, current_workspace_id()),
    )
    conn.commit()
    conn.close()
    if cur.rowcount == 0:
        return jsonify({"error": "Webhook not found"}), 404
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
            workspace_id, retailer, username, email, encrypted_credential_ref, session_status, created_at, updated_at
        ) values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (current_workspace_id(), retailer, username, email, credential_ref, session_status, now_iso, now_iso),
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
        "Legal/ethical note: this project provides stock monitoring + alerts only. Auto-checkout is intentionally not implemented.",
        level="warning",
    )
    if APP_ROLE == "worker":
        worker_running = True
        worker_loop()
    elif APP_ROLE == "all":
        if ENABLE_EMBEDDED_WORKER:
            worker_running = True
            worker_thread = threading.Thread(target=worker_loop, daemon=True)
            worker_thread.start()
        socketio.run(app, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
    else:
        socketio.run(app, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
