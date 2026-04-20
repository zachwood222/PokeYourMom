from __future__ import annotations

import json
import os
import re
import secrets
import sqlite3
import threading
import time
import hashlib
import hmac
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Any, Callable
from uuid import uuid4

import requests
from flask import Flask, g, has_request_context, jsonify, render_template, request
from flask_socketio import SocketIO

app = Flask(__name__)
socketio = SocketIO(app, cors_allowed_origins="*", async_mode="threading")

DB_PATH = os.getenv("DB_PATH", "bot.db")
POLL_LOOP_SECONDS = int(os.getenv("POLL_LOOP_SECONDS", "15"))
DEFAULT_PLAN = os.getenv("DEFAULT_PLAN", "basic")
POKEMON_MSRP_BUFFER_CENTS = int(os.getenv("POKEMON_MSRP_BUFFER_CENTS", "1000"))
APP_VERSION = os.getenv("APP_VERSION", "0.1.0")
RELEASE_CHANNEL = os.getenv("RELEASE_CHANNEL", "stable")
API_AUTH_TOKEN = os.getenv("API_AUTH_TOKEN", "dev-token")
STRIPE_WEBHOOK_SECRET = os.getenv("STRIPE_WEBHOOK_SECRET", "")
UPDATE_CHECK_URL = os.getenv("UPDATE_CHECK_URL", "")
UPDATE_CHECK_TIMEOUT_SECONDS = float(os.getenv("UPDATE_CHECK_TIMEOUT_SECONDS", "2.0"))

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
RETAILER_ALIASES = {
    "pokemon-center": "pokemoncenter",
    "pokemon_center": "pokemoncenter",
    "pokemon center": "pokemoncenter",
    "pokemoncenter": "pokemoncenter",
}

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

running = False
monitor_thread: threading.Thread | None = None
lock = threading.Lock()

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
class MonitorResult:
    in_stock: bool
    price_cents: int | None
    title: str
    status_text: str
    availability_reason: str | None = None
    parser_confidence: float | None = None
    keyword_matched: bool | None = None
    price_within_limit: bool | None = None
    within_msrp_delta: bool | None = None


@dataclass(frozen=True)
class RetailerParser:
    name: str
    parse: Callable[[str, str | None], MonitorResult]

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


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
    entry = format_log_entry(
        level=level,
        message=message,
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
            created_at text not null,
            foreign key(workspace_id) references workspaces(id)
        );

        create table if not exists webhooks (
            id integer primary key autoincrement,
            workspace_id integer not null,
            name text not null,
            webhook_url text not null,
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
            foreign key(workspace_id) references workspaces(id)
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

        create table if not exists checkout_tasks (
            id integer primary key autoincrement,
            workspace_id integer not null,
            monitor_id integer not null,
            task_name text,
            task_config text,
            current_state text not null default 'queued',
            enabled integer not null default 0,
            is_paused integer not null default 0,
            last_error text,
            created_at text not null,
            updated_at text not null,
            last_transition_at text,
            foreign key(workspace_id) references workspaces(id),
            foreign key(monitor_id) references monitors(id)
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
    ensure_column(conn, "workspaces", "subscription_status", "text not null default 'inactive'")
    ensure_column(conn, "workspaces", "subscription_source", "text not null default 'manual'")
    ensure_column(conn, "workspaces", "subscription_updated_at", "text")
    conn.commit()
    conn.close()


def current_workspace_id() -> int:
    workspace_id = getattr(g, "workspace_id", None)
    if workspace_id is None:
        raise RuntimeError("Missing workspace context")
    return workspace_id


def get_workspace_for_user(user_id: int) -> sqlite3.Row | None:
    conn = db()
    row = conn.execute(
        """
        select w.* from workspace_members wm
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
    api_token = (request.headers.get("X-API-Token") or "").strip()
    if api_token:
        return api_token
    auth_header = (request.headers.get("Authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return None


def _set_auth_context(user: sqlite3.Row | dict[str, Any], workspace: sqlite3.Row) -> None:
    g.current_user = dict(user)
    g.workspace_id = int(workspace["id"])
    g.current_workspace = workspace


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
        return None

    token = _token_from_request()
    if token and token == API_AUTH_TOKEN:
        _set_auth_context(DEFAULT_USER, get_workspace(1))
        return None
    return jsonify({"error": "Unauthorized"}), 401


@app.after_request
def add_correlation_id_header(response):
    correlation_id = getattr(g, "correlation_id", None)
    if correlation_id:
        response.headers["X-Correlation-ID"] = correlation_id
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


def canonical_retailer(retailer: str) -> str:
    value = retailer.strip().lower()
    return RETAILER_ALIASES.get(value, value)


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


PARSERS: dict[str, RetailerParser] = {
    "walmart": RetailerParser(name="walmart", parse=walmart_parser),
    "target": RetailerParser(name="target", parse=target_parser),
    "bestbuy": RetailerParser(name="bestbuy", parse=bestbuy_parser),
    "pokemoncenter": RetailerParser(name="pokemoncenter", parse=pokemoncenter_parser),
}


def get_parser_for_retailer(retailer: str | None) -> RetailerParser:
    normalized = canonical_retailer(retailer) if retailer else ""
    return PARSERS.get(normalized, RetailerParser(name="default", parse=default_parser))


def evaluate_page(
    html: str, keyword: str | None = None, retailer: str | None = None
) -> MonitorResult:
    parser = get_parser_for_retailer(retailer)
    return parser.parse(html, keyword)


def fetch_monitor(monitor: sqlite3.Row) -> MonitorResult:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; StockSentinel/1.0; +https://example.com)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(monitor["product_url"], headers=headers, timeout=15)
    r.raise_for_status()
    keyword = (monitor["keyword"] or "").strip() or None
    return evaluate_page(r.text, keyword=keyword, retailer=monitor["retailer"])


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
            resp = requests.post(hook["webhook_url"], json=payload, timeout=8)
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
    enabled = int(normalized_state not in {"stopped", "success", "failed"})
    is_paused = int(normalized_state == "paused")
    now_iso = utc_now()
    conn.execute(
        """
        update checkout_tasks
        set current_state = ?,
            enabled = ?,
            is_paused = ?,
            last_error = ?,
            updated_at = ?,
            last_transition_at = ?
        where id = ? and workspace_id = ?
        """,
        (
            normalized_state,
            enabled,
            is_paused,
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
        task_config={"retailer": monitor["retailer"], "product_url": monitor["product_url"]},
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
    return dict(row)


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


def check_monitor_once(monitor: sqlite3.Row) -> dict[str, Any]:
    try:
        result = fetch_monitor(monitor)
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

    eligible = alert_eligibility(monitor, result)

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


def monitor_loop() -> None:
    log("Monitor loop started")
    while running:
        with lock:
            conn = db()
            apply_due_schedules(conn)
            monitors = conn.execute("select * from monitors where enabled = 1").fetchall()
            conn.close()

            now_ts = time.time()
            for m in monitors:
                if not running:
                    break
                if m["last_checked_at"]:
                    elapsed = now_ts - datetime.fromisoformat(m["last_checked_at"]).timestamp()
                    if elapsed < m["poll_interval_seconds"]:
                        continue
                check_monitor_once(m)

        time.sleep(POLL_LOOP_SECONDS)
    log("Monitor loop stopped")


@app.route("/")
def index():
    return render_template("index.html")


@app.get("/healthz")
def healthz():
    return jsonify({"ok": True, "running": running})


@app.get("/api/meta")
def api_meta():
    return jsonify(
        {
            "app_version": APP_VERSION,
            "release_channel": RELEASE_CHANNEL,
            "python_version": os.sys.version.split()[0],
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
        resp = requests.get(UPDATE_CHECK_URL, timeout=UPDATE_CHECK_TIMEOUT_SECONDS)
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
    owner_error = ensure_workspace_owner()
    if owner_error:
        return owner_error
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
            "running": running,
        }
    )


@app.get("/api/ops/metrics")
@require_auth
def api_ops_metrics():
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
        insert into monitors(workspace_id, retailer, product_url, keyword, max_price_cents, msrp_cents, poll_interval_seconds, created_at)
        values (?, ?, ?, ?, ?, ?, ?, ?)
        """,
        (current_workspace_id(), retailer, url, keyword, max_price_cents, msrp_cents, poll_interval, utc_now()),
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
    if enabled is None:
        return jsonify({"error": "enabled is required"}), 400
    conn = db()
    conn.execute(
        "update monitors set enabled = ? where id = ? and workspace_id = ?",
        (int(bool(enabled)), monitor_id, current_workspace_id()),
    )
    conn.commit()
    row = conn.execute(
        "select * from monitors where id = ? and workspace_id = ?",
        (monitor_id, current_workspace_id()),
    ).fetchone()
    conn.close()
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
    owner_error = ensure_workspace_owner()
    if owner_error:
        return owner_error
    body = request.json or {}
    name = (body.get("name") or "Discord").strip()
    url = (body.get("webhook_url") or "").strip()
    notify_success = int(bool(body.get("notify_success", True)))
    notify_failures = int(bool(body.get("notify_failures", False)))
    notify_restock_only = int(bool(body.get("notify_restock_only", True)))
    if not url.startswith("https://discord.com/api/webhooks/"):
        return jsonify({"error": "Invalid Discord webhook URL"}), 400

    conn = db()
    cur = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, notify_success, notify_failures, notify_restock_only, created_at)
        values (?, ?, ?, ?, ?, ?, ?)
        """,
        (current_workspace_id(), name, url, notify_success, notify_failures, notify_restock_only, utc_now()),
    )
    conn.commit()
    row = conn.execute("select * from webhooks where id = ?", (cur.lastrowid,)).fetchone()
    conn.close()
    return jsonify(serialize_webhook(row)), 201


@app.get("/api/webhooks")
@require_auth
def api_list_webhooks():
    owner_error = ensure_workspace_owner()
    if owner_error:
        return owner_error
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
    owner_error = ensure_workspace_owner()
    if owner_error:
        return owner_error
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
        resp = requests.post(hook["webhook_url"], json=payload, timeout=8)
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
    owner_error = ensure_workspace_owner()
    if owner_error:
        return owner_error
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
    owner_error = ensure_workspace_owner()
    if owner_error:
        return owner_error
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
    global running, monitor_thread
    if running:
        return jsonify({"ok": True, "running": True})
    running = True
    monitor_thread = threading.Thread(target=monitor_loop, daemon=True)
    monitor_thread.start()
    return jsonify({"ok": True, "running": True})


@app.post("/api/stop")
@require_auth
def api_stop():
    global running
    running = False
    return jsonify({"ok": True, "running": False})


if __name__ == "__main__":
    init_db()
    log(
        "Legal/ethical note: this project provides stock monitoring + alerts only. Auto-checkout is intentionally not implemented.",
        level="warning",
    )
    socketio.run(app, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
