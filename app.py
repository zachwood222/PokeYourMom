from __future__ import annotations

import json
import os
import re
import secrets
import sqlite3
import threading
import time
from dataclasses import dataclass
from datetime import datetime, timezone
from functools import wraps
from typing import Any

import requests
from flask import Flask, g, jsonify, render_template, request
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

PLAN_LIMITS = {
    "basic": {"max_monitors": 20, "min_poll_seconds": 20},
    "pro": {"max_monitors": 100, "min_poll_seconds": 10},
    "team": {"max_monitors": 500, "min_poll_seconds": 5},
}
SUPPORTED_RETAILERS = {"walmart", "target", "bestbuy"}

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


@dataclass
class MonitorResult:
    in_stock: bool
    price_cents: int | None
    title: str
    status_text: str
    keyword_matched: bool | None = None
    price_within_limit: bool | None = None
    within_msrp_delta: bool | None = None

def utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()

def log(message: str) -> None:
    entry = f"[{datetime.now(timezone.utc).strftime('%Y-%m-%d %H:%M:%S UTC')}] {message}"
    print(entry)
    socketio.emit("log", {"message": entry})


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
        user = resolve_user_from_request()
        if not user:
            return jsonify({"error": "Unauthorized"}), 401
        workspace = get_workspace_for_user(user["id"])
        if not workspace:
            return jsonify({"error": "No workspace membership found"}), 403
        g.current_user = user
        g.workspace_id = workspace["id"]
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


def _token_from_request() -> str | None:
    auth_header = (request.headers.get("Authorization") or "").strip()
    if auth_header.lower().startswith("bearer "):
        return auth_header[7:].strip()
    return (request.headers.get("X-API-Token") or "").strip() or None


@app.before_request
def require_api_auth() -> tuple[dict[str, str], int] | None:
    if not request.path.startswith("/api/"):
        return None
    token = _token_from_request()
    if not token or token != API_AUTH_TOKEN:
        return jsonify({"error": "Unauthorized"}), 401
    g.current_user = dict(DEFAULT_USER)
    g.current_workspace = get_workspace(1)
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


def evaluate_page(html: str, keyword: str | None = None) -> MonitorResult:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else "Product"
    text = re.sub(r"<[^>]+>", " ", html).lower()

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
        keyword_matched=keyword_matched,
    )


def fetch_monitor(monitor: sqlite3.Row) -> MonitorResult:
    headers = {
        "User-Agent": "Mozilla/5.0 (compatible; StockSentinel/1.0; +https://example.com)",
        "Accept-Language": "en-US,en;q=0.9",
    }
    r = requests.get(monitor["product_url"], headers=headers, timeout=15)
    r.raise_for_status()
    keyword = (monitor["keyword"] or "").strip() or None
    return evaluate_page(r.text, keyword=keyword)


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
    log(f"🚨 In-stock event emitted for monitor {monitor['id']} ({monitor['retailer']})")


def cents_to_dollars(cents: int | None) -> str:
    if cents is None:
        return "unknown"
    return f"${cents / 100:.2f}"


def check_monitor_once(monitor: sqlite3.Row) -> dict[str, Any]:
    try:
        result = fetch_monitor(monitor)
    except Exception as exc:  # noqa: BLE001
        conn = db()
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
        log(f"⚠️ Monitor {monitor['id']} fetch failed: {exc}")
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

    create_event_and_deliver(monitor, result, eligible)

    log(
        f"✅ Checked monitor {monitor['id']} | {monitor['retailer']} | {result.status_text} | {cents_to_dollars(result.price_cents)}"
    )
    return {
        "ok": True,
        "in_stock": result.in_stock,
        "eligible_for_alert": eligible,
        "price_cents": result.price_cents,
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
            f"🗓️ Applied schedule {row['id']} for monitor {row['monitor_id']} (poll={row['new_poll_interval_seconds']}s)"
        )


def monitor_loop() -> None:
    log("▶️ Monitor loop started")
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
    log("⏹️ Monitor loop stopped")


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


@app.get("/api/meta/check-update")
def api_meta_check_update():
    latest = APP_VERSION
    return jsonify(
        {
            "ok": True,
            "current_version": APP_VERSION,
            "latest_version": latest,
            "update_available": latest != APP_VERSION,
            "release_channel": RELEASE_CHANNEL,
        }
    )


@app.get("/api/workspace")
@require_auth
def api_workspace():
    row = get_workspace(current_workspace_id())
    return jsonify(dict(row))


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


@app.get("/api/monitors")
@require_auth
def api_list_monitors():
    workspace_id = get_workspace_id_for_request()
    conn = db()
    rows = conn.execute(
        "select * from monitors where workspace_id = ? order by id desc", (workspace_id,)
        "select * from monitors where workspace_id = ? order by id desc",
        (current_workspace_id(),),
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
    workspace_id = current_workspace_id()
    total_monitors = conn.execute(
        "select count(*) as c from monitors where workspace_id = ?",
        (workspace_id,),
    ).fetchone()["c"]
    enabled_monitors = conn.execute(
        "select count(*) as c from monitors where enabled = 1 and workspace_id = ?",
        (workspace_id,),
    ).fetchone()["c"]
    checks_last_24h = conn.execute(
        """
        select count(*) as c from monitors
        where workspace_id = ?
          and last_checked_at is not null
          and datetime(last_checked_at) >= datetime('now', '-1 day')
        """,
        """
        ,
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
        where m.workspace_id = ? and datetime(event_time) >= datetime('now', '-1 day')
        where m.workspace_id = ?
          and datetime(e.event_time) >= datetime('now', '-1 day')
        """,
        (workspace_id,),
    ).fetchone()["c"]
    events_7d = conn.execute(
        """
        select count(*) as c from events e
        join monitors m on m.id = e.monitor_id
        where m.workspace_id = ? and datetime(event_time) >= datetime('now', '-7 day')
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


@app.post("/api/monitors")
@require_auth
def api_create_monitor():
    body = request.json or {}
    try:
        retailer = body["retailer"].strip().lower()
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

        if retailer not in SUPPORTED_RETAILERS:
            raise ValueError(f"Unsupported retailer: {retailer}")
        if not (url.startswith("http://") or url.startswith("https://")):
            raise ValueError("product_url must start with http:// or https://")

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
        return jsonify({"error": "Monitor not found"}), 404
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
        (current_workspace_id(),),
    ).fetchall()
    conn.close()
    return jsonify([dict(r) for r in rows])


@app.post("/api/webhooks")
@require_auth
def api_add_webhook():
    workspace_id = get_workspace_id_for_request()
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
    workspace_id = get_workspace_id_for_request()
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
    workspace_id = get_workspace_id_for_request()
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
    workspace_id = get_workspace_id_for_request()
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
    workspace_id = get_workspace_id_for_request()
    conn = db()
    conn.execute(
        "delete from webhooks where id = ? and workspace_id = ?",
        (webhook_id, current_workspace_id()),
    )
    conn.commit()
    conn.close()
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
    log("⚠️ Legal/ethical note: this project provides stock monitoring + alerts only. Auto-checkout is intentionally not implemented.")
    socketio.run(app, host="0.0.0.0", port=5000, allow_unsafe_werkzeug=True)
