import importlib
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_app(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "test-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    return reloaded


def _auth_headers():
    return {"Authorization": "Bearer test-token"}


def test_protected_endpoint_requires_auth(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/monitors")

    assert resp.status_code == 401
    assert "Unauthorized" in resp.get_json()["error"]


def test_create_monitor_validates_retailer(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={"retailer": "amazon", "product_url": "https://example.com", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    )

    assert resp.status_code == 400
    assert "Unsupported retailer" in resp.get_json()["error"]


def test_create_monitor_requires_http_url(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={"retailer": "walmart", "product_url": "ftp://example.com", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    )

    assert resp.status_code == 400
    assert "product_url" in resp.get_json()["error"]


def test_authenticated_user_only_sees_own_workspace_data(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/hidden', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/monitors", headers=_auth_headers())

    assert resp.status_code == 200
    assert resp.get_json() == []


def test_keyword_and_max_price_filter_block_event(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    posted_payloads = []

    class DummyResponse:
        status_code = 204
        text = ""

    def fake_post(url, json, timeout):
        posted_payloads.append((url, json, timeout))
        return DummyResponse()

    monkeypatch.setattr(app_module.requests, "post", fake_post)

    conn = app_module.db()
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, keyword, max_price_cents, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/p', 'pokemon', 3000, 20, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor = conn.execute("select * from monitors order by id desc limit 1").fetchone()
    conn.execute(
        "insert into webhooks(workspace_id, name, webhook_url, created_at) values (1, 'Main', 'https://discord.com/api/webhooks/test', ?)",
        (app_module.utc_now(),),
    )
    conn.commit()
    conn.close()

    result_keyword_miss = app_module.MonitorResult(
        in_stock=True,
        price_cents=2500,
        title="Sports Card Bundle",
        status_text="in_stock",
        keyword_matched=False,
    )
    eligible_keyword_miss = app_module.alert_eligibility(monitor, result_keyword_miss)
    app_module.create_event_and_deliver(monitor, result_keyword_miss, eligible_keyword_miss)

    result_price_too_high = app_module.MonitorResult(
        in_stock=True,
        price_cents=3500,
        title="Pokemon 151 Box",
        status_text="in_stock",
        keyword_matched=True,
    )
    eligible_price_too_high = app_module.alert_eligibility(monitor, result_price_too_high)
    app_module.create_event_and_deliver(monitor, result_price_too_high, eligible_price_too_high)

    conn = app_module.db()
    event_count = conn.execute("select count(*) as c from events").fetchone()["c"]
    conn.close()

    assert event_count == 0
    assert posted_payloads == []


def test_evaluate_page_sets_keyword_and_price_fields(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    html = """
    <html>
      <head><title>Pokemon 151 Booster Bundle</title></head>
      <body>
        <button>Add to Cart</button>
        <p>Now only $29.99</p>
      </body>
    </html>
    """
    result = app_module.evaluate_page(html, keyword="pokemon")

    assert result.in_stock is True
    assert result.keyword_matched is True
    assert result.price_cents == 2999
    assert result.status_text == "in_stock"


def test_init_db_migrates_existing_monitors_table_with_msrp_column(tmp_path, monkeypatch):
    db_path = tmp_path / "legacy.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    conn = sqlite3.connect(db_path)
    conn.execute(
        """
        create table monitors (
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
            created_at text not null
        )
        """
    )
    conn.commit()
    conn.close()

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    columns = {row[1] for row in conn.execute("pragma table_info(monitors)").fetchall()}
    conn.close()

    assert "msrp_cents" in columns


def test_api_routes_require_auth(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/monitors")

    assert resp.status_code == 401
    assert resp.get_json() == {"error": "Unauthorized"}


def test_api_routes_allow_authenticated_requests_and_include_context(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/workspace", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["workspace"]["id"] == 1
    assert payload["user"]["email"] == "owner@local.test"


def test_events_are_workspace_scoped_no_cross_tenant_leakage(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    now_iso = app_module.utc_now()
    conn.execute("insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)", (now_iso,))
    other_workspace_id = conn.execute(
        "select id from workspaces where name = 'Other' order by id desc limit 1"
    ).fetchone()["id"]
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'target', 'https://example.com/owned', 20, ?)
        """,
        (now_iso,),
    )
    own_monitor_id = conn.execute("select id from monitors where workspace_id = 1").fetchone()["id"]
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'walmart', 'https://example.com/other', 20, ?)
        """,
        (other_workspace_id, now_iso),
    )
    other_monitor_id = conn.execute(
        "select id from monitors where workspace_id = ?",
        (other_workspace_id,),
    ).fetchone()["id"]
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values (?, 'in_stock', 'owned event', 'https://example.com/owned', 'target', 1999, ?, 'owned-key')
        """,
        (own_monitor_id, now_iso),
    )
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values (?, 'in_stock', 'other event', 'https://example.com/other', 'walmart', 2999, ?, 'other-key')
        """,
        (other_monitor_id, now_iso),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["title"] == "owned event"


def test_structured_log_formatter_shape_and_socket_payload(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    emitted = []

    monkeypatch.setattr(app_module.socketio, "emit", lambda event, payload: emitted.append((event, payload)))

    entry = app_module.format_log_entry(
        level="INFO",
        message="hello",
        workspace_id=123,
        monitor_id=456,
        correlation_id="corr-1",
    )

    assert set(entry.keys()) == {
        "timestamp",
        "level",
        "message",
        "workspace_id",
        "monitor_id",
        "correlation_id",
    }
    assert entry["level"] == "info"
    assert entry["workspace_id"] == 123
    assert entry["monitor_id"] == 456
    assert entry["correlation_id"] == "corr-1"

    app_module.log("test message", workspace_id=123, monitor_id=456, correlation_id="corr-1")
    assert emitted
    event_name, payload = emitted[-1]
    assert event_name == "log"
    assert payload["message"] == "test message"
    assert payload["workspace_id"] == 123
    assert payload["monitor_id"] == 456
    assert payload["correlation_id"] == "corr-1"


def test_correlation_id_header_present_and_consistent(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    emitted = []

    monkeypatch.setattr(app_module.socketio, "emit", lambda event, payload: emitted.append((event, payload)))
    correlation_id = "corr-test-123"
    resp = client.get("/api/workspace", headers={**_auth_headers(), "X-Correlation-ID": correlation_id})

    assert resp.status_code == 200
    assert resp.headers["X-Correlation-ID"] == correlation_id

    with app_module.app.test_request_context("/api/workspace"):
        app_module.g.correlation_id = correlation_id
        app_module.log("inside request")
    assert emitted[-1][1]["correlation_id"] == correlation_id


def test_ops_metrics_endpoint_schema_and_non_negative_values(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    now_iso = app_module.utc_now()
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, last_checked_at, failure_streak, created_at)
        values (1, 'target', 'https://example.com/1', 20, ?, 1, ?)
        """,
        (now_iso, now_iso),
    )
    monitor_id = conn.execute("select id from monitors order by id desc limit 1").fetchone()["id"]
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values (?, 'in_stock', 'event', 'https://example.com/1', 'target', 1499, ?, 'metrics-event-key')
        """,
        (monitor_id, now_iso),
    )
    event_id = conn.execute("select id from events where dedupe_key = 'metrics-event-key'").fetchone()["id"]
    conn.execute(
        """
        insert into deliveries(event_id, webhook_id, status, delivered_at)
        values (?, 1, 'sent', ?), (?, 1, 'failed', ?)
        """,
        (event_id, now_iso, event_id, now_iso),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/ops/metrics", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    expected_keys = {
        "checks_total",
        "checks_failed_total",
        "alerts_created_total",
        "webhook_sent_total",
        "webhook_failed_total",
    }
    assert set(payload.keys()) == expected_keys
    for key in expected_keys:
        assert isinstance(payload[key], int)
        assert payload[key] >= 0
