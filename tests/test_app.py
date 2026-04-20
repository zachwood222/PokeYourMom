import importlib
import sqlite3
import sys
from datetime import datetime, timedelta, timezone
from pathlib import Path

from parser_fixture_harness import load_fixture_html

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_app(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "test-token")
    monkeypatch.setenv("API_AUTH_TOKEN", "test-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    return reloaded


def _auth_headers(token="test-token"):
    return {"Authorization": f"Bearer {token}", "X-API-Token": "api-token"}


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


def test_create_monitor_accepts_pokemon_center_alias(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "pokemon-center",
            "product_url": "https://www.pokemoncenter.com/product/123",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    payload = resp.get_json()

    assert resp.status_code == 201
    assert payload["retailer"] == "pokemoncenter"


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


def test_evaluate_page_handles_pokemon_center_markers(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    html = """
    <html>
      <head><title>Pokemon Center Box</title></head>
      <body>
        <button>Notify Me When Available</button>
        <p>$39.99</p>
      </body>
    </html>
    """
    result = app_module.evaluate_page(html, keyword="pokemon", retailer="pokemon-center")

    assert result.in_stock is False
    assert result.keyword_matched is True
    assert result.price_cents == 3999
    assert result.status_text == "out_or_unknown"


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
    assert payload["user"]["id"] == 1


def test_monitor_failure_trends_returns_seeded_counts(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    now = datetime.now(timezone.utc)

    conn = app_module.db()
    cur = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/one', 20, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = cur.lastrowid
    conn.execute(
        """
        insert into monitor_failures(monitor_id, workspace_id, error_text, failed_at)
        values (?, 1, 'err-1', ?), (?, 1, 'err-2', ?), (?, 1, 'err-3', ?)
        """,
        (
            monitor_id,
            (now - timedelta(hours=2)).isoformat(),
            monitor_id,
            (now - timedelta(days=3)).isoformat(),
            monitor_id,
            (now - timedelta(days=8)).isoformat(),
        ),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/ops/monitor-failure-trends", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["trends"] == [
        {"monitor_id": monitor_id, "failures_last_24h": 1, "failures_last_7d": 2}
    ]


def test_webhook_health_trends_scoped_to_workspace(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    now = datetime.now(timezone.utc)

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]

    conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, fail_streak, last_status_code, created_at)
        values (1, 'Main', 'https://discord.com/api/webhooks/main', 2, 500, ?)
        """,
        (app_module.utc_now(),),
    )
    webhook_id = conn.execute("select id from webhooks where name = 'Main'").fetchone()["id"]
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/seed', 20, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = conn.execute("select id from monitors where product_url = 'https://example.com/seed'").fetchone()["id"]
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values (?, 'in_stock', 'seed', 'https://example.com/seed', 'walmart', 1000, ?, 'seed-key-1')
        """,
        (monitor_id, app_module.utc_now()),
    )
    event_id = conn.execute("select id from events where dedupe_key = 'seed-key-1'").fetchone()["id"]
    conn.execute(
        """
        insert into deliveries(event_id, webhook_id, status, response_code, response_body, delivered_at)
        values (?, ?, 'failed', 500, 'oops', ?), (?, ?, 'failed', 500, 'oops', ?)
        """,
        (
            event_id,
            webhook_id,
            (now - timedelta(hours=4)).isoformat(),
            event_id,
            webhook_id,
            (now - timedelta(days=9)).isoformat(),
        ),
    )

    conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, fail_streak, last_status_code, created_at)
        values (?, 'OtherHook', 'https://discord.com/api/webhooks/other', 5, 429, ?)
        """,
        (other_workspace, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/ops/webhook-health-trends", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload["webhooks"]) == 1
    assert payload["webhooks"][0]["webhook_id"] == webhook_id
    assert payload["webhooks"][0]["fail_streak"] == 2
    assert payload["webhooks"][0]["last_status_code"] == 500
    assert payload["webhooks"][0]["recent_failures_24h"] == 1
    assert payload["webhooks"][0]["recent_failures_7d"] == 1


def test_parser_dispatch_uses_walmart_and_fallback(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    walmart_parser = app_module.get_parser_for_retailer("walmart")
    target_parser = app_module.get_parser_for_retailer("target")
    bestbuy_parser = app_module.get_parser_for_retailer("bestbuy")
    fallback_parser = app_module.get_parser_for_retailer("unknown-retailer")

    assert walmart_parser.name == "walmart"
    assert target_parser.name == "target"
    assert bestbuy_parser.name == "bestbuy"
    assert fallback_parser.name == "default"


def test_walmart_parser_extracts_in_stock_and_out_of_stock(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    in_stock_html = load_fixture_html("walmart", "in_stock")
    out_stock_html = load_fixture_html("walmart", "out_of_stock")

    in_stock = app_module.evaluate_page(in_stock_html, retailer="walmart")
    out_stock = app_module.evaluate_page(out_stock_html, retailer="walmart")

    assert in_stock.in_stock is True
    assert in_stock.price_cents == 2488
    assert in_stock.availability_reason == "walmart_marker_in_stock"
    assert in_stock.parser_confidence == 0.98
    assert out_stock.in_stock is False
    assert out_stock.price_cents == 2488
    assert out_stock.availability_reason == "walmart_marker_out_of_stock"
    assert out_stock.parser_confidence == 0.98


def test_target_parser_extracts_in_stock_and_out_of_stock(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    in_stock_html = load_fixture_html("target", "in_stock")
    out_stock_html = load_fixture_html("target", "out_of_stock")

    in_stock = app_module.evaluate_page(in_stock_html, retailer="target")
    out_stock = app_module.evaluate_page(out_stock_html, retailer="target")

    assert in_stock.in_stock is True
    assert in_stock.price_cents == 1999
    assert in_stock.availability_reason == "target_marker_in_stock"
    assert in_stock.parser_confidence == 0.98
    assert out_stock.in_stock is False
    assert out_stock.price_cents == 1999
    assert out_stock.availability_reason == "target_marker_out_of_stock"
    assert out_stock.parser_confidence == 0.98


def test_bestbuy_parser_extracts_in_stock_and_out_of_stock(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    in_stock_html = load_fixture_html("bestbuy", "in_stock")
    out_stock_html = load_fixture_html("bestbuy", "out_of_stock")

    in_stock = app_module.evaluate_page(in_stock_html, retailer="bestbuy")
    out_stock = app_module.evaluate_page(out_stock_html, retailer="bestbuy")

    assert in_stock.in_stock is True
    assert in_stock.price_cents == 5499
    assert in_stock.availability_reason == "bestbuy_marker_in_stock"
    assert in_stock.parser_confidence == 0.98
    assert out_stock.in_stock is False
    assert out_stock.price_cents == 5499
    assert out_stock.availability_reason == "bestbuy_marker_out_of_stock"
    assert out_stock.parser_confidence == 0.98


def test_target_and_bestbuy_parsers_keep_default_fallback_for_unknown_markup(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    target_unknown_html = load_fixture_html("target", "ambiguous")
    bestbuy_unknown_html = load_fixture_html("bestbuy", "ambiguous")

    target_result = app_module.evaluate_page(target_unknown_html, retailer="target")
    target_default = app_module.default_parser(target_unknown_html)
    bestbuy_result = app_module.evaluate_page(bestbuy_unknown_html, retailer="bestbuy")
    bestbuy_default = app_module.default_parser(bestbuy_unknown_html)

    assert target_result.in_stock == target_default.in_stock
    assert target_result.status_text == target_default.status_text
    assert target_result.price_cents == target_default.price_cents
    assert target_result.availability_reason == target_default.availability_reason
    assert target_result.parser_confidence == target_default.parser_confidence
    assert bestbuy_result.in_stock == bestbuy_default.in_stock
    assert bestbuy_result.status_text == bestbuy_default.status_text
    assert bestbuy_result.price_cents == bestbuy_default.price_cents
    assert bestbuy_result.availability_reason == bestbuy_default.availability_reason
    assert bestbuy_result.parser_confidence == bestbuy_default.parser_confidence


def _seed_monitor(app_module):
    conn = app_module.db()
    cur = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'target', 'https://example.com/item', 20, ?)
        """,
        (app_module.utc_now(),),
    )
    conn.commit()
    monitor_id = cur.lastrowid
    conn.close()
    return monitor_id


def test_check_monitor_api_includes_reason_and_confidence_for_in_stock(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    monitor_id = _seed_monitor(app_module)
    expected = app_module.MonitorResult(
        in_stock=True,
        price_cents=2499,
        title="Pokemon Product",
        status_text="in_stock",
        availability_reason="marker_in_stock",
        parser_confidence=0.91,
        keyword_matched=True,
    )

    monkeypatch.setattr(app_module, "fetch_monitor", lambda monitor: expected)

    resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["availability_reason"] == "marker_in_stock"
    assert payload["parser_confidence"] == 0.91


def test_check_monitor_api_includes_reason_and_confidence_for_out_of_stock(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    monitor_id = _seed_monitor(app_module)
    expected = app_module.MonitorResult(
        in_stock=False,
        price_cents=2499,
        title="Pokemon Product",
        status_text="out_or_unknown",
        availability_reason="marker_out_of_stock",
        parser_confidence=0.94,
        keyword_matched=True,
    )

    monkeypatch.setattr(app_module, "fetch_monitor", lambda monitor: expected)

    resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["availability_reason"] == "marker_out_of_stock"
    assert payload["parser_confidence"] == 0.94


def test_check_monitor_api_includes_reason_and_confidence_for_ambiguous_markup(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    monitor_id = _seed_monitor(app_module)
    expected = app_module.MonitorResult(
        in_stock=False,
        price_cents=None,
        title="Pokemon Product",
        status_text="out_or_unknown",
        availability_reason="fallback_unknown",
        parser_confidence=0.2,
        keyword_matched=None,
    )

    monkeypatch.setattr(app_module, "fetch_monitor", lambda monitor: expected)

    resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["availability_reason"] == "fallback_unknown"
    assert payload["parser_confidence"] == 0.2
