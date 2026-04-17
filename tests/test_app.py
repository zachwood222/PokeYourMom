import importlib
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_app(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    return reloaded


def test_create_monitor_validates_retailer(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={"retailer": "amazon", "product_url": "https://example.com", "poll_interval_seconds": 20},
    )

    assert resp.status_code == 400
    assert "Unsupported retailer" in resp.get_json()["error"]


def test_create_monitor_requires_http_url(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={"retailer": "walmart", "product_url": "ftp://example.com", "poll_interval_seconds": 20},
    )

    assert resp.status_code == 400
    assert "product_url" in resp.get_json()["error"]


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
    app_module.create_event_and_deliver(monitor, result_keyword_miss)

    result_price_too_high = app_module.MonitorResult(
        in_stock=True, price_cents=3500, title="Pokemon 151 Box", status_text="in_stock"
    )
    app_module.create_event_and_deliver(monitor, result_price_too_high)

    conn = app_module.db()
    event_count = conn.execute("select count(*) as c from events").fetchone()["c"]
    conn.close()

    assert event_count == 0
    assert posted_payloads == []


def test_dashboard_summary_endpoint(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/dashboard/summary")

    assert resp.status_code == 200
    payload = resp.get_json()
    assert payload["total_monitors"] == 0
    assert payload["enabled_monitors"] == 0
    assert payload["events_last_24h"] == 0
    assert payload["events_last_7d"] == 0
    assert payload["delivery_success_rate"] == 0.0
