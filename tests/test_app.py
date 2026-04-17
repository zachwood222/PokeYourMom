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
    eligible_keyword_miss = app_module.alert_eligibility(monitor, result_keyword_miss)
    app_module.create_event_and_deliver(monitor, result_keyword_miss, eligible_keyword_miss)

    result_price_too_high = app_module.MonitorResult(
        in_stock=True, price_cents=3500, title="Pokemon 151 Box", status_text="in_stock"
    )
    eligible_price_too_high = app_module.alert_eligibility(monitor, result_price_too_high)
    app_module.create_event_and_deliver(monitor, result_price_too_high, eligible_price_too_high)

    conn = app_module.db()
    event_count = conn.execute("select count(*) as c from events").fetchone()["c"]
    conn.close()

    assert event_count == 0
    assert posted_payloads == []


def test_webhook_test_endpoint_returns_diagnostics_and_updates_health(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    created = client.post(
        "/api/webhooks",
        json={
            "name": "Ops",
            "webhook_url": "https://discord.com/api/webhooks/test",
            "notify_success": True,
            "notify_failures": True,
            "notify_restock_only": False,
        },
    )
    assert created.status_code == 201
    hook_id = created.get_json()["id"]

    class DummyResponse:
        status_code = 204
        text = "ok"

    monkeypatch.setattr(app_module.requests, "post", lambda *args, **kwargs: DummyResponse())
    tested = client.post(f"/api/webhooks/{hook_id}/test")
    payload = tested.get_json()
    assert tested.status_code == 200
    assert payload["ok"] is True
    assert payload["status_code"] == 204
    assert "latency_ms" in payload

    listed = client.get("/api/webhooks").get_json()
    found = next(w for w in listed if w["id"] == hook_id)
    assert found["last_test_status"] == "sent"


def test_meta_and_schedule_creation(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    meta = client.get("/api/meta")
    assert meta.status_code == 200
    assert "app_version" in meta.get_json()

    created_monitor = client.post(
        "/api/monitors",
        json={"retailer": "walmart", "product_url": "https://example.com/p", "poll_interval_seconds": 20},
    )
    monitor_id = created_monitor.get_json()["id"]
    schedule = client.post(
        "/api/schedules",
        json={
            "monitor_ids": [monitor_id],
            "new_poll_interval_seconds": 35,
            "run_at": "2000-01-01T00:00:00+00:00",
        },
    )
    assert schedule.status_code == 200
    created = schedule.get_json()["created"]
    assert len(created) == 1

    conn = app_module.db()
    app_module.apply_due_schedules(conn)
    conn.commit()
    updated = conn.execute("select poll_interval_seconds from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    assert updated["poll_interval_seconds"] == 35
