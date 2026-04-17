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
    monkeypatch.setenv("API_AUTH_TOKEN", "api-token")

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
    assert payload["id"] == 1
    assert payload["name"] == "My Workspace"


def test_webhook_routes_require_owner_role(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    cur = conn.execute(
        "insert into users(email, name, bearer_token, created_at) values (?, ?, ?, ?)",
        ("member@example.com", "Member User", "member-token", app_module.utc_now()),
    )
    member_id = cur.lastrowid
    conn.execute(
        """
        insert into workspace_members(workspace_id, user_id, role, created_at)
        values (1, ?, 'member', ?)
        """,
        (member_id, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    list_resp = client.get("/api/webhooks", headers=_auth_headers("member-token"))
    create_resp = client.post(
        "/api/webhooks",
        json={"name": "Discord", "webhook_url": "https://discord.com/api/webhooks/test"},
        headers=_auth_headers("member-token"),
    )

    assert list_resp.status_code == 403
    assert create_resp.status_code == 403
    assert list_resp.get_json()["error"] == "Workspace owner access required"


def test_webhook_cross_tenant_access_is_blocked(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace_id = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values (?, 'Other Hook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (other_workspace_id, app_module.utc_now()),
    )
    other_webhook_id = conn.execute("select id from webhooks where workspace_id = ?", (other_workspace_id,)).fetchone()[
        "id"
    ]
    conn.commit()
    conn.close()

    test_resp = client.post(f"/api/webhooks/{other_webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(f"/api/webhooks/{other_webhook_id}", json={"enabled": False}, headers=_auth_headers())
    delete_resp = client.delete(f"/api/webhooks/{other_webhook_id}", headers=_auth_headers())

    assert test_resp.status_code == 404
    assert patch_resp.status_code == 404
    assert delete_resp.status_code == 404

    conn = app_module.db()
    still_exists = conn.execute("select id from webhooks where id = ?", (other_webhook_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_webhook_owner_can_manage_webhook_routes(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    class DummyResponse:
        status_code = 204
        text = ""

    monkeypatch.setattr(app_module.requests, "post", lambda *args, **kwargs: DummyResponse())

    create_resp = client.post(
        "/api/webhooks",
        json={"name": "Main", "webhook_url": "https://discord.com/api/webhooks/test"},
        headers=_auth_headers(),
    )
    webhook_id = create_resp.get_json()["id"]

    list_resp = client.get("/api/webhooks", headers=_auth_headers())
    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"enabled": False, "notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())

    assert create_resp.status_code == 201
    assert list_resp.status_code == 200
    assert test_resp.status_code == 200
    assert patch_resp.status_code == 200
    assert delete_resp.status_code == 200
