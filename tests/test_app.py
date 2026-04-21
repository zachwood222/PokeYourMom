import importlib
import hashlib
import hmac
import json
import sqlite3
import sys
import time
from datetime import datetime, timedelta, timezone
from pathlib import Path

import pytest
from parser_fixture_harness import load_fixture_html

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_app(tmp_path, monkeypatch):
    db_path = tmp_path / "test.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "test-token")
    monkeypatch.setenv("API_AUTH_TOKEN", "test-token")
    monkeypatch.setenv("STRIPE_WEBHOOK_SECRET", "whsec_test")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    return reloaded


def _auth_headers(token="test-token"):
    return {"Authorization": f"Bearer {token}", "X-API-Token": "api-token"}


def _stripe_signature(payload: str, secret: str, timestamp: int | None = None) -> str:
    ts = timestamp or int(time.time())
    signed = f"{ts}.{payload}".encode("utf-8")
    digest = hmac.new(secret.encode("utf-8"), signed, hashlib.sha256).hexdigest()
    return f"t={ts},v1={digest}"


def test_format_log_entry_has_structured_shape(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    entry = app_module.format_log_entry(
        level="WARNING",
        message="monitor check failed",
        workspace_id=12,
        monitor_id=34,
        correlation_id="cid-123",
    )

    assert set(entry.keys()) == {
        "timestamp",
        "level",
        "message",
        "workspace_id",
        "monitor_id",
        "correlation_id",
    }
    assert entry["level"] == "warning"
    assert entry["message"] == "monitor check failed"
    assert entry["workspace_id"] == 12
    assert entry["monitor_id"] == 34
    assert entry["correlation_id"] == "cid-123"


def test_log_outputs_json_and_emits_socket_event(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    printed = []
    emitted = []

    monkeypatch.setattr("builtins.print", lambda value: printed.append(value))
    monkeypatch.setattr(app_module.socketio, "emit", lambda event, payload: emitted.append((event, payload)))

    app_module.log(
        "webhook target https://discord.com/api/webhooks/abc123",
        level="WARNING",
        workspace_id=2,
        monitor_id=9,
        correlation_id="corr-9",
    )

    assert len(printed) == 1
    printed_entry = json.loads(printed[0])
    assert printed_entry["level"] == "warning"
    assert printed_entry["workspace_id"] == 2
    assert printed_entry["monitor_id"] == 9
    assert printed_entry["correlation_id"] == "corr-9"
    assert "***redacted***" in printed_entry["message"]

    assert emitted == [("log", printed_entry)]


def test_correlation_id_header_generated_for_api_request(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/monitors", headers=_auth_headers())

    assert resp.status_code == 200
    assert app_module.CORRELATION_ID_HEADER in resp.headers
    assert resp.headers[app_module.CORRELATION_ID_HEADER]


def test_correlation_id_propagates_from_request_to_log_and_response(tmp_path, monkeypatch):
    monkeypatch.setenv("CAPTCHA_SECRET_KEY", "captcha-secret")
    monkeypatch.setenv("CAPTCHA_VERIFY_URL", "https://captcha.local/verify")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    printed = []

    monkeypatch.setattr("builtins.print", lambda value: printed.append(value))

    class DummyCaptchaResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"success": False}

    monkeypatch.setattr(app_module.requests, "post", lambda *args, **kwargs: DummyCaptchaResponse())

    request_cid = "cid-from-client"
    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
            "captcha_token": "bad-token",
        },
        headers={**_auth_headers(), app_module.CORRELATION_ID_HEADER: request_cid},
    )

    assert resp.status_code == 403
    assert resp.headers[app_module.CORRELATION_ID_HEADER] == request_cid
    parsed = [json.loads(line) for line in printed if isinstance(line, str) and line.startswith("{")]
    assert any(entry.get("correlation_id") == request_cid for entry in parsed)


def test_protected_endpoint_requires_auth(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/monitors")

    assert resp.status_code == 401
    assert "Unauthorized" in resp.get_json()["error"]


def test_workspace_endpoint_requires_auth(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/workspace")

    assert resp.status_code == 401
    assert resp.get_json() == {"error": "Unauthorized"}


def test_workspace_endpoint_allows_authenticated_user_and_returns_context(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/workspace", headers={"Authorization": "Bearer test-token"})
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["workspace"]["id"] == 1
    assert payload["user"]["email"] == "owner@local.test"


def test_create_monitor_validates_retailer(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={"retailer": "amazon", "product_url": "https://example.com", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    )

    assert resp.status_code == 400
    assert resp.get_json()["error"] == "Unsupported retailer 'amazon'"


def test_create_monitor_requires_http_url(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={"retailer": "walmart", "product_url": "ftp://example.com", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    )

    assert resp.status_code == 400
    assert resp.get_json()["error"] == "product_url must be http(s)"


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
    assert payload["category"] == "pokemon"


def test_create_monitor_validates_category(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "target",
            "category": "model_kits",
            "product_url": "https://www.target.com/p/example",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )

    assert resp.status_code == 400
    assert resp.get_json()["error"] == "Unsupported category 'model_kits'"


def test_create_monitor_validates_retailer_category_combo(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "category": "sports_cards",
            "product_url": "https://www.walmart.com/ip/example",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )

    assert resp.status_code == 400
    assert resp.get_json()["error"] == "Retailer 'walmart' does not support category 'sports_cards'"


def test_captcha_valid_token_allows_protected_post(tmp_path, monkeypatch):
    monkeypatch.setenv("CAPTCHA_SECRET_KEY", "captcha-secret")
    monkeypatch.setenv("CAPTCHA_VERIFY_URL", "https://captcha.local/verify")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    class DummyResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"success": True}

    def fake_post(url, data, timeout):
        assert url == "https://captcha.local/verify"
        assert data["secret"] == "captcha-secret"
        assert data["response"] == "token-ok"
        return DummyResponse()

    monkeypatch.setattr(app_module.requests, "post", fake_post)

    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
            "captcha_token": "token-ok",
        },
        headers=_auth_headers(),
    )

    assert resp.status_code == 201


def test_create_monitor_validates_behavior_metadata_retailer_profiles(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
            "behavior_metadata": {
                "jitter_ratio": 0.25,
                "retailer_profiles": {
                    "invalid-retailer": {"base_delay_seconds": 0.5},
                },
            },
        },
        headers=_auth_headers(),
    )
    assert resp.status_code == 400
    assert "unsupported retailer" in resp.get_json()["error"]


def test_workspace_behavior_metadata_can_be_patched(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    response = client.patch(
        "/api/workspace",
        json={
            "behavior_metadata": {
                "profile": "safer_default",
                "base_delay_seconds": 0.3,
                "retailer_profiles": {"target": {"base_delay_seconds": 0.4}},
            }
        },
        headers=_auth_headers(),
    )
    assert response.status_code == 200
    workspace = response.get_json()["workspace"]
    payload = json.loads(workspace["behavior_metadata"])
    assert payload["profile"] == "safer_default"
    assert payload["retailer_profiles"]["target"]["base_delay_seconds"] == 0.4


def test_update_monitor_allows_behavior_and_session_metadata(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    created = client.post(
        "/api/monitors",
        json={
            "retailer": "bestbuy",
            "product_url": "https://example.com/bb",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    monitor_id = created.get_json()["id"]
    patched = client.patch(
        f"/api/monitors/{monitor_id}",
        json={
            "enabled": False,
            "session_metadata": {"cookie_profile": "A"},
            "behavior_metadata": {"base_delay_seconds": 0.45, "jitter_ratio": 0.1},
        },
        headers=_auth_headers(),
    )
    assert patched.status_code == 200
    payload = patched.get_json()
    assert payload["enabled"] == 0
    assert json.loads(payload["session_metadata"])["cookie_profile"] == "A"
    assert json.loads(payload["behavior_metadata"])["base_delay_seconds"] == 0.45


def test_captcha_invalid_or_missing_token_rejects_protected_post(tmp_path, monkeypatch):
    monkeypatch.setenv("CAPTCHA_SECRET_KEY", "captcha-secret")
    monkeypatch.setenv("CAPTCHA_VERIFY_URL", "https://captcha.local/verify")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    def fake_post(url, data, timeout):
        class DummyResponse:
            status_code = 200

            @staticmethod
            def json():
                return {"success": False}

        return DummyResponse()

    monkeypatch.setattr(app_module.requests, "post", fake_post)

    missing_token = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    invalid_token = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
            "captcha_token": "bad-token",
        },
        headers=_auth_headers(),
    )

    assert missing_token.status_code == 403
    assert missing_token.get_json()["reason"] == "missing_token"
    assert invalid_token.status_code == 403
    assert invalid_token.get_json()["reason"] == "provider_rejected"


def test_captcha_provider_errors_fail_safely(tmp_path, monkeypatch):
    monkeypatch.setenv("CAPTCHA_SECRET_KEY", "captcha-secret")
    monkeypatch.setenv("CAPTCHA_VERIFY_URL", "https://captcha.local/verify")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    def fake_post(url, data, timeout):
        raise RuntimeError("provider down")

    monkeypatch.setattr(app_module.requests, "post", fake_post)

    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
            "captcha_token": "token-any",
        },
        headers=_auth_headers(),
    )

    assert resp.status_code == 400
    assert resp.get_json()["reason"] == "provider_unreachable"


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


def test_monitor_resource_endpoints_return_404_for_cross_tenant_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-monitor', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    read_resp = client.get(f"/api/monitors/{monitor_id}", headers=_auth_headers())
    update_resp = client.patch(
        f"/api/monitors/{monitor_id}",
        json={"enabled": False},
        headers=_auth_headers(),
    )
    check_resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    delete_resp = client.delete(f"/api/monitors/{monitor_id}", headers=_auth_headers())

    assert read_resp.status_code == 404
    assert update_resp.status_code == 404
    assert check_resp.status_code == 404
    assert delete_resp.status_code == 404

    conn = app_module.db()
    still_exists = conn.execute("select 1 from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_events_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]

    own_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values
        (?, 'in_stock', 'Own Event', 'https://example.com/own', 'walmart', 1999, ?, 'own-event'),
        (?, 'in_stock', 'Other Event', 'https://example.com/other', 'target', 2999, ?, 'other-event')
        """,
        (own_monitor, app_module.utc_now(), other_monitor, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["title"] == "Own Event"


def test_events_endpoint_keeps_desc_limit_and_excludes_cross_tenant_rows(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    own_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own-seed', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-seed', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid

    for idx in range(1, 121):
        conn.execute(
            """
            insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
            values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
            """,
            (
                own_monitor_id,
                f"Own Event {idx}",
                "https://example.com/own-seed",
                "walmart",
                1000 + idx,
                app_module.utc_now(),
                f"own-seq-{idx}",
            ),
        )
        if idx <= 20:
            conn.execute(
                """
                insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
                values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
                """,
                (
                    other_monitor_id,
                    f"Other Event {idx}",
                    "https://example.com/other-seed",
                    "target",
                    2000 + idx,
                    app_module.utc_now(),
                    f"other-seq-{idx}",
                ),
            )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 100
    assert all(row["title"].startswith("Own Event ") for row in payload)
    ids = [row["id"] for row in payload]
    assert ids == sorted(ids, reverse=True)


def test_webhooks_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
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
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values
        (1, 'Own Hook', 'https://discord.com/api/webhooks/own', ?),
        (?, 'Other Hook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (app_module.utc_now(), other_workspace, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/webhooks", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["name"] == "Own Hook"


def test_webhook_routes_allow_authorized_workspace_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_resp = client.post(
        "/api/webhooks",
        json={"name": "Main", "webhook_url": "https://discord.com/api/webhooks/abc123"},
        headers=_auth_headers(),
    )
    created = create_resp.get_json()
    webhook_id = created["id"]

    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    class DummyResponse:
        status_code = 204
        text = ""

    class FakeReqResult:
        def __init__(self, response):
            self.response = response
            self.error = None
            self.telemetry = None

    monkeypatch.setattr(app_module, "perform_request", lambda **kwargs: FakeReqResult(DummyResponse()))
    monkeypatch.setattr(app_module.requests, "post", lambda *args, **kwargs: DummyResponse())

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())

    assert create_resp.status_code == 201
    assert list_resp.status_code == 200
    assert any(row["id"] == webhook_id for row in list_resp.get_json())
    assert test_resp.status_code == 200
    assert patch_resp.status_code == 200
    assert patch_resp.get_json()["notify_failures"] == 1
    assert delete_resp.status_code == 200


def test_webhook_routes_block_cross_tenant_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    webhook_id = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values (?, 'OtherHook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())
    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    assert test_resp.status_code == 404
    assert patch_resp.status_code == 404
    assert delete_resp.status_code == 404
    assert list_resp.status_code == 200
    assert list_resp.get_json() == []

    conn = app_module.db()
    still_exists = conn.execute("select 1 from webhooks where id = ?", (webhook_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_keyword_and_max_price_filter_block_event(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-monitor', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    read_resp = client.get(f"/api/monitors/{monitor_id}", headers=_auth_headers())
    update_resp = client.patch(
        f"/api/monitors/{monitor_id}",
        json={"enabled": False},
        headers=_auth_headers(),
    )
    check_resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    delete_resp = client.delete(f"/api/monitors/{monitor_id}", headers=_auth_headers())

    assert read_resp.status_code == 404
    assert update_resp.status_code == 404
    assert check_resp.status_code == 404
    assert delete_resp.status_code == 404

    conn = app_module.db()
    still_exists = conn.execute("select 1 from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_events_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]

    own_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values
        (?, 'in_stock', 'Own Event', 'https://example.com/own', 'walmart', 1999, ?, 'own-event'),
        (?, 'in_stock', 'Other Event', 'https://example.com/other', 'target', 2999, ?, 'other-event')
        """,
        (own_monitor, app_module.utc_now(), other_monitor, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["title"] == "Own Event"


def test_events_endpoint_keeps_desc_limit_and_excludes_cross_tenant_rows(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    own_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own-seed', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-seed', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid

    for idx in range(1, 121):
        conn.execute(
            """
            insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
            values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
            """,
            (
                own_monitor_id,
                f"Own Event {idx}",
                "https://example.com/own-seed",
                "walmart",
                1000 + idx,
                app_module.utc_now(),
                f"own-seq-{idx}",
            ),
        )
        if idx <= 20:
            conn.execute(
                """
                insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
                values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
                """,
                (
                    other_monitor_id,
                    f"Other Event {idx}",
                    "https://example.com/other-seed",
                    "target",
                    2000 + idx,
                    app_module.utc_now(),
                    f"other-seq-{idx}",
                ),
            )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 100
    assert all(row["title"].startswith("Own Event ") for row in payload)
    ids = [row["id"] for row in payload]
    assert ids == sorted(ids, reverse=True)


def test_webhooks_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
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
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values
        (1, 'Own Hook', 'https://discord.com/api/webhooks/own', ?),
        (?, 'Other Hook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (app_module.utc_now(), other_workspace, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/webhooks", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["name"] == "Own Hook"


def test_webhook_routes_allow_authorized_workspace_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_resp = client.post(
        "/api/webhooks",
        json={"name": "Main", "webhook_url": "https://discord.com/api/webhooks/abc123"},
        headers=_auth_headers(),
    )
    created = create_resp.get_json()
    webhook_id = created["id"]

    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    class DummyResponse:
        status_code = 204
        text = ""

    class FakeReqResult:
        def __init__(self, response):
            self.response = response
            self.error = None
            self.telemetry = None

    monkeypatch.setattr(app_module, "perform_request", lambda **kwargs: FakeReqResult(DummyResponse()))
    monkeypatch.setattr(app_module.requests, "post", lambda *args, **kwargs: DummyResponse())

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())

    assert create_resp.status_code == 201
    assert list_resp.status_code == 200
    assert any(row["id"] == webhook_id for row in list_resp.get_json())
    assert test_resp.status_code == 200
    assert patch_resp.status_code == 200
    assert patch_resp.get_json()["notify_failures"] == 1
    assert delete_resp.status_code == 200


def test_webhook_routes_block_cross_tenant_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    webhook_id = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values (?, 'OtherHook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())
    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    assert test_resp.status_code == 404
    assert patch_resp.status_code == 404
    assert delete_resp.status_code == 404
    assert list_resp.status_code == 200
    assert list_resp.get_json() == []

    conn = app_module.db()
    still_exists = conn.execute("select 1 from webhooks where id = ?", (webhook_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_keyword_and_max_price_filter_block_event(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-monitor', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    read_resp = client.get(f"/api/monitors/{monitor_id}", headers=_auth_headers())
    update_resp = client.patch(
        f"/api/monitors/{monitor_id}",
        json={"enabled": False},
        headers=_auth_headers(),
    )
    check_resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    delete_resp = client.delete(f"/api/monitors/{monitor_id}", headers=_auth_headers())

    assert read_resp.status_code == 404
    assert update_resp.status_code == 404
    assert check_resp.status_code == 404
    assert delete_resp.status_code == 404

    conn = app_module.db()
    still_exists = conn.execute("select 1 from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_events_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]

    own_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values
        (?, 'in_stock', 'Own Event', 'https://example.com/own', 'walmart', 1999, ?, 'own-event'),
        (?, 'in_stock', 'Other Event', 'https://example.com/other', 'target', 2999, ?, 'other-event')
        """,
        (own_monitor, app_module.utc_now(), other_monitor, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["title"] == "Own Event"


def test_events_endpoint_keeps_desc_limit_and_excludes_cross_tenant_rows(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    own_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own-seed', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-seed', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid

def test_create_event_and_deliver_uses_shared_request_helper_only(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    captured = {}

    class DummyResponse:
        status_code = 204
        text = ""
        ok = True

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    def fail_direct_post(*_args, **_kwargs):
        raise AssertionError("direct requests.post should not be used for webhook delivery")

    monkeypatch.setattr(app_module.requests, "post", fail_direct_post)

    conn = app_module.db()
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/p', 20, ?)
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

    result = app_module.MonitorResult(
        in_stock=True,
        price_cents=2500,
        title="Pokemon Product",
        status_text="in_stock",
        keyword_matched=True,
    )
    app_module.create_event_and_deliver(monitor, result, eligible=True)

    assert captured["method"] == "POST"
    assert captured["url"] == "https://discord.com/api/webhooks/test"
    assert captured["retry_total"] == 1
    assert captured["backoff_factor"] == 0.2


def test_evaluate_page_sets_keyword_and_price_fields(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    for idx in range(1, 121):
        conn.execute(
            """
            insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
            values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
            """,
            (
                own_monitor_id,
                f"Own Event {idx}",
                "https://example.com/own-seed",
                "walmart",
                1000 + idx,
                app_module.utc_now(),
                f"own-seq-{idx}",
            ),
        )
        if idx <= 20:
            conn.execute(
                """
                insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
                values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
                """,
                (
                    other_monitor_id,
                    f"Other Event {idx}",
                    "https://example.com/other-seed",
                    "target",
                    2000 + idx,
                    app_module.utc_now(),
                    f"other-seq-{idx}",
                ),
            )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 100
    assert all(row["title"].startswith("Own Event ") for row in payload)
    ids = [row["id"] for row in payload]
    assert ids == sorted(ids, reverse=True)


def test_webhooks_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
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
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values
        (1, 'Own Hook', 'https://discord.com/api/webhooks/own', ?),
        (?, 'Other Hook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (app_module.utc_now(), other_workspace, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/webhooks", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["name"] == "Own Hook"


def test_webhook_routes_allow_authorized_workspace_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_resp = client.post(
        "/api/webhooks",
        json={"name": "Main", "webhook_url": "https://discord.com/api/webhooks/abc123"},
        headers=_auth_headers(),
    )
    created = create_resp.get_json()
    webhook_id = created["id"]

    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    class DummyResponse:
        status_code = 204
        text = ""

    class FakeReqResult:
        def __init__(self, response):
            self.response = response
            self.error = None
            self.telemetry = None

    monkeypatch.setattr(app_module, "perform_request", lambda **kwargs: FakeReqResult(DummyResponse()))
    monkeypatch.setattr(app_module.requests, "post", lambda *args, **kwargs: DummyResponse())

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())

    assert create_resp.status_code == 201
    assert list_resp.status_code == 200
    assert any(row["id"] == webhook_id for row in list_resp.get_json())
    assert test_resp.status_code == 200
    assert patch_resp.status_code == 200
    assert patch_resp.get_json()["notify_failures"] == 1
    assert delete_resp.status_code == 200


def test_webhook_routes_block_cross_tenant_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    webhook_id = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values (?, 'OtherHook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())
    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    assert test_resp.status_code == 404
    assert patch_resp.status_code == 404
    assert delete_resp.status_code == 404
    assert list_resp.status_code == 200
    assert list_resp.get_json() == []

    conn = app_module.db()
    still_exists = conn.execute("select 1 from webhooks where id = ?", (webhook_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_keyword_and_max_price_filter_block_event(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-monitor', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    read_resp = client.get(f"/api/monitors/{monitor_id}", headers=_auth_headers())
    update_resp = client.patch(
        f"/api/monitors/{monitor_id}",
        json={"enabled": False},
        headers=_auth_headers(),
    )
    check_resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    delete_resp = client.delete(f"/api/monitors/{monitor_id}", headers=_auth_headers())

    assert read_resp.status_code == 404
    assert update_resp.status_code == 404
    assert check_resp.status_code == 404
    assert delete_resp.status_code == 404

    conn = app_module.db()
    still_exists = conn.execute("select 1 from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_events_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]

    own_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values
        (?, 'in_stock', 'Own Event', 'https://example.com/own', 'walmart', 1999, ?, 'own-event'),
        (?, 'in_stock', 'Other Event', 'https://example.com/other', 'target', 2999, ?, 'other-event')
        """,
        (own_monitor, app_module.utc_now(), other_monitor, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["title"] == "Own Event"


def test_events_endpoint_keeps_desc_limit_and_excludes_cross_tenant_rows(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    own_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own-seed', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-seed', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid

    for idx in range(1, 121):
        conn.execute(
            """
            insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
            values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
            """,
            (
                own_monitor_id,
                f"Own Event {idx}",
                "https://example.com/own-seed",
                "walmart",
                1000 + idx,
                app_module.utc_now(),
                f"own-seq-{idx}",
            ),
        )
        if idx <= 20:
            conn.execute(
                """
                insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
                values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
                """,
                (
                    other_monitor_id,
                    f"Other Event {idx}",
                    "https://example.com/other-seed",
                    "target",
                    2000 + idx,
                    app_module.utc_now(),
                    f"other-seq-{idx}",
                ),
            )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 100
    assert all(row["title"].startswith("Own Event ") for row in payload)
    ids = [row["id"] for row in payload]
    assert ids == sorted(ids, reverse=True)


def test_webhooks_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
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
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values
        (1, 'Own Hook', 'https://discord.com/api/webhooks/own', ?),
        (?, 'Other Hook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (app_module.utc_now(), other_workspace, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/webhooks", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["name"] == "Own Hook"


def test_webhook_routes_allow_authorized_workspace_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_resp = client.post(
        "/api/webhooks",
        json={"name": "Main", "webhook_url": "https://discord.com/api/webhooks/abc123"},
        headers=_auth_headers(),
    )
    created = create_resp.get_json()
    webhook_id = created["id"]

    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    class DummyResponse:
        status_code = 204
        text = ""

    class FakeReqResult:
        def __init__(self, response):
            self.response = response
            self.error = None
            self.telemetry = None

    monkeypatch.setattr(app_module, "perform_request", lambda **kwargs: FakeReqResult(DummyResponse()))
    monkeypatch.setattr(app_module.requests, "post", lambda *args, **kwargs: DummyResponse())

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())

    assert create_resp.status_code == 201
    assert list_resp.status_code == 200
    assert any(row["id"] == webhook_id for row in list_resp.get_json())
    assert test_resp.status_code == 200
    assert patch_resp.status_code == 200
    assert patch_resp.get_json()["notify_failures"] == 1
    assert delete_resp.status_code == 200


def test_webhook_routes_block_cross_tenant_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    webhook_id = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values (?, 'OtherHook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())
    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    assert test_resp.status_code == 404
    assert patch_resp.status_code == 404
    assert delete_resp.status_code == 404
    assert list_resp.status_code == 200
    assert list_resp.get_json() == []

    conn = app_module.db()
    still_exists = conn.execute("select 1 from webhooks where id = ?", (webhook_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_keyword_and_max_price_filter_block_event(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-monitor', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    read_resp = client.get(f"/api/monitors/{monitor_id}", headers=_auth_headers())
    update_resp = client.patch(
        f"/api/monitors/{monitor_id}",
        json={"enabled": False},
        headers=_auth_headers(),
    )
    check_resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    delete_resp = client.delete(f"/api/monitors/{monitor_id}", headers=_auth_headers())

    assert read_resp.status_code == 404
    assert update_resp.status_code == 404
    assert check_resp.status_code == 404
    assert delete_resp.status_code == 404

    conn = app_module.db()
    still_exists = conn.execute("select 1 from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_events_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]

    own_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values
        (?, 'in_stock', 'Own Event', 'https://example.com/own', 'walmart', 1999, ?, 'own-event'),
        (?, 'in_stock', 'Other Event', 'https://example.com/other', 'target', 2999, ?, 'other-event')
        """,
        (own_monitor, app_module.utc_now(), other_monitor, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["title"] == "Own Event"


def test_events_endpoint_keeps_desc_limit_and_excludes_cross_tenant_rows(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    own_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/own-seed', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    other_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'target', 'https://example.com/other-seed', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid

    for idx in range(1, 121):
        conn.execute(
            """
            insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
            values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
            """,
            (
                own_monitor_id,
                f"Own Event {idx}",
                "https://example.com/own-seed",
                "walmart",
                1000 + idx,
                app_module.utc_now(),
                f"own-seq-{idx}",
            ),
        )
        if idx <= 20:
            conn.execute(
                """
                insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
                values (?, 'in_stock', ?, ?, ?, ?, ?, ?)
                """,
                (
                    other_monitor_id,
                    f"Other Event {idx}",
                    "https://example.com/other-seed",
                    "target",
                    2000 + idx,
                    app_module.utc_now(),
                    f"other-seq-{idx}",
                ),
            )
    conn.commit()
    conn.close()

    resp = client.get("/api/events", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 100
    assert all(row["title"].startswith("Own Event ") for row in payload)
    ids = [row["id"] for row in payload]
    assert ids == sorted(ids, reverse=True)


def test_webhooks_endpoint_scopes_results_to_authenticated_workspace(tmp_path, monkeypatch):
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
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values
        (1, 'Own Hook', 'https://discord.com/api/webhooks/own', ?),
        (?, 'Other Hook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (app_module.utc_now(), other_workspace, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/webhooks", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert len(payload) == 1
    assert payload[0]["name"] == "Own Hook"


def test_webhook_routes_allow_authorized_workspace_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_resp = client.post(
        "/api/webhooks",
        json={"name": "Main", "webhook_url": "https://discord.com/api/webhooks/abc123"},
        headers=_auth_headers(),
    )
    created = create_resp.get_json()
    webhook_id = created["id"]

    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    class DummyResponse:
        status_code = 204
        text = ""

    class FakeReqResult:
        def __init__(self, response):
            self.response = response
            self.error = None
            self.telemetry = None

    monkeypatch.setattr(app_module, "perform_request", lambda **kwargs: FakeReqResult(DummyResponse()))
    monkeypatch.setattr(app_module.requests, "post", lambda *args, **kwargs: DummyResponse())

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())

    assert create_resp.status_code == 201
    assert list_resp.status_code == 200
    assert any(row["id"] == webhook_id for row in list_resp.get_json())
    assert test_resp.status_code == 200
    assert patch_resp.status_code == 200
    assert patch_resp.get_json()["notify_failures"] == 1
    assert delete_resp.status_code == 200


def test_webhook_routes_block_cross_tenant_access(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    webhook_id = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values (?, 'OtherHook', 'https://discord.com/api/webhooks/other', ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.commit()
    conn.close()

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    patch_resp = client.patch(
        f"/api/webhooks/{webhook_id}",
        json={"notify_failures": True},
        headers=_auth_headers(),
    )
    delete_resp = client.delete(f"/api/webhooks/{webhook_id}", headers=_auth_headers())
    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    assert test_resp.status_code == 404
    assert patch_resp.status_code == 404
    assert delete_resp.status_code == 404
    assert list_resp.status_code == 200
    assert list_resp.get_json() == []

    conn = app_module.db()
    still_exists = conn.execute("select 1 from webhooks where id = ?", (webhook_id,)).fetchone()
    conn.close()
    assert still_exists is not None


def test_keyword_and_max_price_filter_block_event(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    posted_payloads = []

    class DummyResponse:
        status_code = 204
        text = ""
        ok = True

    class FakeReqResult:
        def __init__(self, response):
            self.response = response
            self.error = None
            self.telemetry = None

    def fake_request(**kwargs):
        posted_payloads.append((kwargs["url"], kwargs["json"], kwargs["timeout"]))
        return FakeReqResult(DummyResponse())

    monkeypatch.setattr(app_module, "perform_request", fake_request)

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


def test_create_event_and_deliver_uses_shared_request_helper_only(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    captured = {}

    class DummyResponse:
        status_code = 204
        text = ""
        ok = True

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    def fail_direct_post(*_args, **_kwargs):
        raise AssertionError("direct requests.post should not be used for webhook delivery")

    monkeypatch.setattr(app_module.requests, "post", fail_direct_post)

    conn = app_module.db()
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/p', 20, ?)
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

    result = app_module.MonitorResult(
        in_stock=True,
        price_cents=2500,
        title="Pokemon Product",
        status_text="in_stock",
        keyword_matched=True,
    )
    app_module.create_event_and_deliver(monitor, result, eligible=True)

    assert captured["method"] == "POST"
    assert captured["url"] == "https://discord.com/api/webhooks/test"
    assert captured["retry_total"] == 1
    assert captured["backoff_factor"] == 0.2


def test_evaluate_page_sets_keyword_and_price_fields(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    captured = {}

    class DummyResponse:
        status_code = 204
        text = ""
        ok = True

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    def fail_direct_post(*_args, **_kwargs):
        raise AssertionError("direct requests.post should not be used for webhook delivery")

    monkeypatch.setattr(app_module.requests, "post", fail_direct_post)

    conn = app_module.db()
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/p', 20, ?)
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

    result = app_module.MonitorResult(
        in_stock=True,
        price_cents=2500,
        title="Pokemon Product",
        status_text="in_stock",
        keyword_matched=True,
    )
    app_module.create_event_and_deliver(monitor, result, eligible=True)

    assert captured["method"] == "POST"
    assert captured["url"] == "https://discord.com/api/webhooks/test"
    assert captured["retry_total"] == 1
    assert captured["backoff_factor"] == 0.2


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
    workspace_columns = {row[1] for row in conn.execute("pragma table_info(workspaces)").fetchall()}
    conn.close()

    assert "msrp_cents" in columns
    assert "proxy_url" in columns
    assert "session_task_key" in columns
    assert "session_metadata" in columns
    assert "behavior_metadata" in columns
    assert "proxy_url" in workspace_columns
    assert "session_metadata" in workspace_columns
    assert "behavior_metadata" in workspace_columns


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_fetch_monitor_uses_monitor_proxy_override_and_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>in stock add to cart $19.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'target', 'https://example.com/item', 20, 'http://monitor-proxy:9090', 'session-custom-1', ?)
        """,
        (app_module.utc_now(),),
    )
    monitor = conn.execute("select * from monitors where id = ?", (cur.lastrowid,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is True
    assert captured["task_key"] == "session-custom-1"
    assert captured["proxy_url"] == "http://monitor-proxy:9090"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_fetch_monitor_uses_workspace_proxy_and_default_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>out of stock $29.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'walmart', 'https://example.com/item2', 20, null, null, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = int(cur.lastrowid)
    monitor = conn.execute("select * from monitors where id = ?", (monitor_id,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is False
    assert captured["task_key"] == f"monitor-{monitor_id}"
    assert captured["proxy_url"] == "http://workspace-proxy:8080"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_fetch_monitor_uses_monitor_proxy_override_and_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>in stock add to cart $19.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'target', 'https://example.com/item', 20, 'http://monitor-proxy:9090', 'session-custom-1', ?)
        """,
        (app_module.utc_now(),),
    )
    monitor = conn.execute("select * from monitors where id = ?", (cur.lastrowid,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is True
    assert captured["task_key"] == "session-custom-1"
    assert captured["proxy_url"] == "http://monitor-proxy:9090"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_fetch_monitor_uses_workspace_proxy_and_default_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>out of stock $29.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'walmart', 'https://example.com/item2', 20, null, null, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = int(cur.lastrowid)
    monitor = conn.execute("select * from monitors where id = ?", (monitor_id,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is False
    assert captured["task_key"] == f"monitor-{monitor_id}"
    assert captured["proxy_url"] == "http://workspace-proxy:8080"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_fetch_monitor_uses_monitor_proxy_override_and_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>in stock add to cart $19.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'target', 'https://example.com/item', 20, 'http://monitor-proxy:9090', 'session-custom-1', ?)
        """,
        (app_module.utc_now(),),
    )
    monitor = conn.execute("select * from monitors where id = ?", (cur.lastrowid,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is True
    assert captured["task_key"] == "session-custom-1"
    assert captured["proxy_url"] == "http://monitor-proxy:9090"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_fetch_monitor_uses_workspace_proxy_and_default_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>out of stock $29.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'walmart', 'https://example.com/item2', 20, null, null, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = int(cur.lastrowid)
    monitor = conn.execute("select * from monitors where id = ?", (monitor_id,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is False
    assert captured["task_key"] == f"monitor-{monitor_id}"
    assert captured["proxy_url"] == "http://workspace-proxy:8080"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_fetch_monitor_uses_monitor_proxy_override_and_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>in stock add to cart $19.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'target', 'https://example.com/item', 20, 'http://monitor-proxy:9090', 'session-custom-1', ?)
        """,
        (app_module.utc_now(),),
    )
    monitor = conn.execute("select * from monitors where id = ?", (cur.lastrowid,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is True
    assert captured["task_key"] == "session-custom-1"
    assert captured["proxy_url"] == "http://monitor-proxy:9090"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_fetch_monitor_uses_workspace_proxy_and_default_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>out of stock $29.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'walmart', 'https://example.com/item2', 20, null, null, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = int(cur.lastrowid)
    monitor = conn.execute("select * from monitors where id = ?", (monitor_id,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is False
    assert captured["task_key"] == f"monitor-{monitor_id}"
    assert captured["proxy_url"] == "http://workspace-proxy:8080"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_fetch_monitor_uses_monitor_proxy_override_and_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>in stock add to cart $19.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'target', 'https://example.com/item', 20, 'http://monitor-proxy:9090', 'session-custom-1', ?)
        """,
        (app_module.utc_now(),),
    )
    monitor = conn.execute("select * from monitors where id = ?", (cur.lastrowid,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is True
    assert captured["task_key"] == "session-custom-1"
    assert captured["proxy_url"] == "http://monitor-proxy:9090"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_fetch_monitor_uses_workspace_proxy_and_default_session_task_key(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    class DummyResponse:
        status_code = 200
        text = "<html><title>Item</title><body>out of stock $29.99</body></html>"

        @staticmethod
        def raise_for_status():
            return None

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    captured = {}

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute("update workspaces set proxy_url = ? where id = 1", ("http://workspace-proxy:8080",))
    cur = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, proxy_url, session_task_key, created_at
        ) values (1, 'walmart', 'https://example.com/item2', 20, null, null, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = int(cur.lastrowid)
    monitor = conn.execute("select * from monitors where id = ?", (monitor_id,)).fetchone()
    conn.commit()
    conn.close()

    result = app_module.fetch_monitor(monitor)

    assert result.in_stock is False
    assert captured["task_key"] == f"monitor-{monitor_id}"
    assert captured["proxy_url"] == "http://workspace-proxy:8080"
    assert captured["retry_total"] == 2
    assert captured["backoff_factor"] == 0.35


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_init_db_creates_auth_tables_and_is_idempotent(tmp_path, monkeypatch):
    db_path = tmp_path / "auth.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_USER_EMAIL", "owner@example.test")
    monkeypatch.setenv("DEFAULT_USER_NAME", "Owner User")
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "seed-token")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type='table' and name in ('users', 'workspace_members')"
        ).fetchall()
    }
    users_count = conn.execute("select count(*) from users").fetchone()[0]
    members_count = conn.execute("select count(*) from workspace_members").fetchone()[0]
    conn.close()

    assert tables == {"users", "workspace_members"}
    assert users_count == 1
    assert members_count == 1


def test_api_routes_require_auth(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/monitors")

    assert resp.status_code == 401
    assert resp.get_json() == {"error": "Unauthorized"}


def test_api_routes_accept_x_api_token_without_bearer(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/monitors", headers={"X-API-Token": "test-token"})

    assert resp.status_code == 200


def test_bearer_auth_is_checked_before_x_api_token(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get(
        "/api/monitors",
        headers={"Authorization": "Bearer wrong-token", "X-API-Token": "test-token"},
    )

    assert resp.status_code == 401
    assert resp.get_json() == {"error": "Unauthorized"}


def test_captcha_valid_token_allows_request(tmp_path, monkeypatch):
    monkeypatch.setenv("CAPTCHA_SECRET_KEY", "captcha-secret")
    monkeypatch.setenv("CAPTCHA_VERIFY_URL", "https://captcha.example/verify")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    class DummyCaptchaResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"success": True}

    def fake_captcha_post(url, data, timeout):
        assert url == "https://captcha.example/verify"
        assert data["secret"] == "captcha-secret"
        assert data["response"] == "valid-token"
        assert timeout == app_module.CAPTCHA_VERIFY_TIMEOUT_SECONDS
        return DummyCaptchaResponse()

    monkeypatch.setattr(app_module.requests, "post", fake_captcha_post)

    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
        },
        headers={**_auth_headers(), "X-CAPTCHA-Token": "valid-token"},
    )

    assert resp.status_code == 201


def test_captcha_invalid_or_missing_token_rejected(tmp_path, monkeypatch):
    monkeypatch.setenv("CAPTCHA_SECRET_KEY", "captcha-secret")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    class DummyCaptchaRejectResponse:
        status_code = 200

        @staticmethod
        def json():
            return {"success": False}

    def fake_captcha_post(url, data, timeout):
        return DummyCaptchaRejectResponse()

    monkeypatch.setattr(app_module.requests, "post", fake_captcha_post)

    invalid_resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
        },
        headers={**_auth_headers(), "X-CAPTCHA-Token": "invalid-token"},
    )
    missing_resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )

    assert invalid_resp.status_code == 403
    assert invalid_resp.get_json()["error"] == "CAPTCHA verification failed"
    assert invalid_resp.get_json()["reason"] == "provider_rejected"
    assert missing_resp.status_code == 403
    assert missing_resp.get_json()["error"] == "CAPTCHA verification failed"
    assert missing_resp.get_json()["reason"] == "missing_token"


def test_captcha_provider_errors_fail_safely(tmp_path, monkeypatch):
    monkeypatch.setenv("CAPTCHA_SECRET_KEY", "captcha-secret")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    def fake_captcha_post(url, data, timeout):
        raise app_module.requests.RequestException("provider unavailable")

    monkeypatch.setattr(app_module.requests, "post", fake_captcha_post)

    resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/product",
            "poll_interval_seconds": 20,
        },
        headers={**_auth_headers(), "X-CAPTCHA-Token": "valid-token"},
    )

    assert resp.status_code == 403
    assert resp.get_json()["error"] == "CAPTCHA verification failed"
    assert resp.get_json()["reason"] == "provider_request_failed"
def test_create_checkout_task_and_read_state_endpoint(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_monitor_resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://www.walmart.com/ip/sku",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    monitor = create_monitor_resp.get_json()
    assert create_monitor_resp.status_code == 201
    assert create_monitor_resp.status_code == 201
    monitor_id = create_monitor_resp.get_json()["id"]

    create_resp = client.post(
        "/api/checkout/tasks",
        json={
            "monitor_id": monitor["id"],
            "task_config": {
                "retailer": "walmart",
                "product_url": "https://www.walmart.com/ip/sku",
                "profile": "profile-main",
                "account": "acc-primary",
                "payment": "visa-4242",
            },
            "monitor_id": monitor_id,
            "task_name": "Smoke task",
            "task_config": {"profile": "profile-main", "account": "acc-primary", "payment": "visa-4242"},
        },
        headers=_auth_headers(),
    )
    assert create_resp.status_code == 201
    created = create_resp.get_json()
    assert created["current_state"] == "queued"
    task_id = created["id"]
    assert created["monitor_id"] == monitor_id
    assert created["current_state"] == "queued"

    list_resp = client.get("/api/checkout/tasks", headers=_auth_headers())
    assert list_resp.status_code == 200
    tasks = list_resp.get_json()
    assert len(tasks) == 1
    assert tasks[0]["id"] == task_id
    assert tasks[0]["current_state"] == "queued"

    attempts_resp = client.get(f"/api/checkout/tasks/{task_id}/attempts", headers=_auth_headers())
    assert attempts_resp.status_code == 200
    assert attempts_resp.get_json() == []
    state_resp = client.get(f"/api/checkout/tasks/{task_id}/state", headers=_auth_headers())
    assert state_resp.status_code == 200
    state_payload = state_resp.get_json()
    assert state_payload["task_id"] == task_id
    assert state_payload["current_state"] == "queued"
    assert state_payload["last_attempt"] is not None

    attempts_with_created_resp = client.get(
        f"/api/checkout/tasks/{task_id}/attempts?include_created=1",
        headers=_auth_headers(),
    )
    assert attempts_with_created_resp.status_code == 200
    attempts_with_created = attempts_with_created_resp.get_json()
    assert len(attempts_with_created) == 1
    assert attempts_with_created[0]["step"] == "created"


def test_checkout_task_lifecycle_start_pause_stop(tmp_path, monkeypatch):
    monkeypatch.setenv("TASK_STEP_DELAY_SECONDS", "0.01")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_monitor_resp = client.post(
        "/api/monitors",
        json={
            "retailer": "target",
            "product_url": "https://www.target.com/p/abc",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    monitor = create_monitor_resp.get_json()
    assert create_monitor_resp.status_code == 201

    create_resp = client.post(
        "/api/checkout/tasks",
        json={
            "monitor_id": monitor["id"],
            "task_config": {
                "retailer": "target",
                "product_url": "https://www.target.com/p/abc",
                "profile": "default",
                "account": "acct-1",
                "payment": "amex",
            },
        },
        headers=_auth_headers(),
    )
    assert create_resp.status_code == 201
    task_id = create_resp.get_json()["id"]

    start_resp = client.post(f"/api/checkout/tasks/{task_id}/start", headers=_auth_headers())
    assert start_resp.status_code == 200
    assert start_resp.get_json()["task"]["current_state"] == "monitoring"

    started_task = start_resp.get_json()["task"]
    assert started_task["current_state"] == "monitoring"

    assert start_resp.get_json()["task"]["current_state"] == "monitoring"

    pause_resp = client.post(f"/api/checkout/tasks/{task_id}/pause", headers=_auth_headers())
    assert pause_resp.status_code == 200
    assert pause_resp.get_json()["task"]["current_state"] == "paused"


    pause_resp = client.post(f"/api/checkout/tasks/{task_id}/pause", headers=_auth_headers())
    assert pause_resp.status_code == 200
    assert pause_resp.get_json()["task"]["current_state"] == "paused"

    stop_resp = client.post(f"/api/checkout/tasks/{task_id}/stop", headers=_auth_headers())
    assert stop_resp.status_code == 200
    assert stop_resp.get_json()["task"]["current_state"] == "stopped"


def test_stripe_webhook_valid_signature_accepted(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    event = {
        "id": "evt_valid_1",
        "type": "customer.subscription.created",
        "data": {
            "object": {
                "id": "sub_123",
                "customer": "cus_123",
                "status": "active",
                "cancel_at_period_end": False,
                "current_period_end": int(time.time()) + 86400,
                "metadata": {"workspace_id": "1"},
                "plan": {"id": "pro_monthly", "interval": "month"},
                "items": {"data": [{"price": {"lookup_key": "pro-monthly"}}]},
            }
        },
    }
    payload = json.dumps(event)
    signature = _stripe_signature(payload, "whsec_test")

    resp = client.post(
        "/api/billing/stripe/webhook",
        data=payload,
        headers={"Stripe-Signature": signature, "Content-Type": "application/json"},
    )

    assert resp.status_code == 200
    assert resp.get_json() == {"ok": True, "noop": False}

    conn = app_module.db()
    processed_events = conn.execute("select count(*) as c from billing_webhook_events").fetchone()["c"]
    subscription = conn.execute(
        "select provider_subscription_id, status from billing_subscriptions where workspace_id = 1"
    ).fetchone()
    conn.close()

    assert processed_events == 1
    assert subscription["provider_subscription_id"] == "sub_123"
    assert subscription["status"] == "active"


def test_stripe_webhook_invalid_signature_rejected(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    payload = json.dumps({"id": "evt_invalid_1", "type": "customer.subscription.updated", "data": {"object": {}}})

    resp = client.post(
        "/api/billing/stripe/webhook",
        data=payload,
        headers={"Stripe-Signature": "t=1,v1=bad", "Content-Type": "application/json"},
    )

    assert resp.status_code == 401
    conn = app_module.db()
    processed_events = conn.execute("select count(*) as c from billing_webhook_events").fetchone()["c"]
    conn.close()
    assert processed_events == 0


def test_stripe_webhook_duplicate_event_is_noop(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    event = {
        "id": "evt_dupe_1",
        "type": "customer.subscription.updated",
        "data": {
            "object": {
                "id": "sub_dupe",
                "customer": "cus_dupe",
                "status": "active",
                "cancel_at_period_end": False,
                "current_period_end": int(time.time()) + 3600,
                "metadata": {"workspace_id": "1"},
                "plan": {"id": "team_monthly", "interval": "month"},
                "items": {"data": [{"price": {"lookup_key": "team-monthly"}}]},
            }
        },
    }
    payload = json.dumps(event)
    signature = _stripe_signature(payload, "whsec_test")

    first = client.post(
        "/api/billing/stripe/webhook",
        data=payload,
        headers={"Stripe-Signature": signature, "Content-Type": "application/json"},
    )
    second = client.post(
        "/api/billing/stripe/webhook",
        data=payload,
        headers={"Stripe-Signature": signature, "Content-Type": "application/json"},
    )

    assert first.status_code == 200
    assert second.status_code == 200
    assert second.get_json() == {"ok": True, "noop": True}

    conn = app_module.db()
    processed_events = conn.execute("select count(*) as c from billing_webhook_events").fetchone()["c"]
    subscriptions = conn.execute(
        "select count(*) as c from billing_subscriptions where provider_subscription_id = 'sub_dupe'"
    ).fetchone()["c"]
    conn.close()

    assert processed_events == 1
    assert subscriptions == 1


def test_api_routes_allow_authenticated_requests_and_include_context(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    resp = client.get("/api/workspace", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["workspace"]["id"] == 1
    assert payload["user"]["id"] == 1


def test_check_update_reports_update_available_when_upstream_is_newer(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_VERSION", "1.2.3")
    monkeypatch.setenv("UPDATE_CHECK_URL", "https://updates.example.com/latest")
    monkeypatch.setenv("UPDATE_CHECK_TIMEOUT_SECONDS", "1.5")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    class DummyResponse:
        headers = {"Content-Type": "application/json"}

        def raise_for_status(self):
            return None

        def json(self):
            return {"latest_version": "1.2.4"}

    captured = {}

    class FakeReqResult:
        def __init__(self, response):
            self.response = response
            self.error = None
            self.telemetry = None

    def fake_request(**kwargs):
        captured["url"] = kwargs["url"]
        captured["timeout"] = kwargs["timeout"]
        return FakeReqResult(DummyResponse())

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    resp = client.get("/api/meta/check-update", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["current_version"] == "1.2.3"
    assert payload["latest_version"] == "1.2.4"
    assert payload["update_available"] is True
    assert "source_error" not in payload
    assert captured == {"url": "https://updates.example.com/latest", "timeout": 1.5}


def test_check_update_reports_no_update_when_versions_match(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_VERSION", "2.0.0")
    monkeypatch.setenv("UPDATE_CHECK_URL", "https://updates.example.com/latest")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    class DummyResponse:
        headers = {"Content-Type": "application/json"}

        def raise_for_status(self):
            return None

        def json(self):
            return {"latest_version": "2.0.0"}

    class FakeReqResult:
        def __init__(self, response):
            self.response = response
            self.error = None
            self.telemetry = None

    monkeypatch.setattr(
        app_module,
        "perform_request",
        lambda **kwargs: FakeReqResult(DummyResponse()),
    )

    resp = client.get("/api/meta/check-update", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["current_version"] == "2.0.0"
    assert payload["latest_version"] == "2.0.0"
    assert payload["update_available"] is False
    assert "source_error" not in payload


def test_check_update_returns_fallback_payload_on_upstream_failure(tmp_path, monkeypatch):
    monkeypatch.setenv("APP_VERSION", "3.1.0")
    monkeypatch.setenv("UPDATE_CHECK_URL", "https://updates.example.com/latest")
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    class FakeReqResult:
        def __init__(self, error):
            self.response = None
            self.error = error
            self.telemetry = None

    monkeypatch.setattr(
        app_module,
        "perform_request",
        lambda **kwargs: FakeReqResult(app_module.requests.RequestException("connection timeout")),
    )

    resp = client.get("/api/meta/check-update", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["current_version"] == "3.1.0"
    assert payload["latest_version"] == "3.1.0"
    assert payload["update_available"] is False
    assert "source_error" in payload


def test_ops_metrics_returns_expected_schema_and_non_negative_counts(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    checked_monitor_id = conn.execute(
        """
        insert into monitors(
            workspace_id, retailer, product_url, poll_interval_seconds, enabled, last_checked_at, failure_streak, created_at
        ) values (1, 'walmart', 'https://example.com/checked', 20, 1, ?, 1, ?)
        """,
        (app_module.utc_now(), app_module.utc_now()),
    ).lastrowid
    conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values (1, 'Main', 'https://discord.com/api/webhooks/main', ?)
        """,
        (app_module.utc_now(),),
    )
    webhook_id = conn.execute("select id from webhooks where name = 'Main'").fetchone()["id"]
    conn.execute(
        """
        insert into events(monitor_id, event_type, title, product_url, retailer, price_cents, event_time, dedupe_key)
        values (?, 'in_stock', 'seed', 'https://example.com/checked', 'walmart', 1200, ?, 'metrics-event-1')
        """,
        (checked_monitor_id, app_module.utc_now()),
    )
    event_id = conn.execute("select id from events where dedupe_key = 'metrics-event-1'").fetchone()["id"]
    conn.execute(
        """
        insert into deliveries(event_id, webhook_id, status, response_code, response_body, delivered_at)
        values
        (?, ?, 'sent', 204, '', ?),
        (?, ?, 'failed', 500, 'oops', ?)
        """,
        (event_id, webhook_id, app_module.utc_now(), event_id, webhook_id, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/ops/metrics", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert set(payload.keys()) == {
        "checks_total",
        "checks_failed_total",
        "alerts_created_total",
        "webhook_sent_total",
        "webhook_failed_total",
    }
    assert all(isinstance(payload[key], int) for key in payload)
    assert all(payload[key] >= 0 for key in payload)
    assert payload["checks_total"] == 1
    assert payload["checks_failed_total"] == 1
    assert payload["alerts_created_total"] == 1
    assert payload["webhook_sent_total"] == 1
    assert payload["webhook_failed_total"] == 1
def test_webhook_test_endpoint_uses_shared_request_helper(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    captured = {}

    class DummyResponse:
        status_code = 204
        text = ""

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    cur = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, created_at)
        values (1, 'Main', 'https://discord.com/api/webhooks/test-endpoint', ?)
        """,
        (app_module.utc_now(),),
    )
    webhook_id = int(cur.lastrowid)
    conn.commit()
    conn.close()

    resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert payload["ok"] is True
    assert captured["method"] == "POST"
    assert captured["url"] == "https://discord.com/api/webhooks/test-endpoint"
    assert captured["task_key"] == f"webhook-test-{webhook_id}"


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


def test_monitor_failure_trends_includes_zero_counts_and_excludes_other_workspaces(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    now = datetime.now(timezone.utc)

    conn = app_module.db()
    own_monitor_with_failures = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/with-failures', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    own_monitor_without_failures = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'target', 'https://example.com/no-failures', 20, ?)
        """,
        (app_module.utc_now(),),
    ).lastrowid
    conn.execute(
        """
        insert into monitor_failures(monitor_id, workspace_id, error_text, failed_at)
        values (?, 1, 'err-1', ?), (?, 1, 'err-2', ?)
        """,
        (
            own_monitor_with_failures,
            (now - timedelta(hours=3)).isoformat(),
            own_monitor_with_failures,
            (now - timedelta(days=2)).isoformat(),
        ),
    )

    conn.execute(
        "insert into workspaces(name, plan, created_at) values ('Other', 'basic', ?)",
        (app_module.utc_now(),),
    )
    other_workspace = conn.execute("select id from workspaces where name = 'Other'").fetchone()["id"]
    other_monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (?, 'bestbuy', 'https://example.com/other', 20, ?)
        """,
        (other_workspace, app_module.utc_now()),
    ).lastrowid
    conn.execute(
        """
        insert into monitor_failures(monitor_id, workspace_id, error_text, failed_at)
        values (?, ?, 'other-err', ?)
        """,
        (other_monitor_id, other_workspace, (now - timedelta(hours=1)).isoformat()),
    )
    conn.commit()
    conn.close()

    resp = client.get("/api/ops/monitor-failure-trends", headers=_auth_headers())
    payload = resp.get_json()

    assert resp.status_code == 200
    assert set(payload.keys()) == {"trends"}
    trends = sorted(payload["trends"], key=lambda row: row["monitor_id"])
    assert trends == [
        {"monitor_id": own_monitor_with_failures, "failures_last_24h": 1, "failures_last_7d": 2},
        {"monitor_id": own_monitor_without_failures, "failures_last_24h": 0, "failures_last_7d": 0},
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


def test_adapter_dispatch_uses_walmart_and_fallback(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    walmart_adapter = app_module.get_adapter_for_retailer("walmart")
    target_adapter = app_module.get_adapter_for_retailer("target")
    bestbuy_adapter = app_module.get_adapter_for_retailer("bestbuy")
    fallback_adapter = app_module.get_adapter_for_retailer("unknown-retailer")

    assert walmart_adapter.name == "walmart"
    assert target_adapter.name == "target"
    assert bestbuy_adapter.name == "bestbuy"
    assert fallback_adapter.name == "default"


def test_adapter_dispatch_supports_pokemon_alias_and_canonical_name(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    canonical = app_module.get_adapter_for_retailer("pokemoncenter")
    hyphenated = app_module.get_adapter_for_retailer("pokemon-center")
    underscored = app_module.get_adapter_for_retailer("pokemon_center")

    assert canonical.name == "pokemoncenter"
    assert hyphenated.name == "pokemoncenter"
    assert underscored.name == "pokemoncenter"


def test_adapter_dispatch_supports_walmart_aliases(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    canonical = app_module.get_adapter_for_retailer("walmart")
    hyphenated = app_module.get_adapter_for_retailer("wal-mart")
    spaced = app_module.get_adapter_for_retailer("wal mart")

    assert canonical.name == "walmart"
    assert hyphenated.name == "walmart"
    assert spaced.name == "walmart"


def test_adapter_dispatch_supports_target_aliases(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    canonical = app_module.get_adapter_for_retailer("target")
    dotted = app_module.get_adapter_for_retailer("target.com")
    spaced = app_module.get_adapter_for_retailer("target com")

    assert canonical.name == "target"
    assert dotted.name == "target"
    assert spaced.name == "target"


def test_adapter_dispatch_supports_bestbuy_aliases(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)

    canonical = app_module.get_adapter_for_retailer("bestbuy")
    hyphenated = app_module.get_adapter_for_retailer("best-buy")
    spaced = app_module.get_adapter_for_retailer("best buy")
    dotted = app_module.get_adapter_for_retailer("bestbuy.com")

    assert canonical.name == "bestbuy"
    assert hyphenated.name == "bestbuy"
    assert spaced.name == "bestbuy"
    assert dotted.name == "bestbuy"


def test_parse_monitor_html_dispatches_and_keeps_default_fallback(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    walmart_html = load_fixture_html("walmart", "in_stock")
    unknown_html = load_fixture_html("target", "unknown_markup")

    walmart_result = app_module.parse_monitor_html(html=walmart_html, retailer="walmart")
    fallback_result = app_module.parse_monitor_html(html=unknown_html, retailer="unknown-retailer")
    default_result = app_module.default_parser(unknown_html)

    assert walmart_result.availability_reason == "walmart_marker_in_stock"
    assert walmart_result.in_stock is True
    assert fallback_result.in_stock == default_result.in_stock
    assert fallback_result.status_text == default_result.status_text
    assert fallback_result.availability_reason == default_result.availability_reason


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


def test_walmart_parser_uses_default_fallback_for_unknown_markup_fixture(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    unknown_html = load_fixture_html("walmart", "unknown_markup")

    walmart_result = app_module.evaluate_page(unknown_html, retailer="walmart")
    default_result = app_module.default_parser(unknown_html)

    assert walmart_result.in_stock == default_result.in_stock
    assert walmart_result.status_text == default_result.status_text
    assert walmart_result.availability_reason == default_result.availability_reason
    assert walmart_result.parser_confidence == default_result.parser_confidence


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


def test_target_parser_uses_default_fallback_for_unknown_markup_fixture(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    unknown_html = load_fixture_html("target", "unknown_markup")

    target_result = app_module.evaluate_page(unknown_html, retailer="target")
    default_result = app_module.default_parser(unknown_html)

    assert target_result.in_stock == default_result.in_stock
    assert target_result.status_text == default_result.status_text
    assert target_result.availability_reason == default_result.availability_reason
    assert target_result.parser_confidence == default_result.parser_confidence


def test_bestbuy_parser_uses_default_fallback_for_unknown_markup_fixture(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    unknown_html = load_fixture_html("bestbuy", "unknown_markup")

    bestbuy_result = app_module.evaluate_page(unknown_html, retailer="bestbuy")
    default_result = app_module.default_parser(unknown_html)

    assert bestbuy_result.in_stock == default_result.in_stock
    assert bestbuy_result.status_text == default_result.status_text
    assert bestbuy_result.availability_reason == default_result.availability_reason
    assert bestbuy_result.parser_confidence == default_result.parser_confidence


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


def test_check_monitor_notify_failures_do_not_clobber_persisted_state(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    monitor_id = _seed_monitor(app_module)
    expected = app_module.MonitorResult(
        in_stock=True,
        price_cents=1999,
        title="Pokemon Product",
        status_text="in_stock",
        availability_reason="marker_in_stock",
        parser_confidence=0.91,
        keyword_matched=True,
    )

    monkeypatch.setattr(app_module, "fetch_monitor", lambda monitor: expected)
    monkeypatch.setattr(app_module, "create_event_and_deliver", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("notify down")))

    resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["eligible_for_alert"] is True

    conn = app_module.db()
    row = conn.execute("select failure_streak, last_error, last_price_cents, last_in_stock from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    assert row["failure_streak"] == 0
    assert row["last_error"] is None
    assert row["last_price_cents"] == 1999
    assert row["last_in_stock"] == 1


def test_check_monitor_notify_failures_do_not_clobber_persisted_state(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    monitor_id = _seed_monitor(app_module)
    expected = app_module.MonitorResult(
        in_stock=True,
        price_cents=1999,
        title="Pokemon Product",
        status_text="in_stock",
        availability_reason="marker_in_stock",
        parser_confidence=0.91,
        keyword_matched=True,
    )

    monkeypatch.setattr(app_module, "fetch_monitor", lambda monitor: expected)
    monkeypatch.setattr(app_module, "create_event_and_deliver", lambda *_args, **_kwargs: (_ for _ in ()).throw(RuntimeError("notify down")))

    resp = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    payload = resp.get_json()
    assert resp.status_code == 200
    assert payload["ok"] is True
    assert payload["eligible_for_alert"] is True

    conn = app_module.db()
    row = conn.execute("select failure_streak, last_error, last_price_cents, last_in_stock from monitors where id = ?", (monitor_id,)).fetchone()
    conn.close()
    assert row["failure_streak"] == 0
    assert row["last_error"] is None
    assert row["last_price_cents"] == 1999
    assert row["last_in_stock"] == 1


def test_billing_sync_upgrade_relaxes_plan_limits(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute(
        """
        insert into billing_customers(workspace_id, user_id, provider, provider_customer_id, created_at, updated_at)
        values (1, 1, 'stripe', 'cus_upgrade', ?, ?)
        """,
        (app_module.utc_now(), app_module.utc_now()),
    )
    for idx in range(20):
        conn.execute(
            """
            insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
            values (1, 'target', ?, 20, ?)
            """,
            (f"https://example.com/basic-{idx}", app_module.utc_now()),
        )
    conn.commit()
    conn.close()

    blocked = client.post(
        "/api/monitors",
        json={
            "retailer": "target",
            "product_url": "https://example.com/blocked-upgrade",
            "poll_interval_seconds": 10,
        },
        headers=_auth_headers(),
    )
    assert blocked.status_code == 400

    sync = client.post(
        "/api/billing/subscription-events",
        json={
            "provider": "stripe",
            "provider_subscription_id": "sub_upgrade",
            "provider_customer_id": "cus_upgrade",
            "status": "active",
            "plan_code": "price_pro_monthly",
            "plan_lookup_key": "pro",
            "cancel_at_period_end": False,
        },
        headers=_auth_headers(),
    )
    assert sync.status_code == 200

    upgraded = client.post(
        "/api/monitors",
        json={
            "retailer": "target",
            "product_url": "https://example.com/after-upgrade",
            "poll_interval_seconds": 10,
        },
        headers=_auth_headers(),
    )
    assert upgraded.status_code == 201


def test_billing_sync_canceled_subscription_enforces_stricter_limits(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    conn.execute("update workspaces set plan = 'pro' where id = 1")
    conn.execute(
        """
        insert into billing_customers(workspace_id, user_id, provider, provider_customer_id, created_at, updated_at)
        values (1, 1, 'stripe', 'cus_cancel', ?, ?)
        """,
        (app_module.utc_now(), app_module.utc_now()),
    )
    for idx in range(18):
        conn.execute(
            """
            insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
            values (1, 'walmart', ?, 20, ?)
            """,
            (f"https://example.com/pro-{idx}", app_module.utc_now()),
        )
    conn.commit()
    conn.close()

    allowed_before_cancel = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/before-cancel",
            "poll_interval_seconds": 10,
        },
        headers=_auth_headers(),
    )
    assert allowed_before_cancel.status_code == 201

    sync = client.post(
        "/api/billing/subscription-events",
        json={
            "provider": "stripe",
            "provider_subscription_id": "sub_cancel",
            "provider_customer_id": "cus_cancel",
            "status": "canceled",
            "plan_code": "price_pro_monthly",
            "plan_lookup_key": "pro",
            "cancel_at_period_end": True,
            "source": "billing_subscriptions",
        },
        headers=_auth_headers(),
    )
    assert sync.status_code == 200

    conn = app_module.db()
    workspace = conn.execute("select * from workspaces where id = 1").fetchone()
    conn.close()
    assert workspace["plan"] == "basic"
    assert workspace["subscription_status"] == "canceled"
    assert workspace["subscription_source"] == "billing_subscriptions"

    blocked_poll = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/after-cancel-poll",
            "poll_interval_seconds": 10,
        },
        headers=_auth_headers(),
    )
    assert blocked_poll.status_code == 400
    assert "minimum poll interval is 20 seconds" in blocked_poll.get_json()["error"]

    allowed_at_basic_limit = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/after-cancel-allowed",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    assert allowed_at_basic_limit.status_code == 201

    blocked_monitor_count = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/after-cancel-count",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    assert blocked_monitor_count.status_code == 400
    assert "Plan limit reached (20 monitors)" in blocked_monitor_count.get_json()["error"]


def test_init_db_creates_checkout_tables(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    conn = app_module.db()
    tables = {
        row["name"]
        for row in conn.execute(
            "select name from sqlite_master where type = 'table' and name in ('checkout_tasks', 'checkout_attempts', 'task_logs')"
        ).fetchall()
    }
    conn.close()
    assert tables == {"checkout_tasks", "checkout_attempts", "task_logs"}


def test_init_db_creates_proxy_tables(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    conn = app_module.db()
    tables = {
        row["name"]
        for row in conn.execute(
            "select name from sqlite_master where type = 'table' and name in ('proxies', 'proxy_leases')"
        ).fetchall()
    }
    monitor_columns = {row["name"] for row in conn.execute("pragma table_info(monitors)").fetchall()}
    conn.close()

    assert tables == {"proxies", "proxy_leases"}
    assert {"proxy_type", "proxy_region", "proxy_residential_only", "proxy_sticky_session_seconds"}.issubset(
        monitor_columns
    )


def test_check_monitor_enqueues_checkout_task_for_eligible_stock(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_resp = client.post(
        "/api/monitors",
        json={
            "retailer": "target",
            "product_url": "https://example.com/item",
            "keyword": "pokemon",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    monitor = create_resp.get_json()
    assert create_resp.status_code == 201
    account_resp = client.post(
        "/api/accounts",
        json={
            "retailer": "target",
            "username": "target-user",
            "encrypted_credential_ref": "cred-ref",
            "proxy_url": "http://proxy-1.local:8080",
        },
        headers=_auth_headers(),
    )
    assert account_resp.status_code == 201
    binding_resp = client.post(
        "/api/task-profile-bindings",
        json={"monitor_id": monitor["id"], "retailer_account_id": account_resp.get_json()["id"]},
        headers=_auth_headers(),
    )
    assert binding_resp.status_code == 201

    def fake_fetch(_monitor):
        return app_module.MonitorResult(
            in_stock=True,
            price_cents=2499,
            title="Pokemon Test Item",
            status_text="in_stock",
            keyword_matched=True,
        )

    monkeypatch.setattr(app_module, "fetch_monitor", fake_fetch)
    check_resp = client.post(f"/api/monitors/{monitor['id']}/check", headers=_auth_headers())
    assert check_resp.status_code == 200
    assert check_resp.get_json()["eligible_for_alert"] is True

    conn = app_module.db()
    task = conn.execute("select * from checkout_tasks where monitor_id = ?", (monitor_id,)).fetchone()
    attempt_count = conn.execute(
        "select count(*) as c from checkout_attempts where task_id = ?",
        (task["id"],),
    ).fetchone()["c"]
    log_count = conn.execute(
        "select count(*) as c from task_logs where task_id = ?",
        (task["id"],),
    ).fetchone()["c"]
    conn.close()

    assert task is not None
    assert task["current_state"] == "queued"
    assert attempt_count >= 2
    assert log_count >= 2


def test_queue_detected_moves_waiting_for_queue_tasks_to_monitoring(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    conn = app_module.db()
    now = app_module.utc_now()
    monitor_id = conn.execute(
        """
        insert into monitors(workspace_id, retailer, category, product_url, poll_interval_seconds, enabled, created_at)
        values (1, 'pokemoncenter', 'pokemon', 'https://www.pokemoncenter.com/product/test', 20, 1, ?)
        """,
        (now,),
    ).lastrowid
    task = app_module.create_checkout_task(
        conn,
        workspace_id=1,
        monitor_id=monitor_id,
        task_name='Queue waiting task',
        task_config={"retailer": "pokemoncenter", "wait_for_queue": True, "queue_entry_delay_ms": 0},
        initial_state='waiting_for_queue',
    )
    conn.commit()
    conn.close()

    def fake_fetch(_monitor):
        return app_module.MonitorResult(
            in_stock=False,
            price_cents=None,
            title="Pokemon Center Queue",
            status_text="queue_detected",
            availability_reason="pokemoncenter_queue_detected",
            queue_detected=True,
        )

    monkeypatch.setattr(app_module, "fetch_monitor", fake_fetch)

    first_check = client.post(f"/api/monitors/{monitor_id}/check", headers=_auth_headers())
    assert first_check.status_code == 200

    conn = app_module.db()
    updated = conn.execute("select * from checkout_tasks where id = ?", (task["id"],)).fetchone()
    conn.close()
    assert updated is not None
    assert updated["current_state"] == "monitoring"


def test_start_now_override_endpoint_transitions_waiting_tasks(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_monitor_resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/item",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    monitor = create_monitor_resp.get_json()
    assert create_monitor_resp.status_code == 201

    account_resp = client.post(
        "/api/accounts",
        json={"retailer": "walmart", "username": "w-user", "encrypted_credential_ref": "cred-ref"},
        headers=_auth_headers(),
    )
    assert account_resp.status_code == 201

    binding_resp = client.post(
        "/api/task-profile-bindings",
        json={"monitor_id": monitor["id"], "retailer_account_id": account_resp.get_json()["id"]},
        headers=_auth_headers(),
    )
    assert binding_resp.status_code == 201

    create_task_resp = client.post(
        "/api/checkout/tasks",
        json={"monitor_id": monitor["id"], "initial_state": "waiting_for_queue", "task_name": "Waiting task"},
        headers=_auth_headers(),
    )
    assert create_task_resp.status_code == 201
    task = create_task_resp.get_json()

    override_resp = client.post(
        "/api/checkout/tasks/start-now",
        json={"task_ids": [task["id"]]},
        headers=_auth_headers(),
    )
    assert override_resp.status_code == 200
    payload = override_resp.get_json()
    assert payload["ok"] is True
    assert payload["tasks"][0]["current_state"] == "monitoring"

def test_checkout_task_lifecycle_routes(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_monitor_resp = client.post(
        "/api/monitors",
        json={
            "retailer": "walmart",
            "product_url": "https://example.com/item",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    monitor = create_monitor_resp.get_json()
    assert create_monitor_resp.status_code == 201
    account_resp = client.post(
        "/api/accounts",
        json={"retailer": "walmart", "username": "w-user", "encrypted_credential_ref": "cred-ref"},
        headers=_auth_headers(),
    )
    assert account_resp.status_code == 201
    binding_resp = client.post(
        "/api/task-profile-bindings",
        json={"monitor_id": monitor["id"], "retailer_account_id": account_resp.get_json()["id"]},
        headers=_auth_headers(),
    )
    assert binding_resp.status_code == 201

    create_task_resp = client.post(
        "/api/checkout/tasks",
        json={"monitor_id": monitor["id"], "task_name": "Checkout 1"},
        headers=_auth_headers(),
    )
    assert create_task_resp.status_code == 201
    task = create_task_resp.get_json()

    start_resp = client.post(f"/api/checkout/tasks/{task['id']}/start", headers=_auth_headers())
    assert start_resp.status_code == 200
    assert start_resp.get_json()["task"]["current_state"] == "monitoring"

    pause_resp = client.post(f"/api/checkout/tasks/{task['id']}/pause", headers=_auth_headers())
    assert pause_resp.status_code == 200
    assert pause_resp.get_json()["task"]["current_state"] == "paused"

    stop_resp = client.post(f"/api/checkout/tasks/{task['id']}/stop", headers=_auth_headers())
    assert stop_resp.status_code == 200
    assert stop_resp.get_json()["task"]["current_state"] == "stopped"

    state_resp = client.get(f"/api/checkout/tasks/{task['id']}/state", headers=_auth_headers())
    assert state_resp.status_code == 200
    payload = state_resp.get_json()
    assert payload["current_state"] == "stopped"
    assert payload["last_error"] is None


def test_checkout_run_transitions_all_steps_and_marks_success(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_monitor_resp = client.post(
        "/api/monitors",
        json={"retailer": "walmart", "product_url": "https://example.com/item-run", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    )
    monitor = create_monitor_resp.get_json()
    assert create_monitor_resp.status_code == 201

    create_task_resp = client.post(
        "/api/checkout/tasks",
        json={
            "monitor_id": monitor["id"],
            "task_name": "Checkout Run",
            "task_config": {
                "retailer": "walmart",
                "product_url": "https://example.com/item-run",
                "profile": "default-profile",
                "account": "acc-primary",
                "payment": "visa-4242",
            },
        },
        headers=_auth_headers(),
    )
    task = create_task_resp.get_json()
    run_resp = client.post(f"/api/checkout/tasks/{task['id']}/run", headers=_auth_headers())
    assert run_resp.status_code == 200
    assert run_resp.get_json()["task"]["current_state"] == "success"

    conn = app_module.db()
    steps = {
        row["state"]
        for row in conn.execute(
            "select state from checkout_attempts where task_id = ? and status = 'step_success'",
            (task["id"],),
        ).fetchall()
    }
    log_events = {
        row["event_type"]
        for row in conn.execute("select event_type from task_logs where task_id = ?", (task["id"],)).fetchall()
    }
    conn.close()

    assert steps == {"monitoring", "carting", "shipping", "payment", "submitting"}
    assert "state_transition" in log_events
    assert "step_success" in log_events


def test_checkout_run_retries_payment_and_terminal_failure_taxonomy(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_monitor_resp = client.post(
        "/api/monitors",
        json={"retailer": "target", "product_url": "https://example.com/item-fail", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    )
    monitor = create_monitor_resp.get_json()
    conn = app_module.db()
    now = app_module.utc_now()
    profile_id = int(
        conn.execute(
            """
            insert into checkout_profiles(workspace_id, name, email, phone, shipping_address_json, billing_address_json, created_at, updated_at)
            values (1, 'profile-a', 'a@example.com', '5551112222', ?, ?, ?, ?)
            """,
            (json.dumps({"line1": "1 Main"}), json.dumps({"line1": "1 Main"}), now, now),
        ).lastrowid
    )
    account_id = int(
        conn.execute(
            """
            insert into retailer_accounts(workspace_id, retailer, username, email, encrypted_credential_ref, session_status, created_at, updated_at)
            values (1, 'target', 'bound-user', 'bound@example.com', 'enc-ref', 'active', ?, ?)
            """,
            (now, now),
        ).lastrowid
    )
    payment_id = int(
        conn.execute(
            """
            insert into payment_methods(workspace_id, label, provider, token_reference, billing_profile_id, created_at, updated_at)
            values (1, 'visa-bound', 'stripe', 'tok_test', ?, ?, ?)
            """,
            (profile_id, now, now),
        ).lastrowid
    )
    conn.execute(
        """
        insert into task_profile_bindings(workspace_id, monitor_id, checkout_profile_id, retailer_account_id, payment_method_id, created_at, updated_at)
        values (1, ?, ?, ?, ?, ?, ?)
        """,
        (monitor["id"], profile_id, account_id, payment_id, now, now),
    )
    conn.commit()
    conn.close()

    create_task_resp = client.post(
        "/api/checkout/tasks",
        json={
            "monitor_id": monitor["id"],
            "task_name": "Checkout Fail",
            "task_config": {
                "retailer": "target",
                "product_url": "https://example.com/item-fail",
                "profile": "default-profile",
                "account": "acc-primary",
                "payment": "visa-4242",
                "simulate_fail_step": "payment",
                "simulate_fail_times": 5,
                "simulate_retryable": True,
            },
        },
        headers=_auth_headers(),
    )
    task = create_task_resp.get_json()

    run_resp = client.post(f"/api/checkout/tasks/{task['id']}/run", headers=_auth_headers())
    payload = run_resp.get_json()
    assert run_resp.status_code == 200
    assert payload["task"]["current_state"] == "failed"
    assert "terminal_max_attempts_exceeded" in (payload["task"]["last_error"] or "")

    conn = app_module.db()
    payment_failures = conn.execute(
        """
        select count(*) as c
        from checkout_attempts
        where task_id = ? and state = 'payment' and status = 'step_failure'
        """,
        (task["id"],),
    ).fetchone()["c"]
    conn.close()
    assert payment_failures == app_module.CHECKOUT_STEP_RETRY_POLICY["payment"]["max_attempts"]


def test_checkout_attempts_include_status_signal_and_hint_for_antibot(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    monitor = client.post(
        "/api/monitors",
        json={"retailer": "target", "product_url": "https://example.com/item-antibot", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    ).get_json()
    conn = app_module.db()
    now = app_module.utc_now()
    profile_id = int(
        conn.execute(
            """
            insert into checkout_profiles(workspace_id, name, email, phone, shipping_address_json, billing_address_json, created_at, updated_at)
            values (1, 'profile-a', 'a@example.com', '5551112222', ?, ?, ?, ?)
            """,
            (json.dumps({"line1": "1 Main"}), json.dumps({"line1": "1 Main"}), now, now),
        ).lastrowid
    )
    account_id = int(
        conn.execute(
            """
            insert into retailer_accounts(workspace_id, retailer, username, email, encrypted_credential_ref, session_status, created_at, updated_at)
            values (1, 'target', 'bound-user', 'bound@example.com', 'enc-ref', 'active', ?, ?)
            """,
            (now, now),
        ).lastrowid
    )
    payment_id = int(
        conn.execute(
            """
            insert into payment_methods(workspace_id, label, provider, token_reference, billing_profile_id, created_at, updated_at)
            values (1, 'visa-bound', 'stripe', 'tok_test', ?, ?, ?)
            """,
            (profile_id, now, now),
        ).lastrowid
    )
    conn.execute(
        """
        insert into task_profile_bindings(workspace_id, monitor_id, checkout_profile_id, retailer_account_id, payment_method_id, created_at, updated_at)
        values (1, ?, ?, ?, ?, ?, ?)
        """,
        (monitor["id"], profile_id, account_id, payment_id, now, now),
    )
    conn.commit()
    conn.close()
    task = client.post(
        "/api/checkout/tasks",
        json={
            "monitor_id": monitor["id"],
            "task_name": "Checkout DataDome",
            "task_config": {
                "retailer": "target",
                "profile": "default-profile",
                "account": "acc-primary",
                "payment": "visa-4242",
                "simulate_fail_step": "payment",
                "simulate_fail_times": 1,
                "simulate_retryable": True,
                "simulate_fail_error": "DataDome challenge detected",
            },
        },
        headers=_auth_headers(),
    ).get_json()

    run_resp = client.post(f"/api/checkout/tasks/{task['id']}/run", headers=_auth_headers())
    assert run_resp.status_code == 200

    attempts_resp = client.get(f"/api/checkout/tasks/{task['id']}/attempts", headers=_auth_headers())
    assert attempts_resp.status_code == 200
    attempts = attempts_resp.get_json()
    payment_failure = next(a for a in attempts if a["state"] == "payment" and a["step"] == "step_failure")
    assert payment_failure["details"]["status_signal"] == "antibot_datadome_challenge"
    assert payment_failure["details"]["status_hint"] == "likely proxy reputation issue"
    assert payment_failure["details"]["failure_reason"]["failure_class"] == "antibot"


def test_checkout_antibot_group_limit_cooldown_short_circuits_retries(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    monitor = client.post(
        "/api/monitors",
        json={"retailer": "target", "product_url": "https://example.com/item-anti-limit", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    ).get_json()
    conn = app_module.db()
    now = app_module.utc_now()
    profile_id = int(
        conn.execute(
            """
            insert into checkout_profiles(workspace_id, name, email, phone, shipping_address_json, billing_address_json, created_at, updated_at)
            values (1, 'profile-a', 'a@example.com', '5551112222', ?, ?, ?, ?)
            """,
            (json.dumps({"line1": "1 Main"}), json.dumps({"line1": "1 Main"}), now, now),
        ).lastrowid
    )
    account_id = int(
        conn.execute(
            """
            insert into retailer_accounts(workspace_id, retailer, username, email, encrypted_credential_ref, session_status, created_at, updated_at)
            values (1, 'target', 'bound-user', 'bound@example.com', 'enc-ref', 'active', ?, ?)
            """,
            (now, now),
        ).lastrowid
    )
    payment_id = int(
        conn.execute(
            """
            insert into payment_methods(workspace_id, label, provider, token_reference, billing_profile_id, created_at, updated_at)
            values (1, 'visa-bound', 'stripe', 'tok_test', ?, ?, ?)
            """,
            (profile_id, now, now),
        ).lastrowid
    )
    conn.execute(
        """
        insert into task_profile_bindings(workspace_id, monitor_id, checkout_profile_id, retailer_account_id, payment_method_id, created_at, updated_at)
        values (1, ?, ?, ?, ?, ?, ?)
        """,
        (monitor["id"], profile_id, account_id, payment_id, now, now),
    )
    conn.commit()
    conn.close()
    task = client.post(
        "/api/checkout/tasks",
        json={
            "monitor_id": monitor["id"],
            "task_name": "Checkout Anti Limit",
            "task_config": {
                "retailer": "target",
                "profile": "default-profile",
                "account": "acc-primary",
                "payment": "visa-4242",
                "simulate_fail_step": "payment",
                "simulate_fail_times": 5,
                "simulate_retryable": True,
                "simulate_fail_error": "Incapsula challenge page",
                "group_limits": {"max_retries": 4, "antibot_event_threshold": 1, "antibot_cooldown_seconds": 30},
            },
        },
        headers=_auth_headers(),
    ).get_json()

    run_resp = client.post(f"/api/checkout/tasks/{task['id']}/run", headers=_auth_headers())
    payload = run_resp.get_json()
    assert run_resp.status_code == 200
    assert payload["task"]["current_state"] == "failed"
    assert "antibot cooldown active" in (payload["task"]["last_error"] or "")


def test_checkout_run_uses_task_profile_bindings_for_execution_context(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_monitor_resp = client.post(
        "/api/monitors",
        json={"retailer": "target", "product_url": "https://example.com/item-binding", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    )
    monitor = create_monitor_resp.get_json()

    create_task_resp = client.post(
        "/api/checkout/tasks",
        json={
            "monitor_id": monitor["id"],
            "task_name": "Checkout Bound",
            "task_config": {"retailer": "target", "product_url": "https://example.com/item-binding"},
        },
        headers=_auth_headers(),
    )
    task = create_task_resp.get_json()

    conn = app_module.db()
    cur = conn.execute(
        """
        insert into checkout_profiles(workspace_id, name, email, phone, shipping_address_json, billing_address_json, created_at, updated_at)
        values (1, 'profile-bound', 'profile@example.com', '5551112222', ?, ?, ?, ?)
        """,
        (
            json.dumps({"line1": "1 Main", "city": "Austin", "state": "TX", "postal_code": "78701", "country": "US"}),
            json.dumps({"line1": "1 Main", "city": "Austin", "state": "TX", "postal_code": "78701", "country": "US"}),
            app_module.utc_now(),
            app_module.utc_now(),
        ),
    )
    profile_id = int(cur.lastrowid)
    cur = conn.execute(
        """
        insert into retailer_accounts(workspace_id, retailer, username, email, encrypted_credential_ref, session_status, created_at, updated_at)
        values (1, 'target', 'bound-user', 'bound@example.com', 'enc-ref', 'active', ?, ?)
        """,
        (app_module.utc_now(), app_module.utc_now()),
    )
    account_id = int(cur.lastrowid)
    cur = conn.execute(
        """
        insert into payment_methods(workspace_id, label, provider, token_reference, billing_profile_id, created_at, updated_at)
        values (1, 'visa-bound', 'stripe', 'tok_test', ?, ?, ?)
        """,
        (profile_id, app_module.utc_now(), app_module.utc_now()),
    )
    payment_id = int(cur.lastrowid)
    conn.execute(
        """
        insert into task_profile_bindings(workspace_id, monitor_id, checkout_profile_id, retailer_account_id, payment_method_id, created_at, updated_at)
        values (1, ?, ?, ?, ?, ?, ?)
        """,
        (monitor["id"], profile_id, account_id, payment_id, app_module.utc_now(), app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    run_resp = client.post(f"/api/checkout/tasks/{task['id']}/run", headers=_auth_headers())
    assert run_resp.status_code == 200
    assert run_resp.get_json()["task"]["current_state"] == "success"


def test_checkout_run_fails_fast_with_actionable_binding_error(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    create_monitor_resp = client.post(
        "/api/monitors",
        json={"retailer": "bestbuy", "product_url": "https://example.com/item-binding-fail", "poll_interval_seconds": 20},
        headers=_auth_headers(),
    )
    monitor = create_monitor_resp.get_json()

    create_task_resp = client.post(
        "/api/checkout/tasks",
        json={
            "monitor_id": monitor["id"],
            "task_name": "Checkout Bound Fail",
            "task_config": {"retailer": "bestbuy", "product_url": "https://example.com/item-binding-fail"},
        },
        headers=_auth_headers(),
    )
    task = create_task_resp.get_json()

    conn = app_module.db()
    cur = conn.execute(
        """
        insert into checkout_profiles(workspace_id, name, email, phone, shipping_address_json, billing_address_json, created_at, updated_at)
        values (1, 'profile-bound', 'profile@example.com', '5551112222', ?, ?, ?, ?)
        """,
        (
            json.dumps({"line1": "1 Main", "city": "Austin", "state": "TX", "postal_code": "78701", "country": "US"}),
            json.dumps({"line1": "1 Main", "city": "Austin", "state": "TX", "postal_code": "78701", "country": "US"}),
            app_module.utc_now(),
            app_module.utc_now(),
        ),
    )
    profile_id = int(cur.lastrowid)
    cur = conn.execute(
        """
        insert into retailer_accounts(workspace_id, retailer, username, email, encrypted_credential_ref, session_status, created_at, updated_at)
        values (1, 'bestbuy', 'bound-user', 'bound@example.com', 'enc-ref', 'active', ?, ?)
        """,
        (app_module.utc_now(), app_module.utc_now()),
    )
    account_id = int(cur.lastrowid)
    conn.execute(
        """
        insert into task_profile_bindings(workspace_id, monitor_id, checkout_profile_id, retailer_account_id, payment_method_id, created_at, updated_at)
        values (1, ?, ?, ?, null, ?, ?)
        """,
        (monitor["id"], profile_id, account_id, app_module.utc_now(), app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    run_resp = client.post(f"/api/checkout/tasks/{task['id']}/run", headers=_auth_headers())
    payload = run_resp.get_json()
    assert run_resp.status_code == 200
    assert payload["task"]["current_state"] == "failed"
    assert payload["task"]["last_error"] == "binding_payment_missing"


def test_queue_enqueues_single_active_job_per_monitor(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    conn = app_module.db()
    cur = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, enabled, created_at)
        values (1, 'target', 'https://example.com/queue-one', 20, 1, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor = conn.execute("select * from monitors where id = ?", (cur.lastrowid,)).fetchone()
    queue = app_module.SQLiteJobQueue(conn, worker_id="test-worker")
    now_iso = app_module.utc_now()

    queue.enqueue_monitor_check_if_due(monitor, now_iso=now_iso)
    queue.enqueue_monitor_check_if_due(monitor, now_iso=now_iso)
    conn.commit()

    count = conn.execute(
        "select count(*) as c from jobs where monitor_id = ? and status in ('queued', 'retrying', 'running')",
        (monitor["id"],),
    ).fetchone()["c"]
    conn.close()

    assert count == 1


def test_queue_claim_due_job_recovers_from_stale_lock(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    conn = app_module.db()
    now_iso = app_module.utc_now()
    stale_locked_at = (
        datetime.fromtimestamp(
            datetime.now(timezone.utc).timestamp() - app_module.WORKER_LOCK_TIMEOUT_SECONDS - 5,
            tz=timezone.utc,
        ).isoformat()
    )
    cur = conn.execute(
        """
        insert into jobs(job_type, monitor_id, status, attempt_count, next_run_at, locked_by, locked_at, payload_json, created_at, updated_at)
        values ('monitor_check', null, 'retrying', 1, ?, 'old-worker', ?, '{}', ?, ?)
        """,
        (now_iso, stale_locked_at, now_iso, now_iso),
    )
    conn.commit()
    queue = app_module.SQLiteJobQueue(conn, worker_id="new-worker")
    claimed = queue.claim_due_job(now_iso=now_iso)
    row = conn.execute("select status, locked_by from jobs where id = ?", (cur.lastrowid,)).fetchone()
    conn.close()

    assert claimed is not None
    assert claimed.id == cur.lastrowid
    assert row["status"] == "running"
    assert row["locked_by"] == "new-worker"


def test_execute_monitor_job_transitions_retrying_and_failed(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    conn = app_module.db()
    cur = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, enabled, created_at)
        values (1, 'walmart', 'https://example.com/retry', 20, 1, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = int(cur.lastrowid)

    def fail_fetch(_monitor):
        raise app_module.requests.RequestException("upstream timeout")

    monkeypatch.setattr(app_module, "fetch_monitor", fail_fetch)

    queue = app_module.SQLiteJobQueue(conn, worker_id="retry-worker")
    now_iso = app_module.utc_now()
    conn.execute(
        """
        insert into jobs(job_type, monitor_id, status, attempt_count, next_run_at, payload_json, created_at, updated_at)
        values ('monitor_check', ?, 'queued', 0, ?, ?, ?, ?)
        """,
        (monitor_id, now_iso, json.dumps({"step_attempts": {}}), now_iso, now_iso),
    )
    conn.execute(
        """
        insert into jobs(job_type, monitor_id, status, attempt_count, next_run_at, payload_json, created_at, updated_at)
        values ('monitor_check', ?, 'queued', 0, ?, ?, ?, ?)
        """,
        (monitor_id, now_iso, json.dumps({"step_attempts": {"fetch": 4}}), now_iso, now_iso),
    )
    conn.commit()

    first = queue.claim_due_job(now_iso=now_iso)
    assert first is not None
    app_module.execute_monitor_job(queue, first, now_iso=now_iso)
    conn.commit()

    second = queue.claim_due_job(now_iso=now_iso)
    assert second is not None
    app_module.execute_monitor_job(queue, second, now_iso=now_iso)
    conn.commit()

    rows = conn.execute("select id, status, payload_json from jobs order by id asc").fetchall()
    conn.close()

    assert rows[0]["status"] == "retrying"
    assert json.loads(rows[0]["payload_json"])["step_attempts"]["fetch"] == 1
    assert rows[1]["status"] == "failed"


def test_execute_monitor_job_completes_on_success(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    conn = app_module.db()
    cur = conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, enabled, created_at)
        values (1, 'bestbuy', 'https://example.com/success', 20, 1, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor_id = int(cur.lastrowid)

    monkeypatch.setattr(
        app_module,
        "fetch_monitor",
        lambda _monitor: app_module.MonitorResult(
            in_stock=False,
            price_cents=1299,
            title="Success Item",
            status_text="out_or_unknown",
        ),
    )

    queue = app_module.SQLiteJobQueue(conn, worker_id="success-worker")
    now_iso = app_module.utc_now()
    conn.execute(
        """
        insert into jobs(job_type, monitor_id, status, attempt_count, next_run_at, payload_json, created_at, updated_at)
        values ('monitor_check', ?, 'queued', 0, ?, ?, ?, ?)
        """,
        (monitor_id, now_iso, json.dumps({"step_attempts": {}}), now_iso, now_iso),
    )
    conn.commit()

    job = queue.claim_due_job(now_iso=now_iso)
    assert job is not None
    app_module.execute_monitor_job(queue, job, now_iso=now_iso)
    conn.commit()
    row = conn.execute("select status from jobs where id = ?", (job.id,)).fetchone()
    conn.close()

    assert row["status"] == "completed"


def test_worker_loop_idles_when_no_jobs_available(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    sleep_calls = {"count": 0}

    def fake_sleep(_seconds):
        sleep_calls["count"] += 1
        app_module.worker_running = False

    monkeypatch.setattr(app_module.time, "sleep", fake_sleep)
    app_module.worker_running = True
    app_module.worker_loop()

    assert sleep_calls["count"] == 1


def test_webhook_test_endpoint_resolves_secret_backed_webhook_url(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()
    captured = {}
    resolved_url = "https://discord.com/api/webhooks/secret-test-endpoint"

    class DummyResponse:
        status_code = 204
        text = ""

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    secret_id = app_module.create_secret(conn, 1, "webhook_url", resolved_url, 1)
    redacted_url = app_module.redact_webhook_url(resolved_url)
    cur = conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, webhook_secret_id, created_at)
        values (1, 'Secret Main', ?, ?, ?)
        """,
        (redacted_url, secret_id, app_module.utc_now()),
    )
    webhook_id = int(cur.lastrowid)
    conn.commit()
    conn.close()

    test_resp = client.post(f"/api/webhooks/{webhook_id}/test", headers=_auth_headers())
    list_resp = client.get("/api/webhooks", headers=_auth_headers())

    assert test_resp.status_code == 200
    assert captured["url"] == resolved_url
    assert list_resp.status_code == 200
    assert list_resp.get_json()[0]["webhook_url"] == redacted_url


def test_create_event_and_deliver_resolves_secret_backed_webhook_url(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    captured = {}
    resolved_url = "https://discord.com/api/webhooks/secret-delivery"

    class DummyResponse:
        status_code = 204
        text = ""
        ok = True

    class FakeReqResult:
        def __init__(self):
            self.response = DummyResponse()
            self.error = None
            self.telemetry = None

    def fake_request(**kwargs):
        captured.update(kwargs)
        return FakeReqResult()

    monkeypatch.setattr(app_module, "perform_request", fake_request)

    conn = app_module.db()
    conn.execute(
        """
        insert into monitors(workspace_id, retailer, product_url, poll_interval_seconds, created_at)
        values (1, 'walmart', 'https://example.com/secret-monitor', 20, ?)
        """,
        (app_module.utc_now(),),
    )
    monitor = conn.execute("select * from monitors order by id desc limit 1").fetchone()
    secret_id = app_module.create_secret(conn, 1, "webhook_url", resolved_url, 1)
    conn.execute(
        """
        insert into webhooks(workspace_id, name, webhook_url, webhook_secret_id, created_at)
        values (1, 'Secret Delivery', ?, ?, ?)
        """,
        (app_module.redact_webhook_url(resolved_url), secret_id, app_module.utc_now()),
    )
    conn.commit()
    conn.close()

    result = app_module.MonitorResult(
        in_stock=True,
        price_cents=1999,
        title="Pokemon Secret Product",
        status_text="in_stock",
        keyword_matched=True,
    )
    app_module.create_event_and_deliver(monitor, result, eligible=True)

    assert captured["task_key"].startswith("webhook-")
    assert captured["url"] == resolved_url


def test_monitor_assist_apply_updates_monitor_and_logs_task_history(tmp_path, monkeypatch):
    app_module = _load_app(tmp_path, monkeypatch)
    client = app_module.app.test_client()

    monitor_resp = client.post(
        "/api/monitors",
        json={
            "retailer": "pokemoncenter",
            "product_url": "12-34567-111",
            "poll_interval_seconds": 20,
        },
        headers=_auth_headers(),
    )
    monitor_id = monitor_resp.get_json()["id"]

    create_task_resp = client.post(
        "/api/checkout/tasks",
        json={"monitor_id": monitor_id, "task_config": {"profile": "p1", "account": "a1", "payment": "pm1"}},
        headers=_auth_headers(),
    )
    task_id = create_task_resp.get_json()["id"]

    apply_resp = client.post(
        "/api/monitors/monitor-assist/apply",
        json={"monitor_ids": [monitor_id], "pid": "12-34567-890"},
        headers=_auth_headers(),
    )
    assert apply_resp.status_code == 200
    payload = apply_resp.get_json()
    assert payload["updated_monitors"] == 1
    assert payload["updated_tasks"] == 1
    assert payload["product_url"] == "https://www.pokemoncenter.com/product/12-34567-890"

    conn = sqlite3.connect(tmp_path / "test.db")
    conn.row_factory = sqlite3.Row
    monitor_row = conn.execute("select product_url from monitors where id = ?", (monitor_id,)).fetchone()
    attempt_row = conn.execute(
        """
        select step, error_text
        from checkout_attempts
        where task_id = ?
        order by id desc
        limit 1
        """,
        (task_id,),
    ).fetchone()
    conn.close()

    assert monitor_row["product_url"] == "https://www.pokemoncenter.com/product/12-34567-890"
    assert attempt_row["step"] == "PID updated from monitor assist"
    assert attempt_row["error_text"] == "PID updated from monitor assist"
