import importlib
import socket
import sqlite3
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _reload_app_with_db(tmp_path, monkeypatch):
    db_path = tmp_path / "billing-schema.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    monkeypatch.setenv("DEFAULT_BEARER_TOKEN", "test-token")
    monkeypatch.setenv("API_AUTH_TOKEN", "test-token")
    import app as app_module

    return importlib.reload(app_module), db_path


def test_init_db_creates_billing_tables_and_columns(tmp_path, monkeypatch):
    app_module, db_path = _reload_app_with_db(tmp_path, monkeypatch)

    app_module.init_db()

    conn = sqlite3.connect(db_path)
    tables = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type = 'table' and name like 'billing_%'"
        ).fetchall()
    }
    customer_columns = {
        row[1] for row in conn.execute("pragma table_info(billing_customers)").fetchall()
    }
    subscription_columns = {
        row[1] for row in conn.execute("pragma table_info(billing_subscriptions)").fetchall()
    }
    conn.close()

    assert tables == {"billing_customers", "billing_subscriptions", "billing_webhook_events"}
    assert {
        "workspace_id",
        "user_id",
        "provider",
        "provider_customer_id",
        "created_at",
        "updated_at",
    }.issubset(customer_columns)
    assert {
        "workspace_id",
        "provider",
        "provider_subscription_id",
        "billing_customer_id",
        "status",
        "current_period_end",
        "cancel_at_period_end",
        "plan_code",
        "plan_interval",
        "plan_lookup_key",
        "created_at",
        "updated_at",
    }.issubset(subscription_columns)


def test_init_db_is_idempotent_and_preserves_billing_schema(tmp_path, monkeypatch):
    app_module, db_path = _reload_app_with_db(tmp_path, monkeypatch)

    app_module.init_db()
    app_module.init_db()

    conn = sqlite3.connect(db_path)
    index_names = {
        row[0]
        for row in conn.execute(
            "select name from sqlite_master where type = 'index' and name like 'idx_billing_%'"
        ).fetchall()
    }

    customer_columns = {
        row[1] for row in conn.execute("pragma table_info(billing_customers)").fetchall()
    }
    subscription_columns = {
        row[1] for row in conn.execute("pragma table_info(billing_subscriptions)").fetchall()
    }
    conn.close()

    assert index_names == {
        "idx_billing_customers_provider_customer_id",
        "idx_billing_subscriptions_provider_subscription_id",
    }
    assert "provider_customer_id" in customer_columns
    assert "provider_subscription_id" in subscription_columns


def test_init_db_schema_only_no_network_or_stripe_side_effects(tmp_path, monkeypatch):
    app_module, _ = _reload_app_with_db(tmp_path, monkeypatch)

    monkeypatch.delitem(sys.modules, "stripe", raising=False)

    def fail_network(*args, **kwargs):
        raise AssertionError("network access is not expected for schema-only init_db()")

    monkeypatch.setattr(socket, "create_connection", fail_network)
    monkeypatch.setattr(app_module.requests, "get", fail_network)
    monkeypatch.setattr(app_module.requests, "post", fail_network)

    app_module.init_db()
    app_module.init_db()

    assert "stripe" not in sys.modules
