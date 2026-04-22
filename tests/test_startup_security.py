import importlib
import sys
from pathlib import Path

import pytest

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def test_validate_startup_configuration_requires_secrets_in_production(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "prod-missing-secrets.db"))
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.delenv("API_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("SECRET_ENCRYPTION_KEY", raising=False)

    import app as app_module

    reloaded = importlib.reload(app_module)
    with pytest.raises(RuntimeError, match="API_AUTH_TOKEN, SECRET_ENCRYPTION_KEY"):
        reloaded.validate_startup_configuration()


def test_validate_startup_configuration_accepts_explicit_secrets_in_production(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "prod-with-secrets.db"))
    monkeypatch.setenv("APP_ENV", "production")
    monkeypatch.setenv("API_AUTH_TOKEN", "prod-token")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", "prod-secret-key")
    monkeypatch.setenv("ALLOWED_ORIGINS", "https://app.example.com,https://admin.example.com")

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.validate_startup_configuration()

    client = reloaded.app.test_client()
    assert client.get("/api/workspace", headers={"Authorization": "Bearer prod-token"}).status_code == 200
    assert client.get("/api/workspace", headers={"Authorization": "Bearer dev-token"}).status_code == 401
    assert reloaded.ALLOWED_ORIGINS == ["https://app.example.com", "https://admin.example.com"]


def test_development_defaults_allow_local_startup_without_explicit_secrets(tmp_path, monkeypatch):
    monkeypatch.setenv("DB_PATH", str(tmp_path / "dev-defaults.db"))
    monkeypatch.setenv("APP_ENV", "development")
    monkeypatch.delenv("API_AUTH_TOKEN", raising=False)
    monkeypatch.delenv("SECRET_ENCRYPTION_KEY", raising=False)
    monkeypatch.delenv("ALLOWED_ORIGINS", raising=False)

    import app as app_module

    reloaded = importlib.reload(app_module)
    reloaded.init_db()
    reloaded.validate_startup_configuration()

    client = reloaded.app.test_client()
    assert client.get("/api/workspace", headers={"Authorization": "Bearer dev-token"}).status_code == 200
    assert reloaded.API_AUTH_TOKEN == "dev-token"
    assert reloaded.SECRET_ENCRYPTION_KEY == "local-dev-secret-key"
    assert reloaded.ALLOWED_ORIGINS == "*"
