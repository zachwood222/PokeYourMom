import base64
import hashlib
import hmac
import importlib
import sys

from pathlib import Path

import pytest
from cryptography.fernet import Fernet

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


def _legacy_encrypt(secret_key: str, plaintext: str) -> str:
    nonce = bytes(range(16))
    payload = plaintext.encode("utf-8")
    stream = bytearray()
    counter = 0
    key_bytes = secret_key.encode("utf-8")
    while len(stream) < len(payload):
        block = hashlib.sha256(key_bytes + nonce + counter.to_bytes(8, "big")).digest()
        stream.extend(block)
        counter += 1
    cipher = bytes(a ^ b for a, b in zip(payload, bytes(stream[: len(payload)])))
    mac = hmac.new(key_bytes, nonce + cipher, hashlib.sha256).digest()
    return base64.urlsafe_b64encode(nonce + cipher + mac).decode("ascii")


def test_secret_encrypt_decrypt_roundtrip(tmp_path, monkeypatch):
    key = Fernet.generate_key().decode("ascii")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", key)
    app_module = _load_app(tmp_path, monkeypatch)

    ciphertext, key_version = app_module.encrypt_secret_value_with_version("s3cr3t-value")

    assert key_version == app_module.ACTIVE_SECRET_KEY_VERSION
    assert app_module.decrypt_secret_value(ciphertext, key_version=key_version) == "s3cr3t-value"


def test_secret_tamper_detection(tmp_path, monkeypatch):
    key = Fernet.generate_key().decode("ascii")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", key)
    app_module = _load_app(tmp_path, monkeypatch)

    ciphertext, key_version = app_module.encrypt_secret_value_with_version("dont-tamper")
    tampered = ciphertext[:-1] + ("A" if ciphertext[-1] != "A" else "B")

    with pytest.raises(ValueError):
        app_module.decrypt_secret_value(tampered, key_version=key_version)


def test_secret_backward_compatibility_migrates_legacy_ciphertext(tmp_path, monkeypatch):
    legacy_key = "legacy-secret-key"
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", legacy_key)
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY_VERSION", "v1")
    app_module = _load_app(tmp_path, monkeypatch)

    legacy_cipher = _legacy_encrypt(legacy_key, "legacy-value")
    now_iso = app_module.utc_now()

    with app_module.db() as conn:
        cur = conn.execute(
            """
            insert into account_secrets(workspace_id, user_id, secret_type, ciphertext, key_version, created_at, updated_at)
            values (?, ?, ?, ?, ?, ?, ?)
            """,
            (1, 1, "webhook_url", legacy_cipher, None, now_iso, now_iso),
        )
        secret_id = int(cur.lastrowid)
        conn.commit()

        plaintext = app_module.get_secret_plaintext(
            conn,
            workspace_id=1,
            secret_id=secret_id,
            allowed_types={"webhook_url"},
        )
        row = conn.execute("select ciphertext, key_version from account_secrets where id = ?", (secret_id,)).fetchone()

    assert plaintext == "legacy-value"
    assert row["key_version"] == app_module.ACTIVE_SECRET_KEY_VERSION
    assert row["ciphertext"] != legacy_cipher


def test_secret_key_rotation_reencrypts_with_active_version(tmp_path, monkeypatch):
    v1 = Fernet.generate_key().decode("ascii")
    v2 = Fernet.generate_key().decode("ascii")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY_VERSION", "v1")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEYS", f"v1:{v1},v2:{v2}")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", v1)
    app_v1 = _load_app(tmp_path, monkeypatch)

    with app_v1.db() as conn:
        secret_id = app_v1.create_secret(conn, 1, "webhook_url", "rotate-me", 1)
        conn.commit()
        before = conn.execute("select key_version, ciphertext from account_secrets where id = ?", (secret_id,)).fetchone()

    monkeypatch.setenv("SECRET_ENCRYPTION_KEY_VERSION", "v2")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEYS", f"v1:{v1},v2:{v2}")
    monkeypatch.setenv("SECRET_ENCRYPTION_KEY", v2)
    app_v2 = _load_app(tmp_path, monkeypatch)

    with app_v2.db() as conn:
        plaintext = app_v2.get_secret_plaintext(
            conn,
            workspace_id=1,
            secret_id=secret_id,
            allowed_types={"webhook_url"},
        )
        after = conn.execute("select key_version, ciphertext from account_secrets where id = ?", (secret_id,)).fetchone()

    assert before["key_version"] == "v1"
    assert plaintext == "rotate-me"
    assert after["key_version"] == "v2"
    assert after["ciphertext"] != before["ciphertext"]
