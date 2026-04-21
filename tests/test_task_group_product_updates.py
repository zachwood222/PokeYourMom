import importlib.util
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parents[1]
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))


def _load_app_module(tmp_path, monkeypatch):
    db_path = tmp_path / "task-group-products.db"
    monkeypatch.setenv("DB_PATH", str(db_path))
    module_name = "app_task_group_products_test"
    if module_name in sys.modules:
        del sys.modules[module_name]
    spec = importlib.util.spec_from_file_location(module_name, ROOT / "app.py")
    module = importlib.util.module_from_spec(spec)
    assert spec and spec.loader
    sys.modules[module_name] = module
    spec.loader.exec_module(module)
    module.init_db()
    return module


def test_quick_edit_replaces_first_and_appends_remaining(tmp_path, monkeypatch):
    app_module = _load_app_module(tmp_path, monkeypatch)
    config = {
        "retailer": "pokemoncenter",
        "products": [{"pid": "11-11111-111", "quantity": 1, "skip_if_oos": False}],
    }
    updated = app_module.apply_product_group_operation(
        config,
        {"mode": "edit", "input": "22-22222-222:2,33-33333-333:3"},
    )
    assert [p["pid"] for p in updated["products"]] == ["22-22222-222", "33-33333-333"]
    assert [p["quantity"] for p in updated["products"]] == [2, 3]


def test_quick_edit_rejected_when_existing_products_are_multiple(tmp_path, monkeypatch):
    app_module = _load_app_module(tmp_path, monkeypatch)
    config = {
        "retailer": "pokemoncenter",
        "products": [
            {"pid": "11-11111-111", "quantity": 1, "skip_if_oos": False},
            {"pid": "22-22222-222", "quantity": 1, "skip_if_oos": False},
        ],
    }
    try:
        app_module.apply_product_group_operation(
            config,
            {"mode": "edit", "input": "33-33333-333:2,44-44444-444:4"},
        )
        assert False, "Expected quick edit to be rejected when existing product count is not 1."
    except ValueError as exc:
        assert "Quick Edit is only allowed" in str(exc)


def test_remove_and_skip_toggle_updates_rows(tmp_path, monkeypatch):
    app_module = _load_app_module(tmp_path, monkeypatch)
    config = {
        "retailer": "pokemoncenter",
        "products": [
            {"pid": "11-11111-111", "quantity": 1, "skip_if_oos": False},
            {"pid": "22-22222-222", "quantity": 2, "skip_if_oos": False},
        ],
    }
    updated = app_module.apply_product_group_operation(
        config,
        {"mode": "remove", "remove_indices": [0], "skip_updates": [{"index": 0, "skip_if_oos": True}]},
    )
    assert len(updated["products"]) == 1
    assert updated["products"][0]["pid"] == "22-22222-222"
    assert updated["products"][0]["skip_if_oos"] is True
