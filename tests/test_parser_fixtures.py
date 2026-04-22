import pytest

from parser_fixture_harness import (
    FIXTURES,
    PARSER_FIXTURE_CASES,
    PARSER_FIXTURE_EXPECTATIONS,
    REQUIRED_FIXTURE_NAMES,
    load_fixture_html,
)
from test_app import _load_app


@pytest.mark.parametrize("retailer", sorted(PARSER_FIXTURE_EXPECTATIONS.keys()))
def test_fixture_suite_contains_required_snapshots_per_retailer(retailer):
    available = {path.stem for path in (FIXTURES / retailer).glob("*.html")}
    missing = sorted(set(REQUIRED_FIXTURE_NAMES) - available)
    assert not missing, f"Missing required fixture snapshots for {retailer}: {', '.join(missing)}"


@pytest.mark.parametrize(
    "retailer,category,fixture_name,expected_in_stock,expected_status",
    PARSER_FIXTURE_CASES,
)
def test_evaluate_page_matches_fixture_expectations(
    tmp_path,
    monkeypatch,
    retailer,
    category,
    fixture_name,
    expected_in_stock,
    expected_status,
):
    app_module = _load_app(tmp_path, monkeypatch)
    html = load_fixture_html(retailer, fixture_name, category=category)

    result = app_module.evaluate_page(html, retailer=retailer)
    fixture_id = f"{retailer}/{fixture_name}.html"

    assert result.in_stock is expected_in_stock, (
        f"[{fixture_id}] expected in_stock={expected_in_stock}, got {result.in_stock}"
    )
    assert result.status_text == expected_status, (
        f"[{fixture_id}] expected status_text={expected_status}, got {result.status_text}"
    )
