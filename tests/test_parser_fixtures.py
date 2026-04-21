import pytest

from parser_fixture_harness import PARSER_FIXTURE_CASES, load_fixture_html
from test_app import _load_app


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

    result = app_module.evaluate_page(html, retailer=retailer, category=category)

    assert result.in_stock is expected_in_stock
    assert result.status_text == expected_status
