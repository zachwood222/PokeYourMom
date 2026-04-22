from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"
REQUIRED_FIXTURE_NAMES = ("in_stock", "out_of_stock", "ambiguous")


def fixture_case(
    retailer: str,
    category: str,
    fixture_name: str,
    expected_in_stock: bool,
    expected_status: str,
):
    return pytest.param(
        retailer,
        category,
        fixture_name,
        expected_in_stock,
        expected_status,
        id=f"{retailer}:{category}:{fixture_name}",
    )


PARSER_FIXTURE_EXPECTATIONS = {
    "walmart": {
        "in_stock": (True, "in_stock"),
        "out_of_stock": (False, "out_or_unknown"),
        "ambiguous": (False, "out_or_unknown"),
    },
    "target": {
        "in_stock": (True, "in_stock"),
        "out_of_stock": (False, "out_or_unknown"),
        "ambiguous": (False, "out_or_unknown"),
    },
    "bestbuy": {
        "in_stock": (True, "in_stock"),
        "out_of_stock": (False, "out_or_unknown"),
        "ambiguous": (False, "out_or_unknown"),
    },
    "pokemoncenter": {
        "in_stock": (True, "in_stock"),
        "out_of_stock": (False, "out_or_unknown"),
        "ambiguous": (False, "out_or_unknown"),
    },
}


PARSER_FIXTURE_CASES = [
    fixture_case(retailer, "default", fixture_name, expected_in_stock, expected_status)
    for retailer, fixtures in PARSER_FIXTURE_EXPECTATIONS.items()
    for fixture_name, (expected_in_stock, expected_status) in fixtures.items()
]


def load_fixture_html(retailer: str, fixture_name: str, *, category: str = "default") -> str:
    category_normalized = (category or "default").strip().lower()
    if category_normalized in {"", "default"}:
        fixture_path = FIXTURES / retailer / f"{fixture_name}.html"
    else:
        fixture_path = FIXTURES / retailer / category_normalized / f"{fixture_name}.html"
    if not fixture_path.exists():
        raise FileNotFoundError(f"Missing parser fixture: {retailer}/{fixture_name}.html")
    return fixture_path.read_text(encoding="utf-8")
