from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


def fixture_case(retailer: str, fixture_name: str, expected_in_stock: bool, expected_status: str):
    return pytest.param(
        retailer,
        fixture_name,
        expected_in_stock,
        expected_status,
        id=f"{retailer}:{fixture_name}",
    )


PARSER_FIXTURE_CASES = [
    fixture_case("walmart", "in_stock", True, "in_stock"),
    fixture_case("walmart", "out_of_stock", False, "out_or_unknown"),
    fixture_case("walmart", "ambiguous", False, "out_or_unknown"),
    fixture_case("target", "in_stock", True, "in_stock"),
    fixture_case("target", "out_of_stock", False, "out_or_unknown"),
    fixture_case("target", "ambiguous", False, "out_or_unknown"),
    fixture_case("bestbuy", "in_stock", True, "in_stock"),
    fixture_case("bestbuy", "out_of_stock", False, "out_or_unknown"),
    fixture_case("bestbuy", "ambiguous", False, "out_or_unknown"),
    fixture_case("pokemoncenter", "in_stock", True, "in_stock"),
    fixture_case("pokemoncenter", "out_of_stock", False, "out_or_unknown"),
    fixture_case("pokemoncenter", "ambiguous", False, "out_or_unknown"),
]


def load_fixture_html(retailer: str, fixture_name: str) -> str:
    fixture_path = FIXTURES / retailer / f"{fixture_name}.html"
    return fixture_path.read_text(encoding="utf-8")
