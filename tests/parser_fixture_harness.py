from pathlib import Path

import pytest

FIXTURES = Path(__file__).parent / "fixtures"


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


PARSER_FIXTURE_CASES = [
    fixture_case("walmart", "pokemon", "in_stock", True, "in_stock"),
    fixture_case("walmart", "pokemon", "out_of_stock", False, "out_or_unknown"),
    fixture_case("walmart", "pokemon", "ambiguous", False, "out_or_unknown"),
    fixture_case("target", "pokemon", "in_stock", True, "in_stock"),
    fixture_case("target", "pokemon", "out_of_stock", False, "out_or_unknown"),
    fixture_case("target", "pokemon", "ambiguous", False, "out_or_unknown"),
    fixture_case("target", "sports_cards", "in_stock", True, "in_stock"),
    fixture_case("target", "sports_cards", "out_of_stock", False, "out_or_unknown"),
    fixture_case("target", "one_piece", "in_stock", True, "in_stock"),
    fixture_case("target", "one_piece", "out_of_stock", False, "out_or_unknown"),
    fixture_case("target", "lorcana", "in_stock", True, "in_stock"),
    fixture_case("target", "lorcana", "out_of_stock", False, "out_or_unknown"),
    fixture_case("bestbuy", "pokemon", "in_stock", True, "in_stock"),
    fixture_case("bestbuy", "pokemon", "out_of_stock", False, "out_or_unknown"),
    fixture_case("bestbuy", "pokemon", "ambiguous", False, "out_or_unknown"),
    fixture_case("pokemoncenter", "pokemon", "in_stock", True, "in_stock"),
    fixture_case("pokemoncenter", "pokemon", "out_of_stock", False, "out_or_unknown"),
    fixture_case("pokemoncenter", "pokemon", "ambiguous", False, "out_or_unknown"),
    fixture_case("pokemoncenter", "sports_cards", "in_stock", True, "in_stock"),
    fixture_case("pokemoncenter", "sports_cards", "out_of_stock", False, "out_or_unknown"),
    fixture_case("pokemoncenter", "one_piece", "in_stock", True, "in_stock"),
    fixture_case("pokemoncenter", "one_piece", "out_of_stock", False, "out_or_unknown"),
    fixture_case("pokemoncenter", "lorcana", "in_stock", True, "in_stock"),
    fixture_case("pokemoncenter", "lorcana", "out_of_stock", False, "out_or_unknown"),
]


def load_fixture_html(retailer: str, fixture_name: str, category: str = "pokemon") -> str:
    fixture_path = FIXTURES / retailer / category / f"{fixture_name}.html"
    if not fixture_path.exists():
        fixture_path = FIXTURES / retailer / f"{fixture_name}.html"
    return fixture_path.read_text(encoding="utf-8")
