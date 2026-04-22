import pytest
from pathlib import Path
import sys

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from tasks.parsers import MonitorInputValidationError, parse_monitor_input


def test_parse_monitor_input_accepts_pokemoncenter_url():
    rows = parse_monitor_input("https://www.pokemoncenter.com/product/72-10704-101")
    assert rows == [{"pid": "72-10704-101", "quantity": 1, "skip_if_oos": False}]


def test_parse_monitor_input_accepts_pid_with_spaces():
    rows = parse_monitor_input("   72-10704-101   ")
    assert rows == [{"pid": "72-10704-101", "quantity": 1, "skip_if_oos": False}]


def test_parse_monitor_input_accepts_placeholder():
    rows = parse_monitor_input("placeholder")
    assert rows == [{"pid": "placeholder", "quantity": 1, "skip_if_oos": True}]


def test_parse_monitor_input_quick_edit_pid_and_qty():
    rows = parse_monitor_input(
        "72-10704-101:2,72-10705-111:1",
        is_edit_flow=True,
        existing_product_count=1,
    )
    assert rows == [
        {"pid": "72-10704-101", "quantity": 2, "skip_if_oos": False},
        {"pid": "72-10705-111", "quantity": 1, "skip_if_oos": False},
    ]


def test_parse_monitor_input_rejects_invalid_pid_format():
    with pytest.raises(MonitorInputValidationError, match="invalid PID format"):
        parse_monitor_input("bad-pid")


def test_parse_monitor_input_rejects_invalid_quantity():
    with pytest.raises(MonitorInputValidationError, match="invalid quantity"):
        parse_monitor_input("72-10704-101:0", is_edit_flow=True, existing_product_count=1)


def test_parse_monitor_input_rejects_quick_edit_when_not_single_edit_product():
    with pytest.raises(MonitorInputValidationError, match="Quick Edit is only allowed"):
        parse_monitor_input("72-10704-101,72-10705-111")


def test_parse_monitor_input_rejects_empty_segments():
    with pytest.raises(MonitorInputValidationError, match="empty segment"):
        parse_monitor_input("72-10704-101,,72-10705-111", is_edit_flow=True, existing_product_count=1)


def test_parse_monitor_input_dedupes_duplicate_pid_by_first_seen():
    rows = parse_monitor_input(
        "72-10704-101:2,72-10704-101:4,72-10705-111:1",
        is_edit_flow=True,
        existing_product_count=1,
    )
    assert rows == [
        {"pid": "72-10704-101", "quantity": 2, "skip_if_oos": False},
        {"pid": "72-10705-111", "quantity": 1, "skip_if_oos": False},
    ]


def test_parse_monitor_input_rejects_mixed_url_and_pid_segment():
    with pytest.raises(MonitorInputValidationError, match="invalid PID format"):
        parse_monitor_input(
            "https://www.pokemoncenter.com/product/72-10704-101,72-10705-111",
            is_edit_flow=True,
            existing_product_count=1,
        )
