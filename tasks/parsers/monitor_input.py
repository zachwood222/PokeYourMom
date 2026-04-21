from __future__ import annotations

import re
from dataclasses import dataclass
from urllib.parse import parse_qs, urlparse

PID_PATTERN = re.compile(r"^\d{2}-\d{5}-\d{3}$")
URL_PID_PATTERN = re.compile(r"(\d{2}-\d{5}-\d{3})")


@dataclass(frozen=True)
class MonitorInputValidationError(ValueError):
    code: str
    message: str

    def __str__(self) -> str:  # pragma: no cover - trivial string override
        return self.message


def _is_int_string(value: str) -> bool:
    return bool(re.fullmatch(r"\d+", value.strip()))


def _extract_pid_from_url(candidate: str) -> str | None:
    parsed = urlparse(candidate)
    if parsed.scheme not in {"http", "https"}:
        return None
    if "pokemoncenter.com" not in (parsed.netloc or "").lower():
        return None
    query_pid = (parse_qs(parsed.query).get("pid") or [None])[0]
    if query_pid and PID_PATTERN.match(query_pid.strip()):
        return query_pid.strip()
    match = URL_PID_PATTERN.search(parsed.path or "")
    return match.group(1) if match else None


def _parse_pid_quantity(segment: str) -> tuple[str, int]:
    if "://" in segment:
        raise MonitorInputValidationError("invalid_pid_format", f"invalid PID format: {segment}")
    if ":" not in segment:
        pid = segment.strip()
        qty = 1
    else:
        pid_part, qty_part = segment.split(":", 1)
        pid = pid_part.strip()
        qty_str = qty_part.strip()
        if not _is_int_string(qty_str):
            raise MonitorInputValidationError("invalid_quantity", "invalid quantity (must be an integer > 0)")
        qty = int(qty_str)
        if qty <= 0:
            raise MonitorInputValidationError("invalid_quantity", "invalid quantity (must be an integer > 0)")
    if not PID_PATTERN.match(pid):
        raise MonitorInputValidationError("invalid_pid_format", f"invalid PID format: {pid or '(empty)'}")
    return pid, qty


def parse_monitor_input(
    raw_input: str,
    *,
    is_edit_flow: bool = False,
    existing_product_count: int | None = None,
) -> list[dict[str, int | str | bool]]:
    value = (raw_input or "").strip()
    if not value:
        raise MonitorInputValidationError("invalid_pid_format", "invalid PID format: (empty)")

    if value.lower() == "placeholder":
        return [{"pid": "placeholder", "quantity": 1, "skip_if_oos": True}]

    url_pid = _extract_pid_from_url(value) if "," not in value else None
    if url_pid:
        return [{"pid": url_pid, "quantity": 1, "skip_if_oos": False}]

    segments = [segment.strip() for segment in value.split(",")]
    if any(segment == "" for segment in segments):
        raise MonitorInputValidationError("invalid_pid_format", "invalid PID format: empty segment")

    quick_edit_mode = len(segments) > 1 or any(":" in segment for segment in segments)
    if quick_edit_mode and (not is_edit_flow or existing_product_count != 1):
        raise MonitorInputValidationError(
            "quick_edit_not_allowed",
            "Quick Edit is only allowed when editing a task with exactly one product.",
        )

    entries: list[dict[str, int | str | bool]] = []
    seen: set[str] = set()
    for segment in segments:
        pid, quantity = _parse_pid_quantity(segment)
        if pid in seen:
            continue
        seen.add(pid)
        entries.append({"pid": pid, "quantity": quantity, "skip_if_oos": False})

    return entries
