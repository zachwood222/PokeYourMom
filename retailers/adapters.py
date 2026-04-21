from __future__ import annotations

import re
from abc import ABC, abstractmethod
from dataclasses import dataclass
from typing import Any

RETAILER_ALIASES = {
    "wal-mart": "walmart",
    "wal mart": "walmart",
    "target.com": "target",
    "target com": "target",
    "pokemon-center": "pokemoncenter",
    "pokemon_center": "pokemoncenter",
    "pokemon center": "pokemoncenter",
    "pokemoncenter": "pokemoncenter",
}

TaskContext = dict[str, Any]
DEFAULT_CATEGORY = "pokemon"
SUPPORTED_CATEGORIES = {"pokemon", "sports_cards", "one_piece", "lorcana"}


@dataclass
class MonitorResult:
    in_stock: bool
    price_cents: int | None
    title: str
    status_text: str
    availability_reason: str | None = None
    parser_confidence: float | None = None
    keyword_matched: bool | None = None
    price_within_limit: bool | None = None
    within_msrp_delta: bool | None = None


def extract_price_cents(text: str) -> int | None:
    matches = re.findall(r"\$\s*(\d{1,4}(?:\.\d{2})?)", text)
    if not matches:
        return None
    values = []
    for m in matches:
        try:
            v = float(m)
            if 1.0 <= v <= 2000.0:
                values.append(int(round(v * 100)))
        except ValueError:
            continue
    return min(values) if values else None


def canonical_retailer(retailer: str) -> str:
    value = retailer.strip().lower()
    return RETAILER_ALIASES.get(value, value)


def _parse_common_title_and_text(html: str) -> tuple[str, str]:
    title_match = re.search(r"<title[^>]*>(.*?)</title>", html, flags=re.IGNORECASE | re.DOTALL)
    title = re.sub(r"\s+", " ", title_match.group(1)).strip() if title_match else "Product"
    text = re.sub(r"<[^>]+>", " ", html).lower()
    return title[:180], text


def default_parser(html: str, keyword: str | None = None) -> MonitorResult:
    title, text = _parse_common_title_and_text(html)

    out_markers = [
        "out of stock",
        "sold out",
        "unavailable",
        "not available",
        "coming soon",
        "temporarily out of stock",
    ]
    in_markers = [
        "in stock",
        "add to cart",
        "buy now",
        "pickup",
        "ship it",
    ]

    has_out = any(m in text for m in out_markers)
    has_in = any(m in text for m in in_markers)

    in_stock = has_in and not has_out
    availability_reason = "fallback_unknown"
    parser_confidence = 0.2
    if has_out and not has_in:
        availability_reason = "marker_out_of_stock"
        parser_confidence = 0.9
    elif has_in and not has_out:
        availability_reason = "marker_in_stock"
        parser_confidence = 0.9
    elif has_in and has_out:
        availability_reason = "marker_conflict"
        parser_confidence = 0.35
    keyword_matched: bool | None = None
    if keyword:
        keyword_matched = keyword.lower() in text
    price_cents = extract_price_cents(re.sub(r"<[^>]+>", " ", html))
    status_text = "in_stock" if in_stock else "out_or_unknown"
    return MonitorResult(
        in_stock=in_stock,
        price_cents=price_cents,
        title=title[:180],
        status_text=status_text,
        availability_reason=availability_reason,
        parser_confidence=parser_confidence,
        keyword_matched=keyword_matched,
    )


class RetailerAdapter(ABC):
    def __init__(self, name: str):
        self.name = name

    def preload_session(self, task_ctx: TaskContext) -> None:
        return None

    @abstractmethod
    def check_stock(self, task_ctx: TaskContext) -> MonitorResult:
        raise NotImplementedError

    def add_to_cart(self, task_ctx: TaskContext) -> None:
        return None

    def submit_shipping(self, task_ctx: TaskContext) -> None:
        return None

    def submit_payment(self, task_ctx: TaskContext) -> None:
        return None

    def place_order(self, task_ctx: TaskContext) -> None:
        return None


class DefaultRetailerAdapter(RetailerAdapter):
    def __init__(self) -> None:
        super().__init__(name="default")

    def check_stock(self, task_ctx: TaskContext) -> MonitorResult:
        return default_parser(task_ctx["html"], keyword=task_ctx.get("keyword"))


class PokemonCenterAdapter(DefaultRetailerAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.name = "pokemoncenter"

    def check_stock(self, task_ctx: TaskContext) -> MonitorResult:
        html = task_ctx["html"]
        title, text = _parse_common_title_and_text(html)
        result = default_parser(html, keyword=task_ctx.get("keyword"))
        category = (task_ctx.get("category") or DEFAULT_CATEGORY).strip().lower()
        out_markers = [
            "notify me when available",
            "currently unavailable",
        ]
        in_markers = ["add to bag"]
        if category == "one_piece":
            out_markers.append("this item is unavailable in your region")
        if category == "lorcana":
            in_markers.append("preorder")
        has_out = any(m in text for m in out_markers)
        has_in = any(m in text for m in in_markers)
        if has_out:
            result.in_stock = False
            result.status_text = "out_or_unknown"
            result.availability_reason = "pokemoncenter_marker_out_of_stock"
            result.parser_confidence = 0.98
        elif has_in:
            result.in_stock = True
            result.status_text = "in_stock"
            result.availability_reason = "pokemoncenter_marker_in_stock"
            result.parser_confidence = 0.98
        if category == "lorcana":
            price_match = re.search(r'"price"\s*:\s*"?(\d{1,4}(?:\.\d{2})?)"?', text)
            if price_match:
                result.price_cents = int(round(float(price_match.group(1)) * 100))
            elif result.price_cents is None:
                result.price_cents = extract_price_cents(html)
        else:
            result.price_cents = extract_price_cents(html)
        result.title = title
        return result


class WalmartAdapter(DefaultRetailerAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.name = "walmart"

    def check_stock(self, task_ctx: TaskContext) -> MonitorResult:
        html = task_ctx["html"]
        title, text = _parse_common_title_and_text(html)
        result = default_parser(html, keyword=task_ctx.get("keyword"))
        if '"availability":"instock"' in text or "fulfillmentoptions" in text:
            result.in_stock = True
            result.status_text = "in_stock"
            result.availability_reason = "walmart_marker_in_stock"
            result.parser_confidence = 0.98
        if '"availability":"outofstock"' in text or "out of stock" in text:
            result.in_stock = False
            result.status_text = "out_or_unknown"
            result.availability_reason = "walmart_marker_out_of_stock"
            result.parser_confidence = 0.98
        result.price_cents = extract_price_cents(html)
        result.title = title
        return result


class TargetAdapter(DefaultRetailerAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.name = "target"

    def check_stock(self, task_ctx: TaskContext) -> MonitorResult:
        html = task_ctx["html"]
        title, text = _parse_common_title_and_text(html)
        result = default_parser(html, keyword=task_ctx.get("keyword"))
        category = (task_ctx.get("category") or DEFAULT_CATEGORY).strip().lower()

        in_markers = [
            '"availability":"instock"',
            '"availability":"in_stock"',
            "add to cart",
            "ship it",
            "pick up",
        ]
        out_markers = [
            '"availability":"outofstock"',
            '"availability":"out_of_stock"',
            "out of stock",
            "sold out",
            "unavailable",
        ]
        if category == "sports_cards":
            in_markers.extend(["same day delivery", "shipping available"])
        elif category == "one_piece":
            in_markers.append("choose store")
            out_markers.append("limited availability")
        elif category == "lorcana":
            in_markers.append("preorder")
            out_markers.append("release date pending")
        has_in = any(marker in text for marker in in_markers)
        has_out = any(marker in text for marker in out_markers)

        if has_out:
            result.in_stock = False
            result.status_text = "out_or_unknown"
            result.availability_reason = "target_marker_out_of_stock"
            result.parser_confidence = 0.98
        elif has_in:
            result.in_stock = True
            result.status_text = "in_stock"
            result.availability_reason = "target_marker_in_stock"
            result.parser_confidence = 0.98
        if category == "sports_cards":
            deal_price_match = re.search(r'"current_retail"\s*:\s*"?(\d{1,4}(?:\.\d{2})?)"?', text)
            if deal_price_match:
                result.price_cents = int(round(float(deal_price_match.group(1)) * 100))
            else:
                result.price_cents = extract_price_cents(html)
        else:
            result.price_cents = extract_price_cents(html)
        result.title = title
        return result


class BestBuyAdapter(DefaultRetailerAdapter):
    def __init__(self) -> None:
        super().__init__()
        self.name = "bestbuy"

    def check_stock(self, task_ctx: TaskContext) -> MonitorResult:
        html = task_ctx["html"]
        title, text = _parse_common_title_and_text(html)
        result = default_parser(html, keyword=task_ctx.get("keyword"))

        in_markers = [
            '"buttonstate":"add to cart"',
            '"shipping":"available"',
            "ready for pickup today",
        ]
        out_markers = [
            '"buttonstate":"sold out"',
            '"buttonstate":"coming soon"',
            '"shipping":"unavailable"',
            "sold out",
            "coming soon",
        ]
        has_in = any(marker in text for marker in in_markers)
        has_out = any(marker in text for marker in out_markers)

        if has_out:
            result.in_stock = False
            result.status_text = "out_or_unknown"
            result.availability_reason = "bestbuy_marker_out_of_stock"
            result.parser_confidence = 0.98
        elif has_in:
            result.in_stock = True
            result.status_text = "in_stock"
            result.availability_reason = "bestbuy_marker_in_stock"
            result.parser_confidence = 0.98

        result.price_cents = extract_price_cents(html)
        result.title = title
        return result


RETAILER_ADAPTERS: dict[str, RetailerAdapter] = {
    "walmart": WalmartAdapter(),
    "target": TargetAdapter(),
    "bestbuy": BestBuyAdapter(),
    "pokemoncenter": PokemonCenterAdapter(),
}
DEFAULT_ADAPTER = DefaultRetailerAdapter()


def resolve_retailer_adapter(retailer: str | None) -> RetailerAdapter:
    normalized = canonical_retailer(retailer) if retailer else ""
    return RETAILER_ADAPTERS.get(normalized, DEFAULT_ADAPTER)


def run_retailer_flow(adapter: RetailerAdapter, task_ctx: TaskContext) -> MonitorResult:
    adapter.preload_session(task_ctx)
    result = adapter.check_stock(task_ctx)
    task_ctx["monitor_result"] = result
    adapter.add_to_cart(task_ctx)
    adapter.submit_shipping(task_ctx)
    adapter.submit_payment(task_ctx)
    adapter.place_order(task_ctx)
    return result
