from __future__ import annotations

import hashlib
import json
import re
from dataclasses import dataclass
from datetime import datetime, timezone
from typing import Any

from retailers import canonical_retailer


@dataclass
class NormalizedAlertEvent:
    source_event_id: str
    source: str
    retailer: str | None
    product_url: str | None
    sku: str | None
    title: str
    message: str
    event_time: str
    raw_payload: dict[str, Any]

    @property
    def search_blob(self) -> str:
        parts = [self.title, self.message, self.product_url or "", self.sku or ""]
        return " ".join(parts).lower()


def _normalize_iso(value: Any) -> str:
    if isinstance(value, str) and value.strip():
        text = value.strip()
        if text.endswith("Z"):
            text = text[:-1] + "+00:00"
        try:
            return datetime.fromisoformat(text).astimezone(timezone.utc).isoformat()
        except ValueError:
            pass
    if isinstance(value, (int, float)):
        return datetime.fromtimestamp(float(value), tz=timezone.utc).isoformat()
    return datetime.now(timezone.utc).isoformat()


def normalize_discord_alert_event(payload: dict[str, Any], *, fallback_source: str) -> NormalizedAlertEvent:
    content = str(payload.get("content") or payload.get("message") or "").strip()
    embed = (payload.get("embed") or payload.get("embeds") or [None])[0] if isinstance(payload.get("embeds"), list) else payload.get("embed")
    embed = embed if isinstance(embed, dict) else {}
    title = str(payload.get("title") or embed.get("title") or "").strip() or "Discord Alert"
    product_url = str(payload.get("product_url") or payload.get("url") or embed.get("url") or "").strip() or None
    sku = str(payload.get("sku") or payload.get("product_id") or "").strip() or None
    retailer_raw = payload.get("retailer") or payload.get("store") or ""
    retailer = canonical_retailer(str(retailer_raw)) if str(retailer_raw).strip() else None
    source_event_id = str(payload.get("id") or payload.get("event_id") or "").strip()
    if not source_event_id:
        digest_input = json.dumps(payload, sort_keys=True).encode("utf-8")
        source_event_id = hashlib.sha256(digest_input).hexdigest()
    return NormalizedAlertEvent(
        source_event_id=source_event_id,
        source=fallback_source,
        retailer=retailer,
        product_url=product_url,
        sku=sku,
        title=title,
        message=content,
        event_time=_normalize_iso(payload.get("event_time") or payload.get("timestamp")),
        raw_payload=payload,
    )


def _pattern_match(patterns: list[str], text: str) -> bool:
    if not patterns:
        return True
    for pattern in patterns:
        if not pattern:
            continue
        try:
            if re.search(pattern, text, re.IGNORECASE):
                return True
        except re.error:
            if pattern.lower() in text.lower():
                return True
    return False


def subscription_accepts_event(
    event: NormalizedAlertEvent,
    *,
    retailer_filter: str | None,
    url_patterns: list[str],
    sku_patterns: list[str],
    keyword_patterns: list[str],
) -> bool:
    if retailer_filter and event.retailer and canonical_retailer(retailer_filter) != event.retailer:
        return False
    url_text = event.product_url or ""
    if url_patterns and not _pattern_match(url_patterns, url_text):
        return False
    sku_text = event.sku or ""
    if sku_patterns and not _pattern_match(sku_patterns, sku_text):
        return False
    if keyword_patterns and not _pattern_match(keyword_patterns, event.search_blob):
        return False
    return True


def monitor_matches_alert(monitor: dict[str, Any], event: NormalizedAlertEvent) -> bool:
    if event.retailer and canonical_retailer(monitor.get("retailer") or "") != event.retailer:
        return False
    monitor_url = str(monitor.get("product_url") or "").strip().lower()
    event_url = (event.product_url or "").strip().lower()
    if monitor_url and event_url and monitor_url not in event_url and event_url not in monitor_url:
        return False

    keyword = str(monitor.get("keyword") or "").strip()
    if keyword:
        if not re.search(re.escape(keyword), event.search_blob, re.IGNORECASE):
            return False

    if event.sku:
        sku = event.sku.lower()
        if sku not in event.search_blob and sku not in monitor_url:
            return False
    return True


def action_dedupe_key(*, workspace_id: int, monitor_id: int, event_id: int, action_type: str) -> str:
    return f"{workspace_id}:{monitor_id}:{event_id}:{action_type}"
