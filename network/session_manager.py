from __future__ import annotations

import os
import random
import threading
import time
from dataclasses import dataclass
from http.cookiejar import LWPCookieJar
from pathlib import Path
from typing import Any

import requests
from requests import Response
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry


@dataclass(frozen=True)
class RequestTelemetry:
    latency_ms: int
    status_code: int | None
    ok: bool
    error_class: str | None
    task_key: str
    workspace_id: int | None
    proxy_url: str | None
    retried: bool
    pacing_profile: str
    planned_delay_ms: int
    applied_delay_ms: int
    adaptive_backoff_level: int
    throttled: bool
    throttle_reason: str | None


@dataclass(frozen=True)
class RequestResult:
    response: Response | None
    telemetry: RequestTelemetry
    error: Exception | None


@dataclass(frozen=True)
class RequestBehaviorPolicy:
    profile: str = "default"
    base_delay_seconds: float = 0.0
    jitter_ratio: float = 0.15
    min_delay_seconds: float = 0.0
    max_delay_seconds: float = 2.0
    adaptive_backoff_enabled: bool = True
    adaptive_backoff_step_seconds: float = 0.35
    adaptive_backoff_cap_seconds: float = 4.0
    retailer_profiles: dict[str, dict[str, Any]] | None = None

    @staticmethod
    def from_mapping(value: dict[str, Any] | None) -> "RequestBehaviorPolicy":
        if not isinstance(value, dict):
            return RequestBehaviorPolicy()
        return RequestBehaviorPolicy(
            profile=str(value.get("profile", "default")).strip() or "default",
            base_delay_seconds=max(0.0, float(value.get("base_delay_seconds", 0.0))),
            jitter_ratio=max(0.0, min(1.0, float(value.get("jitter_ratio", 0.15)))),
            min_delay_seconds=max(0.0, float(value.get("min_delay_seconds", 0.0))),
            max_delay_seconds=max(0.0, float(value.get("max_delay_seconds", 2.0))),
            adaptive_backoff_enabled=bool(value.get("adaptive_backoff_enabled", True)),
            adaptive_backoff_step_seconds=max(0.0, float(value.get("adaptive_backoff_step_seconds", 0.35))),
            adaptive_backoff_cap_seconds=max(0.0, float(value.get("adaptive_backoff_cap_seconds", 4.0))),
            retailer_profiles=value.get("retailer_profiles") if isinstance(value.get("retailer_profiles"), dict) else None,
        )

    def for_retailer(self, retailer: str | None) -> "RequestBehaviorPolicy":
        if not retailer or not self.retailer_profiles:
            return self
        profile = self.retailer_profiles.get(retailer.lower())
        if not isinstance(profile, dict):
            return self
        merged = {
            "profile": profile.get("profile") or self.profile,
            "base_delay_seconds": profile.get("base_delay_seconds", self.base_delay_seconds),
            "jitter_ratio": profile.get("jitter_ratio", self.jitter_ratio),
            "min_delay_seconds": profile.get("min_delay_seconds", self.min_delay_seconds),
            "max_delay_seconds": profile.get("max_delay_seconds", self.max_delay_seconds),
            "adaptive_backoff_enabled": profile.get("adaptive_backoff_enabled", self.adaptive_backoff_enabled),
            "adaptive_backoff_step_seconds": profile.get("adaptive_backoff_step_seconds", self.adaptive_backoff_step_seconds),
            "adaptive_backoff_cap_seconds": profile.get("adaptive_backoff_cap_seconds", self.adaptive_backoff_cap_seconds),
            "retailer_profiles": self.retailer_profiles,
        }
        return RequestBehaviorPolicy.from_mapping(merged)


class SessionManager:
    def __init__(self, cookie_dir: str | None = None) -> None:
        self.cookie_dir = Path(cookie_dir or os.getenv("SESSION_COOKIE_DIR", ".session_cookies"))
        self.cookie_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._sessions: dict[tuple[int | None, str], requests.Session] = {}
        self._adaptive_backoff_levels: dict[tuple[int | None, str], int] = {}

    def _cookie_path(self, workspace_id: int | None, task_key: str) -> Path:
        safe_workspace = "global" if workspace_id is None else str(workspace_id)
        safe_task = "".join(ch if ch.isalnum() or ch in ("-", "_") else "_" for ch in task_key)[:80]
        return self.cookie_dir / f"{safe_workspace}_{safe_task}.lwp"

    def get_session(
        self,
        *,
        workspace_id: int | None,
        task_key: str,
        retry_total: int,
        backoff_factor: float,
        proxy_url: str | None,
    ) -> requests.Session:
        key = (workspace_id, task_key)
        with self._lock:
            session = self._sessions.get(key)
            if session is not None:
                if proxy_url:
                    session.proxies = {"http": proxy_url, "https": proxy_url}
                return session

            session = requests.Session()
            retry = Retry(
                total=retry_total,
                connect=retry_total,
                read=retry_total,
                backoff_factor=backoff_factor,
                status_forcelist=[429, 500, 502, 503, 504],
                allowed_methods=frozenset(["HEAD", "GET", "POST", "PUT", "PATCH", "DELETE"]),
                raise_on_status=False,
            )
            adapter = HTTPAdapter(max_retries=retry)
            session.mount("http://", adapter)
            session.mount("https://", adapter)
            if proxy_url:
                session.proxies = {"http": proxy_url, "https": proxy_url}

            cookie_path = self._cookie_path(workspace_id, task_key)
            jar = LWPCookieJar(str(cookie_path))
            if cookie_path.exists():
                try:
                    jar.load(ignore_discard=True, ignore_expires=True)
                except Exception:
                    pass
            session.cookies = jar
            self._sessions[key] = session
            return session

    def save_cookies(self, *, workspace_id: int | None, task_key: str) -> None:
        session = self._sessions.get((workspace_id, task_key))
        if session is None:
            return
        jar = session.cookies
        if isinstance(jar, LWPCookieJar):
            jar.save(ignore_discard=True, ignore_expires=True)

    def request(
        self,
        *,
        task_key: str,
        method: str,
        url: str,
        workspace_id: int | None = None,
        proxy_url: str | None = None,
        behavior_policy: RequestBehaviorPolicy | None = None,
        pacing_key: str | None = None,
        throttle_signal: bool = False,
        throttle_reason: str | None = None,
        timeout: float = 10.0,
        retry_total: int = 2,
        backoff_factor: float = 0.35,
        **kwargs: Any,
    ) -> RequestResult:
        session = self.get_session(
            workspace_id=workspace_id,
            task_key=task_key,
            retry_total=retry_total,
            backoff_factor=backoff_factor,
            proxy_url=proxy_url,
        )
        policy = behavior_policy or RequestBehaviorPolicy()
        throttle_key = (workspace_id, pacing_key or task_key)
        adaptive_level = 0
        with self._lock:
            adaptive_level = self._adaptive_backoff_levels.get(throttle_key, 0)
        adaptive_delay = 0.0
        if policy.adaptive_backoff_enabled and adaptive_level > 0:
            adaptive_delay = min(policy.adaptive_backoff_step_seconds * adaptive_level, policy.adaptive_backoff_cap_seconds)
        planned_delay = policy.base_delay_seconds + adaptive_delay
        jitter_delta = planned_delay * policy.jitter_ratio
        jittered_delay = planned_delay + random.uniform(-jitter_delta, jitter_delta) if planned_delay > 0 else 0.0
        applied_delay = min(max(jittered_delay, policy.min_delay_seconds), policy.max_delay_seconds)
        if applied_delay > 0:
            time.sleep(applied_delay)
        started = time.perf_counter()
        status_code = None
        error: Exception | None = None
        response = None
        try:
            response = session.request(method=method, url=url, timeout=timeout, **kwargs)
            status_code = response.status_code
            return RequestResult(
                response=response,
                error=None,
                telemetry=RequestTelemetry(
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    status_code=status_code,
                    ok=response.ok,
                    error_class=None,
                    task_key=task_key,
                    workspace_id=workspace_id,
                    proxy_url=proxy_url,
                    retried=False,
                    pacing_profile=policy.profile,
                    planned_delay_ms=int(planned_delay * 1000),
                    applied_delay_ms=int(applied_delay * 1000),
                    adaptive_backoff_level=adaptive_level,
                    throttled=throttle_signal or status_code == 429,
                    throttle_reason=throttle_reason if throttle_signal else ("http_429" if status_code == 429 else None),
                ),
            )
        except Exception as exc:  # noqa: BLE001
            error = exc
            return RequestResult(
                response=None,
                error=error,
                telemetry=RequestTelemetry(
                    latency_ms=int((time.perf_counter() - started) * 1000),
                    status_code=status_code,
                    ok=False,
                    error_class=exc.__class__.__name__,
                    task_key=task_key,
                    workspace_id=workspace_id,
                    proxy_url=proxy_url,
                    retried=False,
                    pacing_profile=policy.profile,
                    planned_delay_ms=int(planned_delay * 1000),
                    applied_delay_ms=int(applied_delay * 1000),
                    adaptive_backoff_level=adaptive_level,
                    throttled=throttle_signal,
                    throttle_reason=throttle_reason if throttle_signal else None,
                ),
            )
        finally:
            with self._lock:
                level = self._adaptive_backoff_levels.get(throttle_key, 0)
                if error is not None or status_code == 429 or throttle_signal:
                    self._adaptive_backoff_levels[throttle_key] = min(level + 1, 20)
                elif level > 0:
                    self._adaptive_backoff_levels[throttle_key] = max(level - 1, 0)
            self.save_cookies(workspace_id=workspace_id, task_key=task_key)
