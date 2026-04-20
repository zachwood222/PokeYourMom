from __future__ import annotations

import os
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


@dataclass(frozen=True)
class RequestResult:
    response: Response | None
    telemetry: RequestTelemetry
    error: Exception | None


class SessionManager:
    def __init__(self, cookie_dir: str | None = None) -> None:
        self.cookie_dir = Path(cookie_dir or os.getenv("SESSION_COOKIE_DIR", ".session_cookies"))
        self.cookie_dir.mkdir(parents=True, exist_ok=True)
        self._lock = threading.Lock()
        self._sessions: dict[tuple[int | None, str], requests.Session] = {}

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
                ),
            )
        finally:
            self.save_cookies(workspace_id=workspace_id, task_key=task_key)
