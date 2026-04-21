from __future__ import annotations

import hashlib
import json
import secrets
from dataclasses import dataclass
from datetime import datetime, timedelta, timezone
from typing import Any, Protocol


CHALLENGE_STATUSES = {"pending", "manual_required", "solved", "expired"}


@dataclass
class SolveAttempt:
    status: str
    provider_payload: dict[str, Any]
    solved_token: str | None = None


class SolveProvider(Protocol):
    name: str

    def attempt_solve(self, challenge: dict[str, Any]) -> SolveAttempt: ...


class ManualFallbackSolveProvider:
    name = "manual"

    def attempt_solve(self, challenge: dict[str, Any]) -> SolveAttempt:
        return SolveAttempt(
            status="manual_required",
            provider_payload={"note": "Manual solve required"},
        )


class CaptchaChallengeService:
    def __init__(self, *, now_fn) -> None:
        self.now_fn = now_fn

    def now_iso(self) -> str:
        return self.now_fn()

    def create_challenge(
        self,
        conn,
        *,
        workspace_id: int,
        task_id: int,
        retailer_account_id: int | None,
        provider_name: str,
        expires_in_seconds: int = 300,
    ):
        created_at = self.now_iso()
        expires_at = (datetime.now(timezone.utc) + timedelta(seconds=expires_in_seconds)).isoformat()
        cur = conn.execute(
            """
            insert into captcha_challenges(
                workspace_id,
                task_id,
                retailer_account_id,
                provider,
                status,
                provider_payload,
                expires_at,
                created_at,
                updated_at
            )
            values (?, ?, ?, ?, 'pending', '{}', ?, ?, ?)
            """,
            (workspace_id, task_id, retailer_account_id, provider_name, expires_at, created_at, created_at),
        )
        return conn.execute("select * from captcha_challenges where id = ?", (cur.lastrowid,)).fetchone()

    def mark_attempt_result(self, conn, *, challenge_id: int, attempt: SolveAttempt):
        if attempt.status not in CHALLENGE_STATUSES:
            raise ValueError(f"invalid challenge status {attempt.status}")
        now_iso = self.now_iso()
        conn.execute(
            """
            update captcha_challenges
            set status = ?,
                provider_payload = ?,
                solved_token = ?,
                solved_at = case when ? = 'solved' then ? else solved_at end,
                updated_at = ?
            where id = ?
            """,
            (
                attempt.status,
                json.dumps(attempt.provider_payload or {}),
                attempt.solved_token,
                attempt.status,
                now_iso,
                now_iso,
                challenge_id,
            ),
        )

    def mark_manual_solution(
        self,
        conn,
        *,
        challenge_id: int,
        solved_token: str,
        operator_note: str | None,
    ):
        now_iso = self.now_iso()
        conn.execute(
            """
            update captcha_challenges
            set status = 'solved',
                solved_token = ?,
                manual_payload = ?,
                solved_at = ?,
                updated_at = ?
            where id = ?
            """,
            (solved_token, json.dumps({"operator_note": operator_note or ""}), now_iso, now_iso, challenge_id),
        )

    def expire_stale_challenges(self, conn) -> int:
        now_iso = self.now_iso()
        cur = conn.execute(
            """
            update captcha_challenges
            set status = 'expired',
                updated_at = ?
            where status in ('pending', 'manual_required')
              and expires_at is not null
              and datetime(expires_at) <= datetime(?)
            """,
            (now_iso, now_iso),
        )
        return int(cur.rowcount or 0)

    def issue_worker_handoff_token(
        self,
        conn,
        *,
        challenge_id: int,
        ttl_seconds: int = 90,
    ) -> str:
        row = conn.execute("select * from captcha_challenges where id = ?", (challenge_id,)).fetchone()
        if not row:
            raise ValueError("Challenge not found")
        if row["status"] != "solved":
            raise ValueError("Challenge is not solved")
        expires_at = _parse_iso_timestamp(row["expires_at"])
        if expires_at and expires_at <= datetime.now(timezone.utc):
            raise ValueError("Challenge is expired")

        token = secrets.token_urlsafe(32)
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        now_iso = self.now_iso()
        handoff_expires_at = (datetime.now(timezone.utc) + timedelta(seconds=ttl_seconds)).isoformat()
        conn.execute(
            """
            update captcha_challenges
            set worker_handoff_token_hash = ?,
                handoff_issued_at = ?,
                handoff_expires_at = ?,
                handoff_used_at = null,
                updated_at = ?
            where id = ?
            """,
            (token_hash, now_iso, handoff_expires_at, now_iso, challenge_id),
        )
        return token

    def consume_worker_handoff_token(self, conn, *, token: str) -> dict[str, Any]:
        token_hash = hashlib.sha256(token.encode("utf-8")).hexdigest()
        row = conn.execute(
            """
            select * from captcha_challenges
            where worker_handoff_token_hash = ?
            limit 1
            """,
            (token_hash,),
        ).fetchone()
        if not row:
            raise ValueError("Invalid handoff token")
        if row["handoff_used_at"]:
            raise ValueError("Handoff token already used")
        now = datetime.now(timezone.utc)
        handoff_expires_at = _parse_iso_timestamp(row["handoff_expires_at"])
        if handoff_expires_at and handoff_expires_at <= now:
            raise ValueError("Handoff token expired")

        now_iso = self.now_iso()
        conn.execute(
            """
            update captcha_challenges
            set handoff_used_at = ?,
                updated_at = ?
            where id = ?
            """,
            (now_iso, now_iso, row["id"]),
        )
        return {
            "challenge_id": row["id"],
            "task_id": row["task_id"],
            "workspace_id": row["workspace_id"],
            "solved_token": row["solved_token"],
        }


def serialize_challenge(row) -> dict[str, Any] | None:
    if row is None:
        return None
    payload = dict(row)
    for key in ("provider_payload", "manual_payload"):
        raw = payload.get(key)
        try:
            payload[key] = json.loads(raw) if raw else {}
        except (TypeError, json.JSONDecodeError):
            payload[key] = {}
    payload.pop("worker_handoff_token_hash", None)
    return payload


def _parse_iso_timestamp(value: str | None) -> datetime | None:
    if not value:
        return None
    parsed = datetime.fromisoformat(value)
    if parsed.tzinfo is None:
        return parsed.replace(tzinfo=timezone.utc)
    return parsed.astimezone(timezone.utc)
