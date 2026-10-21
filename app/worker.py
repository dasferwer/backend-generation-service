import asyncio
import logging
from datetime import timedelta
from uuid import uuid4

import httpx
from pydantic import BaseModel, Field, ValidationError
from sqlalchemy import and_, func, or_, select, update
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import Settings
from app.db import attempts, tasks
from app.service import OPEN_ATTEMPTS, finish

log = logging.getLogger(__name__)


class ProviderAccepted(BaseModel):
    external_id: str = Field(min_length=1, max_length=128)


class Worker:
    def __init__(
        self, engine: AsyncEngine, settings: Settings, client: httpx.AsyncClient | None = None
    ):
        self.engine = engine
        self.settings = settings
        self.owns_client = client is None
        self.client = (
            client
            if client is not None
            else httpx.AsyncClient(
                timeout=httpx.Timeout(connect=1, read=settings.post_timeout, write=1, pool=1),
                limits=httpx.Limits(
                    max_connections=settings.send_concurrency,
                    max_keepalive_connections=settings.send_concurrency,
                ),
            )
        )

    async def close(self) -> None:
        if self.owns_client:
            await self.client.aclose()

    async def claim(self) -> dict | None:
        async with self.engine.begin() as conn:
            now = await conn.scalar(select(func.clock_timestamp()))
            row = (
                (
                    await conn.execute(
                        select(attempts)
                        .where(
                            or_(
                                attempts.c.status == "queued",
                                and_(attempts.c.status == "sending", attempts.c.lease_until <= now),
                            ),
                            attempts.c.deadline_at > now,
                            attempts.c.send_count < self.settings.max_send_count,
                        )
                        .order_by(attempts.c.created_at, attempts.c.id)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return None
            values = dict(
                status="sending",
                lease_token=uuid4(),
                lease_until=now + timedelta(seconds=self.settings.lease_seconds),
                started_at=row["started_at"] or now,
                send_count=row["send_count"] + 1,
            )
            await conn.execute(update(attempts).where(attempts.c.id == row["id"]).values(**values))
            task = (
                (await conn.execute(select(tasks).where(tasks.c.id == row["task_id"])))
                .mappings()
                .one()
            )
            return dict(row) | values | {"task": dict(task)}

    async def acknowledge(self, attempt: dict, external_id: str) -> bool:
        # Токен аренды отсеивает прежнего владельца даже после повторного захвата строки.
        async with self.engine.begin() as conn:
            result = await conn.execute(
                update(attempts)
                .where(
                    attempts.c.id == attempt["id"],
                    attempts.c.status == "sending",
                    attempts.c.lease_token == attempt["lease_token"],
                    attempts.c.lease_until > func.clock_timestamp(),
                )
                .values(
                    status="waiting", external_id=external_id, lease_token=None, lease_until=None
                )
            )
            return result.rowcount == 1

    async def dispatch_once(self) -> bool:
        attempt = await self.claim()
        if attempt is None:
            return False
        provider = attempt["provider"]
        log.info(
            "dispatch task_id=%s attempt_id=%s provider=%s delivery=%s",
            attempt["task_id"],
            attempt["id"],
            provider,
            attempt["send_count"],
        )
        base = self.settings.alpha_url if provider == "alpha" else self.settings.beta_url
        task = attempt["task"]
        error = detail = None
        try:
            # Общий бюджет ограничивает также ответы, поступающие бесконечно по кусочку.
            async with asyncio.timeout(self.settings.post_timeout):
                response = await self.client.post(
                    base + "/tasks",
                    json={
                        "attempt_id": str(attempt["id"]),
                        "task_id": str(attempt["task_id"]),
                        "prompt": task["prompt"],
                        "ref_images": task["ref_images"],
                    },
                )
            if response.status_code == 202:
                accepted = ProviderAccepted.model_validate(response.json())
                await self.acknowledge(attempt, accepted.external_id)
                return True
            error = (
                "technical"
                if response.status_code >= 500 or response.status_code == 429
                else "payload"
            )
            detail = "http_" + str(response.status_code)
        except (TimeoutError, httpx.TimeoutException):
            error, detail = "technical", "post_timeout"
        except httpx.HTTPError:
            error, detail = "technical", "connection_error"
        except (ValueError, ValidationError):
            error, detail = "technical", "invalid_response"
        await finish(
            self.engine,
            attempt["id"],
            self.settings,
            error_type=error,
            error_detail=detail,
            lease_token=attempt["lease_token"],
        )
        return True

    async def watchdog_once(self) -> int:
        async with self.engine.connect() as conn:
            expired = (
                await conn.scalars(
                    select(attempts.c.id)
                    .where(
                        attempts.c.status.in_(OPEN_ATTEMPTS),
                        attempts.c.deadline_at <= func.clock_timestamp(),
                    )
                    .order_by(attempts.c.deadline_at)
                    .limit(100)
                )
            ).all()
        changed = 0
        for attempt_id in expired:
            changed += await finish(
                self.engine,
                attempt_id,
                self.settings,
                error_type="technical",
                error_detail="provider_timeout",
                expired_only=True,
            )
        return changed

    async def dispatch_loop(self) -> None:
        while True:
            try:
                if await self.dispatch_once():
                    continue
            except Exception:
                log.exception("dispatch_cycle_failed")
            await asyncio.sleep(self.settings.poll_interval)

    async def watchdog_loop(self) -> None:
        while True:
            try:
                await self.watchdog_once()
            except Exception:
                log.exception("watchdog_cycle_failed")
            await asyncio.sleep(self.settings.poll_interval)
