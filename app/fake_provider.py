import asyncio
import hashlib
import json
import logging
import os
from contextlib import asynccontextmanager
from datetime import timedelta
from typing import Annotated, Literal
from uuid import UUID, uuid4

import httpx
from fastapi import Depends, FastAPI, HTTPException, Response
from pydantic import BaseModel, ConfigDict, Field, model_validator
from sqlalchemy import func, or_, select, update
from sqlalchemy.dialects.postgresql import insert
from sqlalchemy.ext.asyncio import AsyncEngine

from app.config import Settings
from app.db import fake_jobs, make_engine
from app.schemas import ErrorType, RefImage
from app.security import sign, verify_signature

log = logging.getLogger(__name__)


class FakeSettings(BaseModel):
    model_config = ConfigDict(extra="forbid")
    failure_rate: float = Field(0, ge=0, le=1)
    error_type: ErrorType = "technical"
    post_mode: Literal["normal", "503", "timeout"] = "normal"
    delay_min: float = Field(2, ge=0, le=3600)
    delay_max: float = Field(20, ge=0, le=3600)
    duplicates: int = Field(1, ge=1, le=2)
    no_webhook: bool = False
    post_delay: float = Field(5, ge=0, le=120)

    @model_validator(mode="after")
    def ordered_delay(self):
        if self.delay_max < self.delay_min:
            raise ValueError("delay_max должен быть не меньше delay_min")
        return self

    @classmethod
    def from_env(cls) -> "FakeSettings":
        return cls(
            **{
                key: os.environ["FAKE_" + key.upper()]
                for key in cls.model_fields
                if "FAKE_" + key.upper() in os.environ
            }
        )


class Submission(BaseModel):
    model_config = ConfigDict(extra="forbid")
    attempt_id: UUID
    task_id: UUID
    prompt: str = Field(min_length=1, max_length=10000)
    ref_images: Annotated[list[RefImage], Field(max_length=100)]


class DeliveryWorker:
    def __init__(
        self,
        engine: AsyncEngine,
        settings: Settings,
        provider: str,
        client: httpx.AsyncClient | None = None,
    ):
        self.engine, self.settings, self.provider = engine, settings, provider
        self.owns_client = client is None
        self.client = (
            client
            if client is not None
            else httpx.AsyncClient(
                timeout=httpx.Timeout(3, connect=1), limits=httpx.Limits(max_connections=2)
            )
        )

    async def close(self) -> None:
        if self.owns_client:
            await self.client.aclose()

    async def deliver_once(self) -> bool:
        async with self.engine.begin() as conn:
            now = await conn.scalar(select(func.clock_timestamp()))
            row = (
                (
                    await conn.execute(
                        select(fake_jobs)
                        .where(
                            fake_jobs.c.provider == self.provider,
                            fake_jobs.c.delivered_at.is_(None),
                            fake_jobs.c.next_delivery <= now,
                            fake_jobs.c.delivery_count < 8,
                            fake_jobs.c.behavior["no_webhook"].as_boolean().is_(False),
                            or_(fake_jobs.c.lease_until.is_(None), fake_jobs.c.lease_until <= now),
                        )
                        .order_by(fake_jobs.c.next_delivery)
                        .limit(1)
                        .with_for_update(skip_locked=True)
                    )
                )
                .mappings()
                .one_or_none()
            )
            if row is None:
                return False
            job = dict(row)
            token = uuid4()
            await conn.execute(
                update(fake_jobs)
                .where(
                    fake_jobs.c.provider == self.provider,
                    fake_jobs.c.attempt_id == job["attempt_id"],
                )
                .values(
                    lease_token=token,
                    lease_until=now + timedelta(seconds=10),
                    delivery_count=job["delivery_count"] + 1,
                )
            )
        behavior = job["behavior"]
        payload = {
            "attempt_id": str(job["attempt_id"]),
            "external_id": job["external_id"],
            "status": behavior["outcome"],
        }
        if payload["status"] == "completed":
            payload["result"] = f"https://results.example/{self.provider}/{job['external_id']}"
        else:
            payload["error_type"] = behavior["error_type"]
        body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
        headers = {
            "Content-Type": "application/json",
            "X-Signature": sign(body, self.settings.webhook_secret),
        }

        async def send() -> bool:
            try:
                async with asyncio.timeout(3):
                    response = await self.client.post(
                        self.settings.callback_base + "/webhooks/providers/" + self.provider,
                        content=body,
                        headers=headers,
                    )
                return response.is_success
            except (httpx.HTTPError, TimeoutError):
                return False

        results = await asyncio.gather(*(send() for _ in range(behavior["duplicates"])))
        async with self.engine.begin() as conn:
            values = {"lease_token": None, "lease_until": None}
            if all(results):
                values["delivered_at"] = func.clock_timestamp()
            else:
                values["next_delivery"] = func.clock_timestamp() + timedelta(
                    seconds=min(2 ** (job["delivery_count"] + 1), 30)
                )
            await conn.execute(
                update(fake_jobs)
                .where(
                    fake_jobs.c.provider == self.provider,
                    fake_jobs.c.attempt_id == job["attempt_id"],
                    fake_jobs.c.lease_token == token,
                    fake_jobs.c.lease_until > func.clock_timestamp(),
                )
                .values(**values)
            )
        log.info(
            "webhook_delivery task_id=%s attempt_id=%s provider=%s acknowledged=%s",
            job["request"]["task_id"],
            job["attempt_id"],
            self.provider,
            all(results),
        )
        return True

    async def loop(self) -> None:
        while True:
            try:
                if await self.deliver_once():
                    continue
            except Exception:
                log.exception("fake_delivery_cycle_failed provider=%s", self.provider)
            await asyncio.sleep(self.settings.poll_interval)


def create_app(
    *,
    engine: AsyncEngine | None = None,
    provider: str | None = None,
    settings: Settings | None = None,
    start_workers: bool = True,
) -> FastAPI:
    config = settings or Settings.from_env()
    db = engine if engine is not None else make_engine(config.database_url)
    name = provider or os.getenv("PROVIDER_NAME", "alpha")
    if name not in ("alpha", "beta"):
        raise ValueError("Неизвестный провайдер")

    @asynccontextmanager
    async def lifespan(app):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        worker = DeliveryWorker(db, config, name)
        running = asyncio.create_task(worker.loop()) if start_workers else None
        try:
            yield
        finally:
            if running is not None:
                running.cancel()
                await asyncio.gather(running, return_exceptions=True)
            await worker.close()
            if engine is None:
                await db.dispose()

    api = FastAPI(title="Фейковый провайдер " + name, lifespan=lifespan)
    api.state.settings = config
    api.state.behavior = FakeSettings.from_env()

    @api.get("/health")
    async def health() -> dict:
        return {"status": "ok", "provider": name}

    @api.post("/settings", dependencies=[Depends(verify_signature)], response_model=FakeSettings)
    async def configure(data: FakeSettings) -> FakeSettings:
        api.state.behavior = data
        return data

    @api.post("/tasks", status_code=202, response_model=dict[str, str])
    async def submit(data: Submission) -> dict | Response:
        request = data.model_dump(mode="json")
        behavior = api.state.behavior.model_dump()
        # Стабильный выбор, без случайных тестов и изменения уже принятого сценария.
        seed = hashlib.sha256((name + str(data.attempt_id)).encode()).digest()
        fraction = int.from_bytes(seed[:8], "big") / 2**64
        delay_fraction = int.from_bytes(seed[8:16], "big") / 2**64
        behavior["outcome"] = "failed" if fraction < behavior["failure_rate"] else "completed"
        if behavior["post_mode"] == "503":
            behavior["no_webhook"] = True
        delay = behavior["delay_min"] + delay_fraction * (
            behavior["delay_max"] - behavior["delay_min"]
        )
        external_id = name + "-" + str(data.attempt_id)
        async with db.begin() as conn:
            now = await conn.scalar(select(func.clock_timestamp()))
            await conn.execute(
                insert(fake_jobs)
                .values(
                    provider=name,
                    attempt_id=data.attempt_id,
                    external_id=external_id,
                    request=request,
                    behavior=behavior,
                    next_delivery=now + timedelta(seconds=delay),
                )
                .on_conflict_do_nothing(
                    index_elements=[fake_jobs.c.provider, fake_jobs.c.attempt_id]
                )
            )
            job = (
                (
                    await conn.execute(
                        select(fake_jobs).where(
                            fake_jobs.c.provider == name, fake_jobs.c.attempt_id == data.attempt_id
                        )
                    )
                )
                .mappings()
                .one()
            )
            if job["request"] != request:
                raise HTTPException(409, "Идентификатор попытки уже связан с другим запросом")
        if job["behavior"]["post_mode"] == "503":
            return Response(status_code=503)
        if job["behavior"]["post_mode"] == "timeout":
            await asyncio.sleep(job["behavior"]["post_delay"])
        return {"external_id": job["external_id"]}

    return api


app = create_app()
