import asyncio
from datetime import timedelta

import httpx
from sqlalchemy import func, select, update

from app.config import Settings
from app.db import attempts, fake_jobs
from app.worker import Worker
from tests.helpers import invariant, signed
from tests.test_transitions import task


async def test_provider_idempotent_acceptance_survives_lost_ack(client, engine):
    from app.fake_provider import create_app

    provider = create_app(engine=engine, provider="alpha", start_workers=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=provider), base_url="http://alpha"
    ) as fake:
        settings = {"delay_min": 0, "delay_max": 0, "failure_rate": 0, "duplicates": 2}
        assert (await fake.post("/settings", **signed(settings))).status_code == 200
        t = await task(client)

        async def lose_ack(request):
            response = await fake.post(
                "/tasks", content=request.content, headers={"Content-Type": "application/json"}
            )
            assert response.status_code == 202
            raise asyncio.CancelledError

        async with httpx.AsyncClient(transport=httpx.MockTransport(lose_ack)) as lost:
            worker = Worker(engine, Settings(), client=lost)
            try:
                await worker.dispatch_once()
            except asyncio.CancelledError:
                pass
            else:
                raise AssertionError("Должно имитироваться падение отправителя")
        async with engine.begin() as conn:
            await conn.execute(
                update(attempts).values(lease_until=func.now() - timedelta(seconds=1))
            )
        worker = Worker(engine, Settings(), client=fake)
        assert await worker.dispatch_once()
        async with engine.connect() as conn:
            jobs = (await conn.execute(select(fake_jobs))).mappings().all()
            assert len(jobs) == 1
            assert jobs[0]["behavior"]["duplicates"] == 2
            assert str(jobs[0]["attempt_id"]) == t["current_leg"]["id"]
            assert await conn.scalar(select(attempts.c.send_count)) == 2
        assert await invariant(engine) == 98


async def test_provider_snapshot_duplicates_and_restart(client, engine):
    from app.fake_provider import DeliveryWorker, create_app

    provider = create_app(engine=engine, provider="alpha", start_workers=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=provider), base_url="http://alpha"
    ) as fake:
        assert (
            await fake.post(
                "/settings", **signed({"delay_min": 0, "delay_max": 0, "duplicates": 2})
            )
        ).status_code == 200
        t = await task(client)
        worker = Worker(engine, Settings(), client=fake)
        assert await worker.dispatch_once()
        assert (
            await fake.post(
                "/settings",
                **signed({"failure_rate": 1, "error_type": "payload", "no_webhook": True}),
            )
        ).status_code == 200
    deliveries = 0

    async def send_to_api(request):
        nonlocal deliveries
        deliveries += 1
        return await client.post(
            "/webhooks/providers/alpha", content=request.content, headers=dict(request.headers)
        )

    # Новый обработчик читает сохранённый сценарий, а не новые настройки приложения.
    async with httpx.AsyncClient(transport=httpx.MockTransport(send_to_api)) as callback:
        delivery = DeliveryWorker(engine, Settings(), "alpha", client=callback)
        assert await delivery.deliver_once()
    assert deliveries == 2
    final = (await client.get("/tasks/" + t["task_id"])).json()
    assert final["status"] == "completed"
    assert await invariant(engine) == 98


async def test_fake_rejects_changed_payload_and_retries_callback(engine):
    from uuid import uuid4

    from app.fake_provider import DeliveryWorker, create_app

    provider = create_app(engine=engine, provider="alpha", start_workers=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=provider), base_url="http://alpha"
    ) as fake:
        assert (
            await fake.post("/settings", **signed({"delay_min": 0, "delay_max": 0}))
        ).status_code == 200
        p = {"attempt_id": str(uuid4()), "task_id": str(uuid4()), "prompt": "x", "ref_images": []}
        response = await fake.post("/tasks", json=p)
        assert response.status_code == 202
        assert (await fake.post("/tasks", json=p)).json() == response.json()
        assert (await fake.post("/tasks", json=p | {"prompt": "changed"})).status_code == 409
    async with httpx.AsyncClient(
        transport=httpx.MockTransport(lambda r: httpx.Response(500))
    ) as http:
        worker = DeliveryWorker(engine, Settings(), "alpha", client=http)
        assert await worker.deliver_once()
    async with engine.connect() as conn:
        row = (await conn.execute(select(fake_jobs))).mappings().one()
        assert row["delivered_at"] is None and row["delivery_count"] == 1
        assert row["lease_token"] is None
