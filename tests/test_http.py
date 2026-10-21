"""Проверки через настоящий HTTP к четырём процессам API и двум провайдерам."""

import asyncio
import os
import time
from uuid import UUID, uuid4

import httpx
import pytest
from sqlalchemy import func, select, text, update
from sqlalchemy.ext.asyncio import create_async_engine

from app.db import attempts, fake_jobs
from tests.helpers import parallel, signed

pytestmark = pytest.mark.skipif(not os.getenv("TEST_API_URL"), reason="Нужен Docker Compose")


@pytest.fixture
async def http_db():
    engine = create_async_engine(os.environ["TEST_DATABASE_URL"])
    try:
        yield engine
    finally:
        await engine.dispose()


@pytest.fixture
async def live(http_db):
    user_id = uuid4().int % 900_000_000 + 2
    async with http_db.begin() as conn:
        await conn.execute(text("INSERT INTO users(id) VALUES(:uid)"), {"uid": user_id})
        await conn.execute(
            text("INSERT INTO ledger(user_id,kind,amount) VALUES(:uid,'initial',100)"),
            {"uid": user_id},
        )
    async with httpx.AsyncClient(
        base_url=os.environ["TEST_API_URL"], timeout=10, limits=httpx.Limits(max_connections=30)
    ) as api:
        for provider in ("alpha", "beta"):
            await configure(provider)
        yield api, user_id


async def configure(provider, **settings):
    url = os.getenv(provider.upper() + "_URL", "http://" + provider + ":8000")
    async with httpx.AsyncClient(timeout=5) as client:
        response = await client.post(
            url + "/settings",
            **signed(
                {"delay_min": 0.1, "delay_max": 0.1} | settings,
                os.getenv("WEBHOOK_SECRET", "demo-secret-change-me"),
            ),
        )
        assert response.status_code == 200, response.text


async def create(live, **values):
    api, user_id = live
    response = await api.post(
        "/generate",
        json={
            "user_id": user_id,
            "model_slug": "image-fast",
            "prompt": "Море",
            "ref_images": [],
        }
        | values,
    )
    assert response.status_code == 202, response.text
    return response.json()["task_id"]


async def wait_task(api, task_id, predicate, wait_seconds=10):
    async with asyncio.timeout(wait_seconds):
        while True:
            response = await api.get("/tasks/" + task_id)
            assert response.status_code == 200, response.text
            task = response.json()
            if predicate(task):
                return task
            await asyncio.sleep(0.05)


async def money(live, http_db, expected, **kinds):
    api, user_id = live
    response = await api.get(f"/users/{user_id}/balance?limit=100")
    assert response.status_code == 200
    body = response.json()
    assert body["balance"] == expected == sum(row["amount"] for row in body["ledger"])
    async with http_db.connect() as conn:
        actual = dict(
            (
                await conn.execute(
                    text("SELECT kind,count(*) FROM ledger WHERE user_id=:uid GROUP BY kind"),
                    {"uid": user_id},
                )
            ).all()
        )
    assert actual == {"initial": 1} | kinds


async def webhook(api, attempt, status="failed", **extra):
    payload = {"attempt_id": attempt["id"], "status": status}
    payload.update(
        {"result": "https://result.example/late"}
        if status == "completed"
        else {"error_type": "technical"}
    )
    response = await api.post(
        "/webhooks/providers/" + attempt["provider"],
        **signed(payload | extra, os.getenv("WEBHOOK_SECRET", "demo-secret-change-me")),
    )
    assert response.status_code == 200, response.text
    return response


async def expire(http_db, attempt):
    async with http_db.begin() as conn:
        await conn.execute(
            update(attempts)
            .where(attempts.c.id == UUID(attempt["id"]))
            .values(deadline_at=func.clock_timestamp())
        )


async def test_http_twenty_payments(live, http_db):
    api, user_id = live
    p = {"payment_id": str(uuid4()), "user_id": user_id, "tokens": 13, "status": "succeeded"}
    responses = await parallel(20, lambda _: api.post("/webhooks/payments", **signed(p)))
    assert [r.status_code for r in responses] == [200] * 20
    await money(live, http_db, 113, payment=1)


async def test_http_ten_last_leg_failures(live, http_db):
    await configure("beta", no_webhook=True)
    api, _ = live
    task_id = await create(live, ref_images=["ref"] * 5)
    t = await wait_task(api, task_id, lambda t: t["current_leg"]["status"] == "waiting")
    await parallel(10, lambda _: webhook(api, t["current_leg"]))
    t = await wait_task(api, task_id, lambda t: t["status"] == "failed")
    assert t["result"] is None and len(t["attempts"]) == 1
    await money(live, http_db, 100, debit=1, refund=1)


async def test_http_real_fallback_with_duplicates(live, http_db):
    await configure("alpha", failure_rate=1, error_type="technical", duplicates=2)
    await configure("beta", duplicates=2)
    api, _ = live
    task_id = await create(live)
    t = await wait_task(api, task_id, lambda t: t["status"] == "completed")
    assert [a["provider"] for a in t["attempts"]] == ["alpha", "beta"]
    assert [a["status"] for a in t["attempts"]] == ["failed", "completed"]
    assert t["result"].startswith("https://results.example/beta/")
    await money(live, http_db, 98, debit=1)


@pytest.mark.parametrize("last_leg", [False, True])
async def test_http_late_completed(live, http_db, last_leg):
    await configure("alpha", no_webhook=True)
    await configure("beta", no_webhook=True)
    api, _ = live
    task_id = await create(live, ref_images=["ref"] * 5 if last_leg else [])
    t = await wait_task(api, task_id, lambda t: t["current_leg"]["status"] == "waiting")
    old = t["current_leg"]
    await expire(http_db, old)
    t = await wait_task(
        api,
        task_id,
        lambda t: t["status"] == "failed" if last_leg else t["current_leg"]["provider"] == "beta",
    )
    await webhook(api, old, "completed")
    t = (await api.get("/tasks/" + task_id)).json()
    assert t["result"] is None and t["status"] == ("failed" if last_leg else "active")
    if not last_leg:
        await webhook(api, t["current_leg"], "completed", result="https://result.example/new")
        t = (await api.get("/tasks/" + task_id)).json()
        assert t["result"] == "https://result.example/new"
    await money(
        live, http_db, 100 if last_leg else 98, debit=1, **({"refund": 1} if last_leg else {})
    )


@pytest.mark.parametrize(("mode", "detail"), [("503", "http_503"), ("timeout", "post_timeout")])
async def test_http_synchronous_failures(live, http_db, mode, detail):
    await configure("alpha", post_mode=mode, no_webhook=True)
    api, _ = live
    task_id = await create(live)
    t = await wait_task(api, task_id, lambda t: t["status"] == "completed")
    assert t["attempts"][0]["error_detail"] == detail
    assert [a["provider"] for a in t["attempts"]] == ["alpha", "beta"]
    await money(live, http_db, 98, debit=1)


async def test_http_early_callback(live, http_db):
    await configure("alpha", delay_min=0, delay_max=0, post_mode="timeout", post_delay=2)
    api, _ = live
    task_id = await create(live)
    t = await wait_task(api, task_id, lambda t: t["status"] == "completed")
    assert t["attempts"][0]["external_id"] is not None
    assert len(t["attempts"]) == 1
    await money(live, http_db, 98, debit=1)


async def test_http_concurrent_generate_and_signature(live, http_db):
    await configure("alpha", no_webhook=True)
    api, user_id = live
    p = {"payment_id": str(uuid4()), "user_id": user_id, "tokens": 20, "status": "succeeded"}
    assert (await api.post("/webhooks/payments", **signed(p, "wrong"))).status_code == 401
    base = {"user_id": user_id, "model_slug": "image-fast", "prompt": "x", "ref_images": []}
    assert (await api.post("/generate", json=base | {"ref_images": ["r"] * 11})).status_code == 400
    await money(live, http_db, 100)
    results = await parallel(12, lambda _: api.post("/generate", json=base))
    assert sorted(r.status_code for r in results) == [202] * 3 + [429] * 9
    await money(live, http_db, 94, debit=3)
    for response in results:
        if response.status_code == 202:
            t = (await api.get("/tasks/" + response.json()["task_id"])).json()
            await webhook(api, t["current_leg"], error_type="payload")
    await money(live, http_db, 100, debit=3, refund=3)


@pytest.mark.slow
async def test_http_real_sixty_second_timeout(live, http_db):
    # Здесь сроки не переписываются: провайдер действительно завершает после 60 секунд.
    await configure("beta", delay_min=62, delay_max=62)
    api, _ = live
    start = time.monotonic()
    task_id = await create(live, ref_images=["ref"] * 5)
    initial = (await api.get("/tasks/" + task_id)).json()
    from datetime import datetime

    attempt = initial["current_leg"]
    assert (
        datetime.fromisoformat(attempt["deadline_at"])
        - datetime.fromisoformat(attempt["created_at"])
    ).total_seconds() == 60
    t = await wait_task(api, task_id, lambda t: t["status"] == "failed", wait_seconds=67)
    assert time.monotonic() - start >= 59
    assert t["current_leg"]["error_detail"] == "provider_timeout"
    await money(live, http_db, 100, debit=1, refund=1)
    async with asyncio.timeout(10):
        while True:
            async with http_db.connect() as conn:
                delivered = await conn.scalar(
                    select(fake_jobs.c.delivered_at).where(
                        fake_jobs.c.provider == "beta",
                        fake_jobs.c.attempt_id == UUID(attempt["id"]),
                    )
                )
            if delivered is not None:
                break
            await asyncio.sleep(0.05)
    t = (await api.get("/tasks/" + task_id)).json()
    assert t["status"] == "failed" and t["result"] is None
    await money(live, http_db, 100, debit=1, refund=1)


@pytest.mark.parametrize(
    ("provider", "model", "error"),
    [
        ("beta", "video-long", "moderation"),
        ("alpha", "image-fast", "payload"),
    ],
)
async def test_http_terminal_failure_routing(live, http_db, provider, model, error):
    await configure(provider, failure_rate=1, error_type=error)
    api, _ = live
    task_id = await create(live, model_slug=model)
    t = await wait_task(api, task_id, lambda t: t["status"] == "failed")
    assert len(t["attempts"]) == 1
    assert t["current_leg"]["provider"] == provider
    assert t["current_leg"]["error_type"] == error
    assert t["result"] is None
    await money(live, http_db, 100, debit=1, refund=1)
