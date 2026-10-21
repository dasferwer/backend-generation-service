from datetime import timedelta
from uuid import UUID, uuid4

import pytest
from sqlalchemy import func, text, update

from app.config import Settings
from app.db import attempts
from app.worker import Worker
from tests.helpers import generate, invariant, parallel, signed


async def task(client, **kwargs):
    response = await generate(client, **kwargs)
    assert response.status_code == 202, response.text
    return (await client.get("/tasks/" + response.json()["task_id"])).json()


async def event(client, attempt, status="failed", **values):
    payload = {"attempt_id": attempt["id"], "status": status}
    payload.update(
        {"result": "https://result.example/image"}
        if status == "completed"
        else {"error_type": "technical"}
    )
    payload.update(values)
    return await client.post("/webhooks/providers/" + attempt["provider"], **signed(payload))


async def expire(engine, attempt_id):
    async with engine.begin() as conn:
        await conn.execute(
            update(attempts)
            .where(attempts.c.id == UUID(attempt_id))
            .values(deadline_at=func.clock_timestamp() - timedelta(seconds=1))
        )


async def counts(engine):
    async with engine.connect() as conn:
        return dict(
            (await conn.execute(text("SELECT kind,count(*) FROM ledger GROUP BY kind"))).all()
        )


async def test_ten_failures_last_leg_refund_once(client, engine):
    t = await task(client, ref_images=["ref"] * 5)
    responses = await parallel(10, lambda _: event(client, t["current_leg"]))
    assert [r.status_code for r in responses] == [200] * 10
    final = (await client.get("/tasks/" + t["task_id"])).json()
    assert final["status"] == "failed" and final["result"] is None
    assert len(final["attempts"]) == 1
    assert await counts(engine) == {"initial": 1, "debit": 1, "refund": 1}
    assert await invariant(engine) == 100


async def test_fallback_completed(client, engine):
    t = await task(client)
    assert (await event(client, t["current_leg"])).status_code == 200
    t = (await client.get("/tasks/" + t["task_id"])).json()
    assert t["current_leg"]["provider"] == "beta"
    assert (await event(client, t["current_leg"], "completed")).status_code == 200
    t = (await client.get("/tasks/" + t["task_id"])).json()
    assert t["status"] == "completed" and t["result"] == "https://result.example/image"
    assert [a["provider"] for a in t["attempts"]] == ["alpha", "beta"]
    assert [a["status"] for a in t["attempts"]] == ["failed", "completed"]
    assert await counts(engine) == {"initial": 1, "debit": 1}
    assert await invariant(engine) == 98


@pytest.mark.parametrize(
    ("model", "error", "attempt_count", "final_status"),
    [
        ("video-long", "moderation", 1, "failed"),
        ("image-fast", "moderation", 2, "active"),
        ("image-fast", "payload", 1, "failed"),
    ],
)
async def test_failure_routing(client, engine, model, error, attempt_count, final_status):
    t = await task(client, model_slug=model)
    assert (await event(client, t["current_leg"], error_type=error)).status_code == 200
    t = (await client.get("/tasks/" + t["task_id"])).json()
    assert t["status"] == final_status and len(t["attempts"]) == attempt_count
    assert await invariant(engine) == (100 if final_status == "failed" else 98)
    assert (await counts(engine)).get("refund", 0) == int(final_status == "failed")


@pytest.mark.parametrize("last_leg", [False, True])
async def test_late_completed_after_watchdog(client, engine, last_leg):
    t = await task(client, ref_images=["ref"] * 5 if last_leg else [])
    old = t["current_leg"]
    await expire(engine, old["id"])
    worker = Worker(engine, Settings())
    assert await worker.watchdog_once() == 1
    t = (await client.get("/tasks/" + t["task_id"])).json()
    assert t["status"] == ("failed" if last_leg else "active")
    assert (await event(client, old, "completed")).status_code == 200
    t = (await client.get("/tasks/" + t["task_id"])).json()
    assert t["result"] is None
    if not last_leg:
        assert t["current_leg"]["provider"] == "beta"
        assert (
            await event(client, t["current_leg"], "completed", result="https://result.example/beta")
        ).status_code == 200
        t = (await client.get("/tasks/" + t["task_id"])).json()
        assert t["result"] == "https://result.example/beta"
    assert (await counts(engine)).get("refund", 0) == int(last_leg)
    assert await invariant(engine) == (100 if last_leg else 98)
    await worker.close()


async def test_claim_recovery_and_fencing(client, engine):
    t = await task(client)
    worker = Worker(engine, Settings())
    old = await worker.claim()
    assert old is not None
    assert str(old["id"]) == t["current_leg"]["id"]
    async with engine.begin() as conn:
        await conn.execute(
            update(attempts)
            .where(attempts.c.id == old["id"])
            .values(lease_until=func.now() - timedelta(seconds=1))
        )
    new = await worker.claim()
    assert new is not None and new["id"] == old["id"]
    assert new["lease_token"] != old["lease_token"]
    assert not await worker.acknowledge(old, "obsolete")
    assert await worker.acknowledge(new, "accepted")
    current = (await client.get("/tasks/" + t["task_id"])).json()["current_leg"]
    assert current["external_id"] == "accepted" and current["status"] == "waiting"
    assert await invariant(engine) == 98
    await worker.close()


async def test_many_watchdogs_vs_completed(client, engine):
    t = await task(client, ref_images=["ref"] * 5)
    await expire(engine, t["current_leg"]["id"])
    worker = Worker(engine, Settings())

    async def race(i):
        if i == 0:
            return await event(client, t["current_leg"], "completed")
        return await worker.watchdog_once()

    await parallel(10, race)
    t = (await client.get("/tasks/" + t["task_id"])).json()
    assert t["status"] in ("completed", "failed")
    refunded = (await counts(engine)).get("refund", 0)
    assert refunded == int(t["status"] == "failed")
    assert (t["result"] is not None) == (refunded == 0)
    assert await invariant(engine) == (100 if refunded else 98)
    await worker.close()


async def test_provider_belongs_to_attempt_and_unknowns(client, engine):
    t = await task(client)
    p = {
        "attempt_id": t["current_leg"]["id"],
        "status": "completed",
        "result": "https://result.example/unknown",
    }
    assert (await client.post("/webhooks/providers/beta", **signed(p))).status_code == 409
    assert (await client.post("/webhooks/providers/absent", **signed(p))).status_code == 404
    assert (await client.post("/webhooks/providers/alpha", **signed(p, "wrong"))).status_code == 401
    p["attempt_id"] = str(uuid4())
    assert (await client.post("/webhooks/providers/alpha", **signed(p))).status_code == 404
    assert await invariant(engine) == 98


async def test_price_snapshot_refund(client, engine):
    t = await task(client)
    async with engine.begin() as conn:
        await conn.execute(text("UPDATE models SET price=99 WHERE slug='image-fast'"))
    assert (await event(client, t["current_leg"], error_type="payload")).status_code == 200
    assert await invariant(engine) == 100


@pytest.mark.parametrize(
    ("failure", "error"),
    [
        ("503", "technical"),
        ("timeout", "technical"),
        ("400", "payload"),
        ("bad-response", "technical"),
    ],
)
async def test_dispatch_failures(client, engine, failure, error):
    import httpx

    t = await task(client)

    async def handle(request):
        if failure == "timeout":
            raise httpx.ReadTimeout("Нет ответа", request=request)
        if failure == "bad-response":
            return httpx.Response(202, json={"external_id": ""})
        return httpx.Response(int(failure))

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        worker = Worker(engine, Settings(), client=http)
        assert await worker.dispatch_once()
    final = (await client.get("/tasks/" + t["task_id"])).json()
    assert final["attempts"][0]["error_type"] == error
    assert len(final["attempts"]) == (1 if error == "payload" else 2)
    assert await invariant(engine) == (100 if error == "payload" else 98)


async def test_webhook_before_external_id_is_saved(client, engine):
    import httpx

    t = await task(client)

    async def handle(request):
        assert (await event(client, t["current_leg"], "completed")).status_code == 200
        return httpx.Response(202, json={"external_id": "late-post-response"})

    async with httpx.AsyncClient(transport=httpx.MockTransport(handle)) as http:
        worker = Worker(engine, Settings(), client=http)
        assert await worker.dispatch_once()
    final = (await client.get("/tasks/" + t["task_id"])).json()
    assert final["status"] == "completed"
    assert final["current_leg"]["status"] == "completed"
    assert final["current_leg"]["external_id"] is None
    assert await counts(engine) == {"initial": 1, "debit": 1}
    assert await invariant(engine) == 98


async def test_webhook_database_error_rolls_back_and_can_retry(client, engine):
    import httpx

    from app.api import create_app

    t = await task(client, ref_images=["ref"] * 5)
    async with engine.begin() as conn:
        await conn.execute(
            text("""CREATE FUNCTION fail_refund() RETURNS trigger
            LANGUAGE plpgsql AS $$ BEGIN IF NEW.kind='refund' THEN
            RAISE EXCEPTION 'Имитируем обрыв транзакции'; END IF; RETURN NEW; END $$""")
        )
        await conn.execute(
            text(
                "CREATE TRIGGER fail_refund BEFORE INSERT ON ledger "
                "FOR EACH ROW EXECUTE FUNCTION fail_refund()"
            )
        )
    app = create_app(engine=engine, start_workers=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app, raise_app_exceptions=False), base_url="http://test"
    ) as error_client:
        assert (await event(error_client, t["current_leg"])).status_code == 500
    final = (await client.get("/tasks/" + t["task_id"])).json()
    assert final["status"] == "active" and final["attempts"][0]["finished_at"] is None
    assert await counts(engine) == {"initial": 1, "debit": 1}
    assert await invariant(engine) == 98
    async with engine.begin() as conn:
        await conn.execute(text("DROP TRIGGER fail_refund ON ledger"))
    assert (await event(client, t["current_leg"])).status_code == 200
    assert await invariant(engine) == 100
    assert await counts(engine) == {"initial": 1, "debit": 1, "refund": 1}


async def test_simultaneous_success_and_failure(client, engine):
    t = await task(client, ref_images=["ref"] * 5)
    responses = await parallel(
        10, lambda i: event(client, t["current_leg"], "completed" if i % 2 else "failed")
    )
    assert [r.status_code for r in responses] == [200] * 10
    t = (await client.get("/tasks/" + t["task_id"])).json()
    refunded = (await counts(engine)).get("refund", 0)
    assert refunded == int(t["status"] == "failed")
    assert (t["result"] is not None) == (refunded == 0)
    assert await invariant(engine) == (100 if refunded else 98)
