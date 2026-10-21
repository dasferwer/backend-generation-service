from uuid import uuid4

import pytest
from sqlalchemy import text

from tests.helpers import generate, invariant, parallel, signed


async def test_twenty_payment_duplicates(client, engine):
    payment = {"payment_id": "same-payment", "user_id": 1, "tokens": 13, "status": "succeeded"}
    responses = await parallel(20, lambda _: client.post("/webhooks/payments", **signed(payment)))
    assert [r.status_code for r in responses] == [200] * 20
    assert await invariant(engine) == 113
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM ledger WHERE kind='payment'")) == 1


async def test_failed_payment_can_succeed_and_conflicts(client, engine):
    p = {"payment_id": "recoverable", "user_id": 1, "tokens": 9, "status": "failed"}
    assert (await client.post("/webhooks/payments", **signed(p))).status_code == 200
    assert await invariant(engine) == 100
    p["status"] = "succeeded"
    assert (await client.post("/webhooks/payments", **signed(p))).status_code == 200
    for changed in ({"tokens": 10}, {"user_id": 2}):
        async with engine.begin() as conn:
            await conn.execute(text("INSERT INTO users(id) VALUES(2) ON CONFLICT DO NOTHING"))
        assert (await client.post("/webhooks/payments", **signed(p | changed))).status_code == 409
    p["status"] = "pending"
    assert (await client.post("/webhooks/payments", **signed(p))).status_code == 200
    assert await invariant(engine) == 109


@pytest.mark.parametrize("tokens", [0, -1, True, 1.5, "3"])
async def test_payment_requires_positive_integer(client, engine, tokens):
    p = {"payment_id": "invalid", "user_id": 1, "tokens": tokens, "status": "succeeded"}
    assert (await client.post("/webhooks/payments", **signed(p))).status_code == 422
    assert await invariant(engine) == 100


async def test_bad_signature_cannot_change_state(client, engine):
    p = {"payment_id": "bad-sign", "user_id": 1, "tokens": 20, "status": "succeeded"}
    assert (await client.post("/webhooks/payments", json=p)).status_code == 401
    assert (await client.post("/webhooks/payments", **signed(p, "wrong"))).status_code == 401
    assert await invariant(engine) == 100


async def test_ref_filter_precedes_debit(client, engine):
    assert (await generate(client, ref_images=["x"] * 11)).status_code == 400
    assert await invariant(engine) == 100
    response = await generate(client, ref_images=["x"] * 5)
    assert response.status_code == 202
    task = (await client.get("/tasks/" + response.json()["task_id"])).json()
    assert [a["provider"] for a in task["attempts"]] == ["beta"]
    assert await invariant(engine) == 98


async def test_generate_serializes_active_limit(client, engine):
    responses = await parallel(12, lambda _: generate(client))
    assert sorted(r.status_code for r in responses) == [202] * 3 + [429] * 9
    assert await invariant(engine) == 94
    async with engine.connect() as conn:
        assert await conn.scalar(text("SELECT count(*) FROM attempts")) == 3
        assert await conn.scalar(text("SELECT count(*) FROM ledger WHERE kind='debit'")) == 3


async def test_insufficient_balance_is_serialized(client, engine):
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO users(id) VALUES(3)"))
        await conn.execute(text("INSERT INTO ledger(user_id,kind,amount) VALUES(3,'initial',2)"))
    responses = await parallel(8, lambda _: generate(client, user_id=3))
    assert sorted(r.status_code for r in responses) == [202] + [402] * 7
    assert await invariant(engine, 3) == 0


async def test_unknown_identifiers_and_validation(client):
    assert (await client.get(f"/tasks/{uuid4()}")).status_code == 404
    assert (await client.get("/users/999/balance")).status_code == 404
    assert (await generate(client, user_id=999)).status_code == 404
    assert (await generate(client, model_slug="absent")).status_code == 404
    assert (await generate(client, prompt="   ")).status_code == 422
    assert (await generate(client, ref_images=[""])).status_code == 422


async def test_balance_order_and_database_guards(client, engine):
    for i in range(3):
        p = {"payment_id": str(i), "user_id": 1, "tokens": 1, "status": "succeeded"}
        assert (await client.post("/webhooks/payments", **signed(p))).status_code == 200
    response = (await client.get("/users/1/balance?limit=2")).json()
    assert response["balance"] == 103
    assert len(response["ledger"]) == 2
    assert response["ledger"][0]["id"] > response["ledger"][1]["id"]
    from sqlalchemy.exc import DBAPIError

    with pytest.raises(DBAPIError):
        async with engine.begin() as conn:
            await conn.execute(text("UPDATE users SET balance=999 WHERE id=1"))
    assert await invariant(engine) == 103


async def test_concurrent_payment_conflicting_users(client, engine):
    async with engine.begin() as conn:
        await conn.execute(text("INSERT INTO users(id) VALUES(2)"))

    async def request(i):
        p = {"payment_id": "cross-user", "user_id": i + 1, "tokens": 9, "status": "succeeded"}
        return await client.post("/webhooks/payments", **signed(p))

    responses = await parallel(2, request)
    assert sorted(r.status_code for r in responses) == [200, 409]
    assert await invariant(engine, 1) + await invariant(engine, 2) == 109


async def test_balance_and_ledger_share_snapshot(client, engine):
    async def request(i):
        if i % 2:
            p = {"payment_id": str(i), "user_id": 1, "tokens": 1, "status": "succeeded"}
            assert (await client.post("/webhooks/payments", **signed(p))).status_code == 200
        else:
            r = await client.get("/users/1/balance?limit=100")
            assert r.status_code == 200
            assert r.json()["balance"] == sum(row["amount"] for row in r.json()["ledger"])

    await parallel(20, request)
    assert await invariant(engine) == 110
