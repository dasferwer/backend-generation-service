import asyncio
import hashlib
import hmac
import json

SECRET = "demo-secret-change-me"


def signed(payload, secret=SECRET):
    body = json.dumps(payload, separators=(",", ":"), ensure_ascii=False).encode()
    signature = hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()
    return {
        "content": body,
        "headers": {"X-Signature": signature, "Content-Type": "application/json"},
    }


async def parallel(count, action):
    barrier = asyncio.Barrier(count)

    async def call(i):
        await barrier.wait()
        return await action(i)

    return await asyncio.wait_for(asyncio.gather(*(call(i) for i in range(count))), 15)


async def generate(client, **values):
    payload = {"user_id": 1, "model_slug": "image-fast", "prompt": "Река", "ref_images": []}
    payload.update(values)
    return await client.post("/generate", json=payload)


async def invariant(engine, user_id=1):
    from sqlalchemy import text

    async with engine.connect() as conn:
        row = (
            await conn.execute(
                text("""SELECT balance,
            (SELECT coalesce(sum(amount),0) FROM ledger WHERE user_id=:uid) AS total
            FROM users WHERE id=:uid"""),
                {"uid": user_id},
            )
        ).one()
        assert row.balance == row.total
        assert row.balance >= 0
        return row.balance
