import hashlib
import hmac

from fastapi import HTTPException, Request


def sign(body: bytes, secret: str) -> str:
    return hmac.new(secret.encode(), body, hashlib.sha256).hexdigest()


async def verify_signature(request: Request) -> None:
    actual = request.headers.get("X-Signature", "")
    expected = sign(await request.body(), request.app.state.settings.webhook_secret)
    if not hmac.compare_digest(actual.encode(), expected.encode()):
        raise HTTPException(401, "Отсутствует или неверна подпись")
