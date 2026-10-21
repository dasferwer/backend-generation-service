import logging
from datetime import timedelta
from uuid import UUID, uuid4

from fastapi import HTTPException
from sqlalchemy import func, insert, select, update
from sqlalchemy.dialects.postgresql import insert as pg_insert
from sqlalchemy.ext.asyncio import AsyncConnection, AsyncEngine

from app.config import Settings
from app.db import attempts, ledger, legs, models, payments, tasks, users
from app.schemas import Generate, Payment

log = logging.getLogger(__name__)
OPEN_ATTEMPTS = ("queued", "sending", "waiting")


async def lock_user(conn: AsyncConnection, user_id: int):
    row = (
        (await conn.execute(select(users).where(users.c.id == user_id).with_for_update()))
        .mappings()
        .one_or_none()
    )
    if row is None:
        raise HTTPException(404, "Пользователь не найден")
    return row


async def add_attempt(
    conn: AsyncConnection, task_id: UUID, leg: dict, settings: Settings, now
) -> UUID:
    attempt_id = uuid4()
    await conn.execute(
        insert(attempts).values(
            id=attempt_id,
            task_id=task_id,
            provider=leg["provider"],
            priority=leg["priority"],
            status="queued",
            created_at=now,
            deadline_at=now + timedelta(seconds=settings.provider_timeout),
        )
    )
    await conn.execute(
        update(tasks).where(tasks.c.id == task_id).values(current_attempt_id=attempt_id)
    )
    return attempt_id


async def generate(engine: AsyncEngine, data: Generate, settings: Settings) -> UUID:
    async with engine.begin() as conn:
        user = await lock_user(conn, data.user_id)
        model = (
            (await conn.execute(select(models).where(models.c.slug == data.model_slug)))
            .mappings()
            .one_or_none()
        )
        if model is None:
            raise HTTPException(404, "Модель не найдена")
        plan = [
            dict(row)
            for row in (
                await conn.execute(
                    select(legs)
                    .where(legs.c.model_slug == data.model_slug)
                    .order_by(legs.c.priority)
                )
            ).mappings()
        ]
        suitable = [leg for leg in plan if len(data.ref_images) <= leg["max_ref_images"]]
        if not suitable:
            raise HTTPException(400, "Ни одна нога не принимает столько референсов")
        occupied = set(
            (
                await conn.scalars(
                    select(tasks.c.slot).where(
                        tasks.c.user_id == data.user_id, tasks.c.status == "active"
                    )
                )
            ).all()
        )
        if len(occupied) >= 3:
            raise HTTPException(429, "У пользователя уже три активные задачи")
        if user["balance"] < model["price"]:
            raise HTTPException(402, "Недостаточно токенов")
        task_id = uuid4()
        now = await conn.scalar(select(func.clock_timestamp()))
        await conn.execute(
            insert(tasks).values(
                id=task_id,
                user_id=data.user_id,
                model_slug=data.model_slug,
                price=model["price"],
                slot=next(slot for slot in (1, 2, 3) if slot not in occupied),
                prompt=data.prompt,
                ref_images=data.ref_images,
                plan=plan,
                status="active",
                created_at=now,
            )
        )
        await conn.execute(
            insert(ledger).values(
                user_id=data.user_id, task_id=task_id, kind="debit", amount=-model["price"]
            )
        )
        await add_attempt(conn, task_id, suitable[0], settings, now)
    log.info("task_created task_id=%s", task_id)
    return task_id


async def pay(engine: AsyncEngine, data: Payment) -> None:
    async with engine.begin() as conn:
        existing = (
            (await conn.execute(select(payments).where(payments.c.payment_id == data.payment_id)))
            .mappings()
            .one_or_none()
        )
        if existing is not None and (existing["user_id"], existing["tokens"]) != (
            data.user_id,
            data.tokens,
        ):
            raise HTTPException(409, "payment_id уже связан с другими данными")
        await lock_user(conn, data.user_id)
        await conn.execute(
            pg_insert(payments)
            .values(
                payment_id=data.payment_id,
                user_id=data.user_id,
                tokens=data.tokens,
                status="pending",
                created_at=func.now(),
            )
            .on_conflict_do_nothing(index_elements=[payments.c.payment_id])
        )
        payment = (
            (
                await conn.execute(
                    select(payments)
                    .where(payments.c.payment_id == data.payment_id)
                    .with_for_update()
                )
            )
            .mappings()
            .one()
        )
        if (payment["user_id"], payment["tokens"]) != (data.user_id, data.tokens):
            raise HTTPException(409, "payment_id уже связан с другими данными")
        if payment["status"] == "succeeded":
            return
        if data.status == "succeeded":
            await conn.execute(
                pg_insert(ledger)
                .values(
                    user_id=data.user_id,
                    payment_id=data.payment_id,
                    kind="payment",
                    amount=data.tokens,
                )
                .on_conflict_do_nothing(index_elements=[ledger.c.payment_id])
            )
        await conn.execute(
            update(payments)
            .where(payments.c.payment_id == data.payment_id)
            .values(status=data.status)
        )


async def finish(
    engine: AsyncEngine,
    attempt_id: UUID,
    settings: Settings,
    *,
    provider: str | None = None,
    result: str | None = None,
    error_type: str | None = None,
    error_detail: str | None = None,
    external_id: str | None = None,
    lease_token: UUID | None = None,
    expired_only: bool = False,
) -> bool:
    """Вебхук, ответ отправителя и сторож сходятся в одном переходе под блокировками."""
    async with engine.begin() as conn:
        owner = (
            await conn.execute(
                select(tasks.c.id, tasks.c.user_id)
                .join(attempts, attempts.c.task_id == tasks.c.id)
                .where(attempts.c.id == attempt_id)
            )
        ).one_or_none()
        if owner is None:
            raise HTTPException(404, "Попытка не найдена")
        # Отправитель захватывает только attempt; здесь всегда user -> task -> attempt.
        await lock_user(conn, owner.user_id)
        task = (
            (await conn.execute(select(tasks).where(tasks.c.id == owner.id).with_for_update()))
            .mappings()
            .one()
        )
        attempt = (
            (
                await conn.execute(
                    select(attempts).where(attempts.c.id == attempt_id).with_for_update()
                )
            )
            .mappings()
            .one()
        )
        if provider is not None and provider != attempt["provider"]:
            raise HTTPException(409, "Попытка принадлежит другому провайдеру")
        if task["status"] != "active" or attempt["status"] not in OPEN_ATTEMPTS:
            return False
        if task["current_attempt_id"] != attempt_id:
            return False
        now = await conn.scalar(select(func.clock_timestamp()))
        if lease_token is not None and (
            attempt["lease_token"] != lease_token or attempt["lease_until"] <= now
        ):
            return False
        if expired_only and attempt["deadline_at"] > now:
            return False
        status = "completed" if result is not None else "failed"
        await conn.execute(
            update(attempts)
            .where(attempts.c.id == attempt_id)
            .values(
                status=status,
                error_type=error_type,
                error_detail=error_detail,
                finished_at=now,
                lease_token=None,
                lease_until=None,
                external_id=external_id or attempt["external_id"],
            )
        )
        if result is not None:
            await conn.execute(
                update(tasks)
                .where(tasks.c.id == task["id"])
                .values(status="completed", result=result, finished_at=now)
            )
        else:
            next_leg = next(
                (
                    leg
                    for leg in task["plan"]
                    if leg["priority"] > attempt["priority"]
                    and len(task["ref_images"]) <= leg["max_ref_images"]
                    and not (error_type == "moderation" and leg["skip_on_moderation"])
                ),
                None,
            )
            if next_leg is not None and error_type != "payload":
                await add_attempt(conn, task["id"], next_leg, settings, now)
            else:
                await conn.execute(
                    update(tasks)
                    .where(tasks.c.id == task["id"])
                    .values(status="failed", finished_at=now)
                )
                await conn.execute(
                    insert(ledger).values(
                        user_id=task["user_id"],
                        task_id=task["id"],
                        kind="refund",
                        amount=task["price"],
                    )
                )
    log.info(
        "attempt_finished task_id=%s attempt_id=%s provider=%s status=%s error=%s",
        owner.id,
        attempt_id,
        attempt["provider"],
        status,
        error_type,
    )
    return True
