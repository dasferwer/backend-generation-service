import asyncio
import logging
from contextlib import asynccontextmanager
from uuid import UUID

from fastapi import Depends, FastAPI, HTTPException, Path, Query
from sqlalchemy import select, text
from sqlalchemy.ext.asyncio import AsyncEngine

from app import service
from app.config import Settings
from app.db import attempts, ledger, make_engine, tasks, users
from app.schemas import (
    Acknowledged,
    BalanceView,
    Created,
    Generate,
    Payment,
    ProviderEvent,
    TaskView,
)
from app.security import verify_signature


def create_app(
    *,
    engine: AsyncEngine | None = None,
    settings: Settings | None = None,
    start_workers: bool = True,
) -> FastAPI:
    config = settings or Settings.from_env()
    db = engine if engine is not None else make_engine(config.database_url)

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        logging.basicConfig(level=logging.INFO, format="%(asctime)s %(levelname)s %(message)s")
        running = []
        worker = None
        if start_workers:
            from app.worker import Worker

            worker = Worker(db, config)
            running = [
                asyncio.create_task(worker.dispatch_loop()) for _ in range(config.send_concurrency)
            ]
            running.append(asyncio.create_task(worker.watchdog_loop()))
        try:
            yield
        finally:
            for task in running:
                task.cancel()
            await asyncio.gather(*running, return_exceptions=True)
            if worker is not None:
                await worker.close()
            if engine is None:
                await db.dispose()

    api = FastAPI(title="Сервис генерации", lifespan=lifespan)
    api.state.settings = config
    api.state.engine = db

    @api.get("/health")
    async def health() -> dict[str, str]:
        async with db.connect() as conn:
            await conn.execute(text("SELECT 1"))
        return {"status": "ok"}

    @api.post("/generate", status_code=202, response_model=Created)
    async def generate(data: Generate) -> Created:
        return Created(task_id=await service.generate(db, data, config))

    @api.post(
        "/webhooks/payments", dependencies=[Depends(verify_signature)], response_model=Acknowledged
    )
    async def payments(data: Payment) -> Acknowledged:
        await service.pay(db, data)
        return Acknowledged()

    @api.post(
        "/webhooks/providers/{name}",
        dependencies=[Depends(verify_signature)],
        response_model=Acknowledged,
    )
    async def webhook(name: str, data: ProviderEvent) -> Acknowledged:
        if name not in ("alpha", "beta"):
            raise HTTPException(404, "Провайдер не найден")
        await service.finish(
            db,
            data.attempt_id,
            config,
            provider=name,
            result=str(data.result) if data.status == "completed" else None,
            error_type=data.error_type if data.status == "failed" else None,
            error_detail="webhook" if data.status == "failed" else None,
            external_id=data.external_id,
        )
        return Acknowledged()

    @api.get("/tasks/{task_id}", response_model=TaskView)
    async def task(task_id: UUID) -> dict:
        async with db.connect() as raw:
            conn = await raw.execution_options(isolation_level="REPEATABLE READ")
            async with conn.begin():
                row = (
                    (await conn.execute(select(tasks).where(tasks.c.id == task_id)))
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise HTTPException(404, "Задача не найдена")
                history = [
                    dict(a)
                    for a in (
                        await conn.execute(
                            select(attempts)
                            .where(attempts.c.task_id == task_id)
                            .order_by(attempts.c.priority)
                        )
                    ).mappings()
                ]
                return dict(row) | {
                    "task_id": task_id,
                    "attempts": history,
                    "current_leg": next(a for a in history if a["id"] == row["current_attempt_id"]),
                }

    @api.get("/users/{user_id}/balance", response_model=BalanceView)
    async def balance(user_id: int = Path(gt=0), limit: int = Query(20, ge=1, le=100)) -> dict:
        async with db.connect() as raw:
            conn = await raw.execution_options(isolation_level="REPEATABLE READ")
            async with conn.begin():
                row = (
                    (await conn.execute(select(users).where(users.c.id == user_id)))
                    .mappings()
                    .one_or_none()
                )
                if row is None:
                    raise HTTPException(404, "Пользователь не найден")
                history = (
                    (
                        await conn.execute(
                            select(ledger)
                            .where(ledger.c.user_id == user_id)
                            .order_by(ledger.c.id.desc())
                            .limit(limit)
                        )
                    )
                    .mappings()
                    .all()
                )
                return {"user_id": user_id, "balance": row["balance"], "ledger": history}

    return api


app = create_app()
