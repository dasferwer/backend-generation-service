import os
from uuid import uuid4

import httpx
import pytest
from alembic import command
from alembic.config import Config
from sqlalchemy import text
from sqlalchemy.ext.asyncio import create_async_engine

from app.api import create_app


@pytest.fixture
async def engine():
    url = os.environ.get("TEST_DATABASE_URL")
    if not url:
        pytest.fail("Задайте TEST_DATABASE_URL с адресом настоящего PostgreSQL")
    schema = "test_" + uuid4().hex
    admin = create_async_engine(url)
    async with admin.begin() as conn:
        await conn.execute(text(f'CREATE SCHEMA "{schema}"'))
    db = create_async_engine(
        url, connect_args={"server_settings": {"search_path": schema}}, pool_size=25, max_overflow=5
    )
    try:
        async with db.begin() as conn:

            def migrate(sync_conn):
                cfg = Config("alembic.ini")
                cfg.attributes["connection"] = sync_conn
                command.upgrade(cfg, "head")

            await conn.run_sync(migrate)
        yield db
    finally:
        await db.dispose()
        async with admin.begin() as conn:
            await conn.execute(text(f'DROP SCHEMA "{schema}" CASCADE'))
        await admin.dispose()


@pytest.fixture
async def client(engine):
    app = create_app(engine=engine, start_workers=False)
    async with httpx.AsyncClient(
        transport=httpx.ASGITransport(app=app), base_url="http://test"
    ) as client:
        yield client
