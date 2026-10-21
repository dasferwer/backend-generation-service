from alembic import command
from alembic.config import Config
from sqlalchemy import text


async def test_migration_repeat_and_roundtrip(engine):
    async with engine.begin() as conn:

        def upgrade(sync_conn):
            cfg = Config("alembic.ini")
            cfg.attributes["connection"] = sync_conn
            command.upgrade(cfg, "head")

        await conn.run_sync(upgrade)
        assert await conn.scalar(text("SELECT balance FROM users WHERE id=1")) == 100
        assert await conn.scalar(text("SELECT count(*) FROM ledger WHERE kind='initial'")) == 1
    async with engine.begin() as conn:

        def roundtrip(sync_conn):
            cfg = Config("alembic.ini")
            cfg.attributes["connection"] = sync_conn
            command.downgrade(cfg, "base")
            command.upgrade(cfg, "head")

        await conn.run_sync(roundtrip)
        assert await conn.scalar(text("SELECT balance FROM users WHERE id=1")) == 100
        assert await conn.scalar(text("SELECT count(*) FROM legs")) == 4
