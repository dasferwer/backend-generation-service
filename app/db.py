from sqlalchemy import (
    JSON,
    BigInteger,
    Boolean,
    Column,
    DateTime,
    ForeignKey,
    Integer,
    MetaData,
    String,
    Table,
    Text,
    Uuid,
    func,
)
from sqlalchemy.dialects.postgresql import JSONB
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

metadata = MetaData()
json_type = JSON().with_variant(JSONB, "postgresql")


def timestamp(name: str, *, nullable: bool = True) -> Column:
    return Column(name, DateTime(timezone=True), nullable=nullable)


users = Table(
    "users",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("balance", BigInteger, nullable=False, server_default="0"),
)
models = Table(
    "models",
    metadata,
    Column("slug", String(64), primary_key=True),
    Column("price", Integer, nullable=False),
)
legs = Table(
    "legs",
    metadata,
    Column("id", Integer, primary_key=True),
    Column("model_slug", ForeignKey("models.slug"), nullable=False),
    Column("provider", String(16), nullable=False),
    Column("priority", Integer, nullable=False),
    Column("max_ref_images", Integer, nullable=False),
    Column("skip_on_moderation", Boolean, nullable=False),
)
tasks = Table(
    "tasks",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("user_id", ForeignKey("users.id"), nullable=False),
    Column("model_slug", ForeignKey("models.slug"), nullable=False),
    Column("price", Integer, nullable=False),
    Column("slot", Integer, nullable=False),
    Column("prompt", Text, nullable=False),
    Column("ref_images", json_type, nullable=False),
    Column("plan", json_type, nullable=False),
    Column("status", String(16), nullable=False),
    Column("result", Text),
    Column("current_attempt_id", Uuid),
    timestamp("created_at", nullable=False),
    timestamp("finished_at"),
)
attempts = Table(
    "attempts",
    metadata,
    Column("id", Uuid, primary_key=True),
    Column("task_id", ForeignKey("tasks.id"), nullable=False),
    Column("provider", String(16), nullable=False),
    Column("priority", Integer, nullable=False),
    Column("status", String(16), nullable=False),
    Column("error_type", String(16)),
    Column("error_detail", String(64)),
    Column("external_id", String(128)),
    Column("lease_token", Uuid),
    Column("send_count", Integer, nullable=False, server_default="0"),
    timestamp("lease_until"),
    timestamp("deadline_at", nullable=False),
    timestamp("created_at", nullable=False),
    timestamp("started_at"),
    timestamp("finished_at"),
)
payments = Table(
    "payments",
    metadata,
    Column("payment_id", String(128), primary_key=True),
    Column("user_id", ForeignKey("users.id"), nullable=False),
    Column("tokens", BigInteger, nullable=False),
    Column("status", String(16), nullable=False),
    timestamp("created_at", nullable=False),
)
ledger = Table(
    "ledger",
    metadata,
    Column("id", BigInteger, primary_key=True),
    Column("user_id", ForeignKey("users.id"), nullable=False),
    Column("task_id", ForeignKey("tasks.id")),
    Column("payment_id", ForeignKey("payments.payment_id")),
    Column("kind", String(16), nullable=False),
    Column("amount", BigInteger, nullable=False),
    Column("created_at", DateTime(timezone=True), nullable=False, server_default=func.now()),
)
fake_jobs = Table(
    "fake_jobs",
    metadata,
    Column("provider", String(16), primary_key=True),
    Column("attempt_id", Uuid, primary_key=True),
    Column("external_id", String(128), nullable=False),
    Column("request", json_type, nullable=False),
    Column("behavior", json_type, nullable=False),
    timestamp("next_delivery", nullable=False),
    timestamp("delivered_at"),
    timestamp("lease_until"),
    Column("lease_token", Uuid),
    Column("delivery_count", Integer, nullable=False, server_default="0"),
)


def make_engine(url: str) -> AsyncEngine:
    return create_async_engine(
        url, pool_size=5, max_overflow=5, pool_pre_ping=True, hide_parameters=True
    )
