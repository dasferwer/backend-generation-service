"""Каталог, очередь попыток и ограничения журнала токенов."""

import sqlalchemy as sa
from alembic import op
from sqlalchemy.dialects.postgresql import JSONB, UUID

revision = "001"
down_revision = None
branch_labels = None
depends_on = None


def ts(name, nullable=True):
    return sa.Column(name, sa.DateTime(timezone=True), nullable=nullable)


def upgrade():
    op.create_table(
        "users",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("balance", sa.BigInteger, nullable=False, server_default="0"),
        sa.CheckConstraint("balance >= 0", name="nonnegative_balance"),
    )
    op.create_table(
        "models",
        sa.Column("slug", sa.String(64), primary_key=True),
        sa.Column("price", sa.Integer, nullable=False),
        sa.CheckConstraint("price > 0"),
    )
    op.create_table(
        "legs",
        sa.Column("id", sa.Integer, primary_key=True),
        sa.Column("model_slug", sa.String(64), sa.ForeignKey("models.slug"), nullable=False),
        sa.Column("provider", sa.String(16), nullable=False),
        sa.Column("priority", sa.Integer, nullable=False),
        sa.Column("max_ref_images", sa.Integer, nullable=False),
        sa.Column("skip_on_moderation", sa.Boolean, nullable=False),
        sa.UniqueConstraint("model_slug", "priority"),
        sa.CheckConstraint(
            "provider IN ('alpha', 'beta') AND priority > 0 AND max_ref_images >= 0"
        ),
    )
    op.create_table(
        "tasks",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("user_id", sa.BigInteger, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("model_slug", sa.String(64), sa.ForeignKey("models.slug"), nullable=False),
        sa.Column("price", sa.Integer, nullable=False),
        sa.Column("slot", sa.Integer, nullable=False),
        sa.Column("prompt", sa.Text, nullable=False),
        sa.Column("ref_images", JSONB, nullable=False),
        sa.Column("plan", JSONB, nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("result", sa.Text),
        sa.Column("current_attempt_id", UUID),
        ts("created_at", False),
        ts("finished_at"),
        sa.CheckConstraint("price > 0 AND slot BETWEEN 1 AND 3"),
        sa.CheckConstraint("status IN ('active', 'completed', 'failed')"),
        sa.CheckConstraint("(status = 'completed') = (result IS NOT NULL)"),
        sa.CheckConstraint("(status = 'active') = (finished_at IS NULL)"),
    )
    op.create_index(
        "active_user_slots",
        "tasks",
        ["user_id", "slot"],
        unique=True,
        postgresql_where=sa.text("status = 'active'"),
    )
    op.create_table(
        "attempts",
        sa.Column("id", UUID, primary_key=True),
        sa.Column("task_id", UUID, sa.ForeignKey("tasks.id"), nullable=False),
        sa.Column("provider", sa.String(16), nullable=False),
        sa.Column("priority", sa.Integer, nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        sa.Column("error_type", sa.String(16)),
        sa.Column("error_detail", sa.String(64)),
        sa.Column("external_id", sa.String(128)),
        sa.Column("lease_token", UUID),
        sa.Column("send_count", sa.Integer, nullable=False, server_default="0"),
        ts("lease_until"),
        ts("deadline_at", False),
        ts("created_at", False),
        ts("started_at"),
        ts("finished_at"),
        sa.UniqueConstraint("task_id", "priority"),
        sa.UniqueConstraint("task_id", "id", name="attempt_task_id"),
        sa.CheckConstraint("provider IN ('alpha', 'beta')"),
        sa.CheckConstraint("status IN ('queued', 'sending', 'waiting', 'completed', 'failed')"),
        sa.CheckConstraint(
            "error_type IS NULL OR error_type IN ('technical','moderation','payload')"
        ),
        sa.CheckConstraint("(status = 'failed') = (error_type IS NOT NULL)"),
        sa.CheckConstraint("(status IN ('completed','failed')) = (finished_at IS NOT NULL)"),
    )
    op.create_foreign_key(
        "current_attempt_owner",
        "tasks",
        "attempts",
        ["id", "current_attempt_id"],
        ["task_id", "id"],
        deferrable=True,
        initially="DEFERRED",
    )
    op.create_index("attempt_dispatch", "attempts", ["status", "lease_until", "created_at"])
    op.create_index(
        "attempt_deadline",
        "attempts",
        ["deadline_at"],
        postgresql_where=sa.text("status IN ('queued','sending','waiting')"),
    )
    op.create_table(
        "payments",
        sa.Column("payment_id", sa.String(128), primary_key=True),
        sa.Column("user_id", sa.BigInteger, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("tokens", sa.BigInteger, nullable=False),
        sa.Column("status", sa.String(16), nullable=False),
        ts("created_at", False),
        sa.CheckConstraint("tokens > 0"),
        sa.CheckConstraint("status IN ('pending','failed','succeeded')"),
    )
    op.create_table(
        "ledger",
        sa.Column("id", sa.BigInteger, primary_key=True),
        sa.Column("user_id", sa.BigInteger, sa.ForeignKey("users.id"), nullable=False),
        sa.Column("task_id", UUID, sa.ForeignKey("tasks.id")),
        sa.Column("payment_id", sa.String(128), sa.ForeignKey("payments.payment_id")),
        sa.Column("kind", sa.String(16), nullable=False),
        sa.Column("amount", sa.BigInteger, nullable=False),
        sa.Column(
            "created_at", sa.DateTime(timezone=True), nullable=False, server_default=sa.func.now()
        ),
        sa.UniqueConstraint("task_id", "kind", name="one_task_movement"),
        sa.UniqueConstraint("payment_id", name="one_payment_credit"),
        sa.CheckConstraint(
            "(kind = 'debit' AND amount < 0 AND task_id IS NOT NULL AND payment_id IS NULL) OR "
            "(kind = 'refund' AND amount > 0 AND task_id IS NOT NULL AND payment_id IS NULL) OR "
            "(kind = 'payment' AND amount > 0 AND payment_id IS NOT NULL AND task_id IS NULL) OR "
            "(kind = 'initial' AND amount > 0 AND payment_id IS NULL AND task_id IS NULL)"
        ),
    )
    op.create_index(
        "one_initial_credit",
        "ledger",
        ["user_id"],
        unique=True,
        postgresql_where=sa.text("kind = 'initial'"),
    )
    op.create_index("user_ledger", "ledger", ["user_id", "id"])
    op.execute("""CREATE FUNCTION apply_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            UPDATE users SET balance = balance + NEW.amount WHERE id = NEW.user_id;
            RETURN NEW;
        END $$""")
    # AFTER INSERT не срабатывает для проигравшего ON CONFLICT DO NOTHING.
    op.execute(
        "CREATE TRIGGER ledger_balance AFTER INSERT ON ledger "
        "FOR EACH ROW EXECUTE FUNCTION apply_ledger()"
    )
    op.execute("""CREATE FUNCTION immutable_ledger() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN RAISE EXCEPTION 'Журнал операций допускает только добавление'; END $$""")
    op.execute(
        "CREATE TRIGGER ledger_immutable BEFORE UPDATE OR DELETE ON ledger "
        "FOR EACH ROW EXECUTE FUNCTION immutable_ledger()"
    )
    op.execute("""CREATE FUNCTION verify_balance() RETURNS trigger LANGUAGE plpgsql AS $$
        BEGIN
            IF (SELECT balance FROM users WHERE id = NEW.id) <>
               (SELECT COALESCE(SUM(amount), 0) FROM ledger WHERE user_id = NEW.id) THEN
                RAISE EXCEPTION 'Баланс не равен сумме журнала';
            END IF;
            RETURN NULL;
        END $$""")
    op.execute("""CREATE CONSTRAINT TRIGGER balance_matches_ledger AFTER INSERT OR UPDATE ON users
        DEFERRABLE INITIALLY DEFERRED FOR EACH ROW EXECUTE FUNCTION verify_balance()""")
    op.create_table(
        "fake_jobs",
        sa.Column("provider", sa.String(16), primary_key=True),
        sa.Column("attempt_id", UUID, primary_key=True),
        sa.Column("external_id", sa.String(128), nullable=False),
        sa.Column("request", JSONB, nullable=False),
        sa.Column("behavior", JSONB, nullable=False),
        ts("next_delivery", False),
        ts("delivered_at"),
        ts("lease_until"),
        sa.Column("lease_token", UUID),
        sa.Column("delivery_count", sa.Integer, nullable=False, server_default="0"),
    )
    op.execute("INSERT INTO models VALUES ('image-fast', 2), ('video-long', 25)")
    op.execute("""INSERT INTO legs
        (model_slug, provider, priority, max_ref_images, skip_on_moderation)
        VALUES ('image-fast','alpha',1,4,false), ('image-fast','beta',2,10,false),
               ('video-long','beta',1,2,false), ('video-long','alpha',2,1,true)""")
    op.execute("INSERT INTO users (id) VALUES (1)")
    op.execute("INSERT INTO ledger (user_id, kind, amount) VALUES (1,'initial',100)")


def downgrade():
    op.drop_table("fake_jobs")
    op.execute("DROP TRIGGER balance_matches_ledger ON users")
    op.execute("DROP FUNCTION verify_balance()")
    op.drop_table("ledger")
    op.execute("DROP FUNCTION apply_ledger()")
    op.execute("DROP FUNCTION immutable_ledger()")
    op.drop_table("payments")
    op.drop_constraint("current_attempt_owner", "tasks", type_="foreignkey")
    op.drop_table("attempts")
    op.drop_table("tasks")
    op.drop_table("legs")
    op.drop_table("models")
    op.drop_table("users")
