from datetime import datetime
from typing import Annotated, Literal
from uuid import UUID

from pydantic import BaseModel, ConfigDict, Field, HttpUrl, field_validator

PositiveInt = Annotated[int, Field(strict=True, gt=0, le=1_000_000_000)]
RefImage = Annotated[str, Field(min_length=1, max_length=2048)]
ErrorType = Literal["technical", "moderation", "payload"]
AttemptStatus = Literal["queued", "sending", "waiting", "completed", "failed"]


class Input(BaseModel):
    model_config = ConfigDict(extra="forbid")


class Generate(Input):
    user_id: PositiveInt
    model_slug: Annotated[str, Field(min_length=1, max_length=64)]
    prompt: Annotated[str, Field(min_length=1, max_length=10000)]
    ref_images: Annotated[list[RefImage], Field(max_length=100)] = Field(default_factory=list)

    @field_validator("prompt")
    @classmethod
    def nonempty_prompt(cls, value: str) -> str:
        if not value.strip():
            raise ValueError("Промт не должен быть пустым")
        return value


class Payment(Input):
    payment_id: Annotated[str, Field(min_length=1, max_length=128)]
    user_id: PositiveInt
    tokens: PositiveInt
    status: Literal["pending", "failed", "succeeded"]


class Completed(Input):
    attempt_id: UUID
    status: Literal["completed"]
    result: HttpUrl
    external_id: Annotated[str | None, Field(max_length=128)] = None


class Failed(Input):
    attempt_id: UUID
    status: Literal["failed"]
    error_type: ErrorType
    external_id: Annotated[str | None, Field(max_length=128)] = None


ProviderEvent = Annotated[Completed | Failed, Field(discriminator="status")]


class Created(BaseModel):
    task_id: UUID


class Acknowledged(BaseModel):
    accepted: bool = True


class AttemptView(BaseModel):
    id: UUID
    provider: Literal["alpha", "beta"]
    priority: int
    status: AttemptStatus
    error_type: ErrorType | None
    error_detail: str | None
    external_id: str | None
    created_at: datetime
    started_at: datetime | None
    finished_at: datetime | None
    deadline_at: datetime


class TaskView(BaseModel):
    task_id: UUID
    user_id: int
    model_slug: str
    price: int
    status: Literal["active", "completed", "failed"]
    current_leg: AttemptView
    result: str | None
    created_at: datetime
    finished_at: datetime | None
    attempts: list[AttemptView]


class LedgerView(BaseModel):
    id: int
    kind: Literal["initial", "payment", "debit", "refund"]
    amount: int
    task_id: UUID | None
    payment_id: str | None
    created_at: datetime


class BalanceView(BaseModel):
    user_id: int
    balance: int
    ledger: list[LedgerView]
