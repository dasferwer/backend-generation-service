import os
from dataclasses import dataclass


@dataclass(frozen=True)
class Settings:
    database_url: str = "postgresql+asyncpg://generation:demo-password@db:5432/generation"
    webhook_secret: str = "demo-secret-change-me"
    provider_timeout: float = 60
    lease_seconds: float = 10
    poll_interval: float = 0.2
    send_concurrency: int = 2
    max_send_count: int = 3
    post_timeout: float = 3
    alpha_url: str = "http://alpha:8000"
    beta_url: str = "http://beta:8000"
    callback_base: str = "http://api:8000"

    def __post_init__(self):
        if (
            min(
                self.provider_timeout,
                self.lease_seconds,
                self.poll_interval,
                self.post_timeout,
                self.send_concurrency,
                self.max_send_count,
            )
            <= 0
        ):
            raise ValueError("Таймауты и ограничения должны быть положительными")
        if self.lease_seconds <= self.post_timeout:
            raise ValueError("Аренда должна превышать полный таймаут HTTP-запроса")
        if not self.webhook_secret:
            raise ValueError("WEBHOOK_SECRET не должен быть пустым")

    @classmethod
    def from_env(cls) -> "Settings":
        return cls(
            database_url=os.getenv("DATABASE_URL", cls.database_url),
            webhook_secret=os.getenv("WEBHOOK_SECRET", cls.webhook_secret),
            provider_timeout=float(os.getenv("PROVIDER_TIMEOUT", "60")),
            lease_seconds=float(os.getenv("LEASE_SECONDS", "10")),
            poll_interval=float(os.getenv("POLL_INTERVAL", "0.2")),
            send_concurrency=int(os.getenv("SEND_CONCURRENCY", "2")),
            max_send_count=int(os.getenv("MAX_SEND_COUNT", "3")),
            post_timeout=float(os.getenv("POST_TIMEOUT", "3")),
            alpha_url=os.getenv("ALPHA_URL", cls.alpha_url),
            beta_url=os.getenv("BETA_URL", cls.beta_url),
            callback_base=os.getenv("API_CALLBACK_BASE", cls.callback_base),
        )
