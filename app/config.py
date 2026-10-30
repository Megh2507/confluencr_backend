from functools import lru_cache

from pydantic_settings import BaseSettings, SettingsConfigDict


class Settings(BaseSettings):
    """Runtime configuration, read from environment variables (or a local .env file)."""

    model_config = SettingsConfigDict(env_file=".env", extra="ignore")

    # Any SQLAlchemy URL. Plain `postgres://` / `postgresql://` URLs (as handed out by
    # Neon, Supabase, Render, ...) are converted to the asyncpg driver automatically.
    database_url: str = "sqlite+aiosqlite:///./transactions.db"

    # Simulated external API latency for each transaction.
    processing_delay_seconds: float = 30.0

    # The webhook must answer within 500ms. If persisting the transaction takes longer than
    # this, we acknowledge anyway and let the (shielded) write finish in the background.
    ack_deadline_ms: int = 300

    # How often the sweeper looks for transactions that are pending, retryable, or orphaned
    # by a crashed worker.
    sweep_interval_seconds: float = 5.0

    # A claimed transaction is leased for `processing_delay + lease_grace` seconds. If the
    # worker dies, the lease expires and another worker picks the transaction up.
    lease_grace_seconds: float = 30.0

    # Failed attempts are retried with exponential backoff up to this many attempts,
    # after which the transaction is marked FAILED.
    max_attempts: int = 5
    retry_backoff_seconds: float = 5.0

    # Upper bound on transactions processed concurrently by one instance.
    max_concurrent_jobs: int = 500

    log_level: str = "INFO"


@lru_cache
def get_settings() -> Settings:
    return Settings()
