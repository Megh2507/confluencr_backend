from typing import Any

from sqlalchemy.engine import make_url
from sqlalchemy.ext.asyncio import AsyncEngine, create_async_engine

from app.models import Base

_POSTGRES_DRIVERS = {"postgres", "postgresql", "postgresql+psycopg2", "postgresql+psycopg", "postgresql+asyncpg"}


def normalize_database_url(raw_url: str) -> tuple[str, dict[str, Any]]:
    """Turn a provider-style database URL into an async SQLAlchemy URL plus connect args.

    Hosted Postgres providers hand out URLs like
    `postgresql://user:pw@host/db?sslmode=require&channel_binding=require`. asyncpg does not
    understand libpq query parameters, so they are translated into asyncpg's `ssl` argument.
    """
    url = make_url(raw_url)
    connect_args: dict[str, Any] = {}

    if url.drivername in _POSTGRES_DRIVERS:
        query = dict(url.query)
        sslmode = query.pop("sslmode", None)
        query.pop("channel_binding", None)
        url = url.set(drivername="postgresql+asyncpg", query=query)
        if sslmode and sslmode != "disable":
            connect_args["ssl"] = sslmode
    elif url.drivername in {"sqlite", "sqlite+aiosqlite"}:
        url = url.set(drivername="sqlite+aiosqlite")
        connect_args["timeout"] = 30

    return url.render_as_string(hide_password=False), connect_args


def create_engine(raw_url: str) -> AsyncEngine:
    url, connect_args = normalize_database_url(raw_url)
    options: dict[str, Any] = {"connect_args": connect_args}
    if url.startswith("postgresql"):
        # No pool_pre_ping: it doubles the queries per operation. Connections are recycled
        # instead, and a dropped connection invalidates the pool and surfaces as an error
        # that ingest and the processor already retry.
        options.update(pool_size=10, max_overflow=20, pool_recycle=300)
    return create_async_engine(url, **options)


async def init_models(engine: AsyncEngine) -> None:
    async with engine.begin() as conn:
        await conn.run_sync(Base.metadata.create_all)
