import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI, Request

from app.api import router
from app.config import Settings, get_settings
from app.db import create_engine, init_models
from app.ingest import WebhookIngestor
from app.repository import TransactionRepository
from app.worker import TransactionHandler, TransactionProcessor


def create_app(settings: Settings | None = None, handler: TransactionHandler | None = None) -> FastAPI:
    settings = settings or get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )

    @asynccontextmanager
    async def lifespan(app: FastAPI):
        engine = create_engine(settings.database_url)
        await init_models(engine)
        repository = TransactionRepository(engine)
        processor = TransactionProcessor(repository, settings, handler)
        app.state.repository = repository
        app.state.processor = processor
        app.state.ingestor = WebhookIngestor(repository, processor, settings.ack_deadline_ms)
        await processor.start()
        try:
            yield
        finally:
            await app.state.ingestor.drain()
            await processor.stop()
            await engine.dispose()

    app = FastAPI(
        title="Transaction Webhook Service",
        description="Receives payment-processor webhooks, acknowledges them immediately "
        "and processes each transaction exactly once in the background.",
        version="1.0.0",
        lifespan=lifespan,
    )

    @app.middleware("http")
    async def response_time_header(request: Request, call_next):
        started = time.perf_counter()
        response = await call_next(request)
        response.headers["X-Response-Time-Ms"] = f"{(time.perf_counter() - started) * 1000:.1f}"
        return response

    app.include_router(router)
    return app


app = create_app()
