import logging
import time
from contextlib import asynccontextmanager

from fastapi import FastAPI
from starlette.types import ASGIApp, Message, Receive, Scope, Send

from app.api import router
from app.config import Settings, get_settings
from app.db import create_engine, init_models
from app.ingest import WebhookIngestor
from app.repository import TransactionRepository
from app.worker import TransactionHandler, TransactionProcessor


class ResponseTimeMiddleware:
    """Adds an X-Response-Time-Ms header. Plain ASGI rather than @app.middleware("http"),
    whose BaseHTTPMiddleware machinery costs noticeable CPU per request on small instances."""

    def __init__(self, app: ASGIApp):
        self.app = app

    async def __call__(self, scope: Scope, receive: Receive, send: Send) -> None:
        if scope["type"] != "http":
            await self.app(scope, receive, send)
            return
        started = time.perf_counter()

        async def send_with_timing(message: Message) -> None:
            if message["type"] == "http.response.start":
                elapsed = f"{(time.perf_counter() - started) * 1000:.1f}".encode()
                message = {**message, "headers": [*message.get("headers", []), (b"x-response-time-ms", elapsed)]}
            await send(message)

        await self.app(scope, receive, send_with_timing)


def create_app(settings: Settings | None = None, handler: TransactionHandler | None = None) -> FastAPI:
    settings = settings or get_settings()
    logging.basicConfig(
        level=settings.log_level.upper(),
        format="%(asctime)s %(levelname)s [%(name)s] %(message)s",
    )
    # One access-log line per request is pure overhead here: the app already logs every
    # transaction it receives and processes.
    logging.getLogger("uvicorn.access").setLevel(logging.WARNING)

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

    app.add_middleware(ResponseTimeMiddleware)
    app.include_router(router)
    return app


app = create_app()
