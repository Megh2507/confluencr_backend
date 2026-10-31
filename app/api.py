import asyncio
import logging

from fastapi import APIRouter, HTTPException, Request, status
from fastapi.responses import JSONResponse

from app.repository import utcnow
from app.schemas import HealthOut, TransactionOut, TransactionWebhook, WebhookAck, to_iso_z

logger = logging.getLogger(__name__)

router = APIRouter()


@router.get("/", response_model=HealthOut, tags=["health"])
async def health(request: Request):
    healthy = True
    try:
        await asyncio.wait_for(request.app.state.repository.ping(), timeout=5)
    except Exception:
        logger.warning("Health check: database unreachable", exc_info=True)
        healthy = False
    body = HealthOut(status="HEALTHY" if healthy else "UNHEALTHY", current_time=to_iso_z(utcnow()))
    return JSONResponse(
        body.model_dump(),
        status_code=status.HTTP_200_OK if healthy else status.HTTP_503_SERVICE_UNAVAILABLE,
    )


@router.post(
    "/v1/webhooks/transactions",
    response_model=WebhookAck,
    status_code=status.HTTP_202_ACCEPTED,
    tags=["webhooks"],
)
async def receive_transaction_webhook(payload: TransactionWebhook, request: Request):
    try:
        created = await request.app.state.ingestor.accept(payload)
    except Exception:
        # Not stored, so don't acknowledge: a non-2xx makes the payment processor retry.
        raise HTTPException(status.HTTP_503_SERVICE_UNAVAILABLE, "Temporarily unable to accept webhook")
    return WebhookAck(
        transaction_id=payload.transaction_id,
        duplicate=None if created is None else not created,
    )


@router.get(
    "/v1/transactions/{transaction_id}",
    response_model=list[TransactionOut],
    tags=["transactions"],
)
async def get_transaction(transaction_id: str, request: Request):
    transaction = await request.app.state.repository.get(transaction_id)
    if transaction is None:
        raise HTTPException(status.HTTP_404_NOT_FOUND, f"Transaction {transaction_id} not found")
    return [TransactionOut.model_validate(transaction)]
