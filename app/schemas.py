from datetime import datetime, timezone
from decimal import Decimal

from pydantic import BaseModel, ConfigDict, Field, field_serializer, field_validator


def to_iso_z(value: datetime) -> str:
    """Format a datetime as ISO-8601 UTC with a trailing `Z`, e.g. 2024-01-15T10:30:00.123Z."""
    if value.tzinfo is None:
        value = value.replace(tzinfo=timezone.utc)
    return value.astimezone(timezone.utc).isoformat(timespec="milliseconds").replace("+00:00", "Z")


class TransactionWebhook(BaseModel):
    # Payment processors routinely add fields to their payloads; ignore what we don't use.
    model_config = ConfigDict(extra="ignore", str_strip_whitespace=True)

    transaction_id: str = Field(min_length=1, max_length=128, examples=["txn_abc123def456"])
    source_account: str = Field(min_length=1, max_length=128, examples=["acc_user_789"])
    destination_account: str = Field(min_length=1, max_length=128, examples=["acc_merchant_456"])
    amount: Decimal = Field(gt=0, max_digits=18, decimal_places=2, examples=[1500])
    currency: str = Field(pattern=r"^[A-Za-z]{3}$", examples=["INR"])

    @field_validator("currency")
    @classmethod
    def uppercase_currency(cls, value: str) -> str:
        return value.upper()


class WebhookAck(BaseModel):
    status: str = "ACCEPTED"
    transaction_id: str
    # True if this transaction_id had already been received; null if the write was still
    # in flight when the acknowledgement deadline passed.
    duplicate: bool | None = None


class TransactionOut(BaseModel):
    model_config = ConfigDict(from_attributes=True)

    transaction_id: str
    source_account: str
    destination_account: str
    amount: float
    currency: str
    status: str
    created_at: datetime
    processed_at: datetime | None

    @field_serializer("created_at", "processed_at")
    def serialize_timestamp(self, value: datetime | None) -> str | None:
        return to_iso_z(value) if value is not None else None


class HealthOut(BaseModel):
    status: str
    current_time: str
