"""Domain models for the e-commerce event platform.

These Pydantic models are the single source of truth for the event schema.
They are used by the data generator to build valid events and by the Spark
streaming job's documentation as the canonical field reference (Spark itself
uses an equivalent StructType defined in spark/schemas.py, since Structured
Streaming cannot import Pydantic models directly into its DataFrame schema).
"""
from __future__ import annotations

import uuid
from datetime import datetime, timezone
from enum import Enum
from typing import Optional

from pydantic import BaseModel, Field, field_validator


class EventType(str, Enum):
    PRODUCT_VIEW = "product_view"
    ADD_TO_CART = "add_to_cart"
    REMOVE_FROM_CART = "remove_from_cart"
    PURCHASE = "purchase"
    PAYMENT = "payment"
    REFUND = "refund"
    LOGIN = "login"
    LOGOUT = "logout"


class DeviceType(str, Enum):
    DESKTOP = "desktop"
    MOBILE = "mobile"
    TABLET = "tablet"


class PaymentMethod(str, Enum):
    CREDIT_CARD = "credit_card"
    DEBIT_CARD = "debit_card"
    PAYPAL = "paypal"
    APPLE_PAY = "apple_pay"
    GOOGLE_PAY = "google_pay"
    BANK_TRANSFER = "bank_transfer"


# Event types that inherently do not carry a product/price (session-level events).
NON_PRODUCT_EVENT_TYPES = {EventType.LOGIN, EventType.LOGOUT}


class User(BaseModel):
    user_id: str
    country: str
    signup_date: datetime
    preferred_device: DeviceType


class Product(BaseModel):
    product_id: str
    name: str
    category: str
    price: float = Field(gt=0)
    currency: str = "USD"


class EcommerceEvent(BaseModel):
    """A single e-commerce interaction event.

    This is the contract published to the `ecommerce-events` Kafka topic.
    """

    event_id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    event_timestamp: datetime = Field(default_factory=lambda: datetime.now(timezone.utc))
    user_id: str
    session_id: str
    product_id: Optional[str] = None
    event_type: EventType
    quantity: int = Field(default=1, ge=0)
    price: float = Field(default=0.0, ge=0)
    currency: str = "USD"
    device_type: DeviceType
    country: str
    payment_method: Optional[PaymentMethod] = None

    @field_validator("event_id")
    @classmethod
    def validate_uuid(cls, value: str) -> str:
        # Raises ValueError automatically if not a valid UUID, which Pydantic
        # surfaces as a validation error to the caller.
        uuid.UUID(value)
        return value

    @field_validator("event_timestamp")
    @classmethod
    def ensure_utc(cls, value: datetime) -> datetime:
        if value.tzinfo is None:
            return value.replace(tzinfo=timezone.utc)
        return value.astimezone(timezone.utc)

    def to_kafka_json(self) -> str:
        return self.model_dump_json()
