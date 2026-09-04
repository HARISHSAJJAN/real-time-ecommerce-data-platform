"""Synthetic user, product, and event generation for the e-commerce platform.

The generator produces a realistic funnel distribution of event types
(views far outnumber purchases), occasionally injects malformed payloads
(to exercise the data-quality / dead-letter path) and occasionally
re-emits a previously seen event (to exercise Spark's deduplication logic).
"""
from __future__ import annotations

import random
import uuid
from datetime import datetime, timedelta, timezone
from typing import Any

from faker import Faker

from data_generator.config import GeneratorConfig
from data_generator.models import (
    DeviceType,
    EcommerceEvent,
    EventType,
    PaymentMethod,
    Product,
    User,
)

# Realistic e-commerce funnel: most traffic is browsing, only a fraction converts.
EVENT_TYPE_WEIGHTS: dict[EventType, float] = {
    EventType.PRODUCT_VIEW: 0.50,
    EventType.ADD_TO_CART: 0.15,
    EventType.REMOVE_FROM_CART: 0.05,
    EventType.PURCHASE: 0.08,
    EventType.PAYMENT: 0.07,
    EventType.REFUND: 0.02,
    EventType.LOGIN: 0.08,
    EventType.LOGOUT: 0.05,
}

COUNTRIES = ["US", "GB", "DE", "FR", "IN", "BR", "CA", "AU", "JP", "MX"]

def _seeded_uuid() -> str:
    """UUID drawn from the seeded `random` module.

    `uuid.uuid4()` reads from `os.urandom` and is never affected by
    `random.seed()`, so generator determinism (required when RANDOM_SEED is
    set) depends on generating IDs this way instead.
    """
    return str(uuid.UUID(int=random.getrandbits(128)))


PRODUCT_CATEGORIES = [
    "electronics",
    "home_and_kitchen",
    "clothing",
    "books",
    "sports_outdoors",
    "beauty",
    "toys",
    "grocery",
]


class EcommerceDataGenerator:
    """Generates a fixed catalog of users/products plus a continuous event stream."""

    def __init__(self, config: GeneratorConfig):
        self.config = config
        self._faker = Faker()
        if config.random_seed is not None:
            Faker.seed(config.random_seed)
            random.seed(config.random_seed)

        self.users: list[User] = self._generate_users(config.num_users)
        self.products: list[Product] = self._generate_products(config.num_products)

        # Rolling buffer of recently emitted events, used to source realistic
        # duplicate re-sends (mirrors at-least-once producer/consumer retries).
        self._recent_events: list[dict[str, Any]] = []
        self._recent_events_max = 200

        # Active sessions: user_id -> session_id, so events from the same user
        # in a short time span share a session (as they would in production).
        self._active_sessions: dict[str, str] = {}

    # ---------------------------------------------------------------- setup
    def _generate_users(self, n: int) -> list[User]:
        users = []
        for _ in range(n):
            users.append(
                User(
                    user_id=_seeded_uuid(),
                    country=random.choice(COUNTRIES),
                    signup_date=self._faker.date_time_between(
                        start_date="-2y", end_date="-1d", tzinfo=timezone.utc
                    ),
                    preferred_device=random.choice(list(DeviceType)),
                )
            )
        return users

    def _generate_products(self, n: int) -> list[Product]:
        products = []
        for _ in range(n):
            category = random.choice(PRODUCT_CATEGORIES)
            products.append(
                Product(
                    product_id=_seeded_uuid(),
                    name=self._faker.catch_phrase(),
                    category=category,
                    price=round(random.uniform(4.99, 899.99), 2),
                    currency="USD",
                )
            )
        return products

    # ------------------------------------------------------------- helpers
    def _pick_event_type(self) -> EventType:
        types = list(EVENT_TYPE_WEIGHTS.keys())
        weights = list(EVENT_TYPE_WEIGHTS.values())
        return random.choices(types, weights=weights, k=1)[0]

    def _session_for_user(self, user_id: str) -> str:
        # 85% chance of continuing the current session, otherwise start a new one.
        if user_id in self._active_sessions and random.random() < 0.85:
            return self._active_sessions[user_id]
        session_id = _seeded_uuid()
        self._active_sessions[user_id] = session_id
        return session_id

    def _remember(self, payload: dict[str, Any]) -> None:
        self._recent_events.append(payload)
        if len(self._recent_events) > self._recent_events_max:
            self._recent_events.pop(0)

    # ------------------------------------------------------------- events
    def generate_valid_event(self) -> dict[str, Any]:
        user = random.choice(self.users)
        product = random.choice(self.products)
        event_type = self._pick_event_type()
        device = random.choice(list(DeviceType))
        is_product_event = event_type not in (EventType.LOGIN, EventType.LOGOUT)

        quantity = random.randint(1, 5) if event_type in (
            EventType.ADD_TO_CART,
            EventType.REMOVE_FROM_CART,
            EventType.PURCHASE,
        ) else (1 if is_product_event else 0)

        price = product.price if is_product_event else 0.0
        payment_method = (
            random.choice(list(PaymentMethod))
            if event_type in (EventType.PURCHASE, EventType.PAYMENT, EventType.REFUND)
            else None
        )

        event = EcommerceEvent(
            event_id=_seeded_uuid(),
            user_id=user.user_id,
            session_id=self._session_for_user(user.user_id),
            product_id=product.product_id if is_product_event else None,
            event_type=event_type,
            quantity=quantity,
            price=price,
            currency=product.currency,
            device_type=device,
            country=user.country,
            payment_method=payment_method,
        )
        payload = event.model_dump(mode="json")
        self._remember(payload)
        return payload

    def generate_malformed_event(self) -> dict[str, Any]:
        """Produce a payload that intentionally violates the event contract.

        These records are still published to the main topic (as they would
        be by a buggy upstream client in the real world) so the Spark job's
        validation logic and dead-letter routing can be exercised end to end.
        """
        user = random.choice(self.users)
        product = random.choice(self.products)
        base = {
            "event_id": _seeded_uuid(),
            "event_timestamp": datetime.now(timezone.utc).isoformat(),
            "user_id": user.user_id,
            "session_id": self._session_for_user(user.user_id),
            "product_id": product.product_id,
            "event_type": EventType.PURCHASE.value,
            "quantity": 1,
            "price": product.price,
            "currency": product.currency,
            "device_type": DeviceType.DESKTOP.value,
            "country": user.country,
            "payment_method": PaymentMethod.CREDIT_CARD.value,
        }

        mutation = random.choice(
            [
                "missing_required_field",
                "invalid_event_type",
                "negative_price",
                "negative_quantity",
                "invalid_uuid",
                "bad_timestamp",
                "null_user_id",
            ]
        )

        if mutation == "missing_required_field":
            del base["user_id"]
        elif mutation == "invalid_event_type":
            base["event_type"] = "checkout_abandoned"
        elif mutation == "negative_price":
            base["price"] = -19.99
        elif mutation == "negative_quantity":
            base["quantity"] = -3
        elif mutation == "invalid_uuid":
            base["event_id"] = "not-a-valid-uuid"
        elif mutation == "bad_timestamp":
            base["event_timestamp"] = "not-a-timestamp"
        elif mutation == "null_user_id":
            base["user_id"] = None

        base["_malformed_reason"] = mutation
        return base

    def generate_duplicate_event(self) -> dict[str, Any] | None:
        """Re-emit a previously seen valid event, simulating a producer retry."""
        if not self._recent_events:
            return None
        return dict(random.choice(self._recent_events))

    def next_event(self) -> tuple[dict[str, Any], str]:
        """Return (payload, kind) where kind is 'valid', 'malformed', or 'duplicate'."""
        roll = random.random()
        if roll < self.config.malformed_event_rate:
            return self.generate_malformed_event(), "malformed"
        if roll < self.config.malformed_event_rate + self.config.duplicate_event_rate:
            duplicate = self.generate_duplicate_event()
            if duplicate is not None:
                return duplicate, "duplicate"
        return self.generate_valid_event(), "valid"
