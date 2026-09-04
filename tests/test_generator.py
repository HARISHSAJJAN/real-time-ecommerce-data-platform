"""Tests for the e-commerce event generator."""
import uuid

import pytest
from pydantic import ValidationError

from data_generator.config import GeneratorConfig
from data_generator.generator import EcommerceDataGenerator
from data_generator.models import EcommerceEvent


@pytest.fixture
def generator() -> EcommerceDataGenerator:
    config = GeneratorConfig(random_seed=42, num_users=50, num_products=20)
    return EcommerceDataGenerator(config)


def test_generates_requested_number_of_users_and_products(generator: EcommerceDataGenerator) -> None:
    assert len(generator.users) == 50
    assert len(generator.products) == 20


def test_users_have_unique_ids(generator: EcommerceDataGenerator) -> None:
    ids = [u.user_id for u in generator.users]
    assert len(ids) == len(set(ids))


def test_valid_event_conforms_to_schema(generator: EcommerceDataGenerator) -> None:
    payload = generator.generate_valid_event()
    # Should not raise - the payload must satisfy every field constraint.
    event = EcommerceEvent(**payload)
    assert uuid.UUID(event.event_id)
    assert event.price >= 0
    assert event.quantity >= 0


def test_valid_event_uses_known_user_and_product(generator: EcommerceDataGenerator) -> None:
    payload = generator.generate_valid_event()
    user_ids = {u.user_id for u in generator.users}
    assert payload["user_id"] in user_ids


def test_malformed_event_fails_schema_validation(generator: EcommerceDataGenerator) -> None:
    failures = 0
    for _ in range(50):
        payload = generator.generate_malformed_event()
        reason = payload.pop("_malformed_reason")
        try:
            EcommerceEvent(**payload)
        except (ValidationError, TypeError, ValueError):
            failures += 1
        else:
            pytest.fail(f"Malformed event with reason '{reason}' unexpectedly passed validation")
    assert failures == 50


def test_duplicate_event_matches_a_previously_emitted_event(generator: EcommerceDataGenerator) -> None:
    original = generator.generate_valid_event()
    duplicate = generator.generate_duplicate_event()
    assert duplicate is not None
    assert duplicate["event_id"] in {original["event_id"]}.union(
        {e["event_id"] for e in generator._recent_events}
    )


def test_deterministic_with_fixed_seed() -> None:
    config = GeneratorConfig(random_seed=123, num_users=10, num_products=5)
    gen_a = EcommerceDataGenerator(config)
    gen_b = EcommerceDataGenerator(config)
    assert [u.user_id for u in gen_a.users] == [u.user_id for u in gen_b.users]
    assert [p.product_id for p in gen_a.products] == [p.product_id for p in gen_b.products]


def test_event_type_distribution_favors_views_over_purchases() -> None:
    config = GeneratorConfig(random_seed=7, num_users=100, num_products=50, malformed_event_rate=0, duplicate_event_rate=0)
    generator = EcommerceDataGenerator(config)
    counts: dict[str, int] = {}
    for _ in range(2000):
        payload, kind = generator.next_event()
        assert kind == "valid"
        counts[payload["event_type"]] = counts.get(payload["event_type"], 0) + 1

    assert counts.get("product_view", 0) > counts.get("purchase", 0)
    assert counts.get("product_view", 0) > counts.get("refund", 0)


def test_config_rejects_invalid_rates() -> None:
    with pytest.raises(ValueError):
        GeneratorConfig(malformed_event_rate=1.5)
    with pytest.raises(ValueError):
        GeneratorConfig(events_per_second=0)
