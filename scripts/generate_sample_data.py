"""Generate a batch of sample events to a local JSONL file, without needing Kafka.

Useful for quickly inspecting what the generator produces, or as fixture
data for offline experimentation, without standing up the full Docker stack.

Usage:
    python scripts/generate_sample_data.py --count 500 --output sample_events.jsonl
"""
from __future__ import annotations

import argparse
import json
import os
import sys

sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))

from data_generator.config import GeneratorConfig  # noqa: E402
from data_generator.generator import EcommerceDataGenerator  # noqa: E402


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("--count", type=int, default=500, help="Number of events to generate")
    parser.add_argument("--output", type=str, default="sample_events.jsonl", help="Output JSONL file path")
    parser.add_argument("--seed", type=int, default=42, help="Random seed for reproducibility")
    args = parser.parse_args()

    config = GeneratorConfig(random_seed=args.seed)
    generator = EcommerceDataGenerator(config)

    counts = {"valid": 0, "malformed": 0, "duplicate": 0}
    with open(args.output, "w", encoding="utf-8") as f:
        for _ in range(args.count):
            payload, kind = generator.next_event()
            counts[kind] += 1
            f.write(json.dumps(payload) + "\n")

    print(f"Wrote {args.count} events to {args.output}")
    print(f"Breakdown: {counts}")


if __name__ == "__main__":
    main()
