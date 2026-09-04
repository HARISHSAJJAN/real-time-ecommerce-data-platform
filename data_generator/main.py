"""Entry point for the e-commerce event generator service."""
from __future__ import annotations

import logging
import sys

from data_generator.config import GeneratorConfig
from data_generator.producer import EventProducer


def configure_logging() -> None:
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s [%(levelname)s] %(name)s: %(message)s",
        stream=sys.stdout,
    )


def main() -> None:
    configure_logging()
    config = GeneratorConfig()
    producer = EventProducer(config)
    producer.run()


if __name__ == "__main__":
    main()
