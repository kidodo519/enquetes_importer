"""Command line entry point for exporting Salesforce data to S3."""

from __future__ import annotations

import argparse
import logging
from pathlib import Path

from salesforce_exporter.config import AppConfig
from salesforce_exporter.exporter import SalesforceExporter


def setup_logging(verbose: bool = False) -> None:
    level = logging.DEBUG if verbose else logging.INFO
    logging.basicConfig(level=level, format="%(asctime)s %(levelname)s %(name)s %(message)s")


def parse_args() -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Export Salesforce data to S3")
    parser.add_argument(
        "--config",
        type=Path,
        default=Path(__file__).with_name("config.yaml"),
        help="Path to the configuration file (default: config.yaml)",
    )
    parser.add_argument("--verbose", action="store_true", help="Enable debug logging")
    return parser.parse_args()


def main() -> None:
    args = parse_args()
    setup_logging(args.verbose)

    if not args.config.exists():
        raise FileNotFoundError(f"Configuration file '{args.config}' was not found")

    config = AppConfig.load(args.config)
    exporter = SalesforceExporter(config)
    exporter.run()


if __name__ == "__main__":
    main()
