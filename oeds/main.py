#!/usr/bin/env python3
# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later
import argparse
import logging
from importlib.metadata import EntryPoint, entry_points
from pathlib import Path
from typing import Any

from oeds.base_crawler import (
    BaseCrawler,
    ContinuousCrawler,
    DownloadOnceCrawler,
    empty_config,
    load_config,
)

log = logging.getLogger("OEDS")
log.setLevel(logging.INFO)


def available_crawlers() -> dict[str, Any]:
    """Built-in crawlers plus those other packages register as ``oeds.crawlers``.

    A package declares ``[project.entry-points."oeds.crawlers"]`` with
    ``name = "module:Class"`` in its pyproject.toml. Entry points are kept
    unloaded here and imported only when selected, so listing stays cheap. A
    registered crawler replaces a built-in one of the same name.
    """
    from oeds.crawler import crawlers

    registry: dict[str, Any] = dict(crawlers)
    for entry in entry_points(group="oeds.crawlers"):
        registry[entry.name] = entry
    return registry


def resolve_crawler_class(target: Any) -> type[BaseCrawler]:
    return target.load() if isinstance(target, EntryPoint) else target


def start_crawler(crawler: BaseCrawler):
    if isinstance(crawler, DownloadOnceCrawler):
        crawler.crawl_structural()
    if isinstance(crawler, ContinuousCrawler):
        crawler.crawl_temporal()


def cli(args=None):
    parser = argparse.ArgumentParser(description="Open-Energy-Data-Server CLI")
    parser.add_argument(
        "--db",
        type=str,
        help="set the DB URI",
    )
    parser.add_argument(
        "--crawler-list", nargs="*", help="List of crawlers to run (default: all)"
    )
    parser.add_argument(
        "-l",
        "--loglevel",
        help="logging level used for file log",
        default="INFO",
        type=str,
        metavar="LOGLEVEL",
        choices=set(logging._nameToLevel.keys()),
    )
    parser.add_argument("--config", type=str, help="Path to config.yaml")
    parsed_args = parser.parse_args(args)

    logging.basicConfig(level=parsed_args.loglevel)
    crawlers = available_crawlers()

    selected_crawlers = (
        parsed_args.crawler_list if parsed_args.crawler_list else list(crawlers.keys())
    )
    if parsed_args.config:
        config = load_config(parsed_args.config)
    else:
        config = empty_config()

    if parsed_args.db:
        config["db_uri"] = parsed_args.db

    for crawler_name in selected_crawlers:
        log.info("Starting crawler: %s", crawler_name)
        try:
            crawler_class = resolve_crawler_class(crawlers[crawler_name])
        except KeyError:
            raise ValueError(f"crawler {crawler_name} does not exist")

        crawler = crawler_class(crawler_name, config)
        start_crawler(crawler)


if __name__ == "__main__":
    logging.basicConfig()
    from oeds.crawler import crawlers

    config = load_config(Path(__file__).parent.parent / "config.yml")
    for schema_name, crawler_class in crawlers.items():
        crawler = crawler_class(schema_name, config)
        start_crawler(crawler)
