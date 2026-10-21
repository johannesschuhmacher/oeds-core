import sys
import types
import unittest
from importlib.metadata import EntryPoint
from unittest import mock

from oeds import main

# The built-in registry imports every crawler module and their dependencies;
# these tests only exercise the entry-point logic, so stand in a small one.
BUILT_IN = types.SimpleNamespace(crawlers={"smard": object})


class FakeCrawler:
    instances = []

    def __init__(self, schema_name, config):
        FakeCrawler.instances.append((schema_name, config))


def fake_entry_point() -> EntryPoint:
    return EntryPoint(
        name="fake_source", value=f"{__name__}:FakeCrawler", group="oeds.crawlers"
    )


class EntryPointCrawlerTests(unittest.TestCase):
    def test_registered_crawlers_extend_the_built_in_registry(self):
        with (
            mock.patch.dict(sys.modules, {"oeds.crawler": BUILT_IN}),
            mock.patch.object(main, "entry_points", return_value=[fake_entry_point()]),
        ):
            crawlers = main.available_crawlers()

        self.assertIn("smard", crawlers)
        self.assertIsInstance(crawlers["fake_source"], EntryPoint)
        self.assertIs(main.resolve_crawler_class(crawlers["fake_source"]), FakeCrawler)

    def test_cli_runs_a_registered_crawler(self):
        FakeCrawler.instances.clear()
        with (
            mock.patch.dict(sys.modules, {"oeds.crawler": BUILT_IN}),
            mock.patch.object(main, "entry_points", return_value=[fake_entry_point()]),
            mock.patch.object(main, "start_crawler") as start_crawler,
        ):
            main.cli(["--crawler-list", "fake_source", "--db", "sqlite://"])

        self.assertEqual(FakeCrawler.instances[0][0], "fake_source")
        self.assertEqual(FakeCrawler.instances[0][1]["db_uri"], "sqlite://")
        start_crawler.assert_called_once()


if __name__ == "__main__":
    unittest.main()
