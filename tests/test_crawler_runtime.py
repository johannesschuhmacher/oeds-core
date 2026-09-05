import io
import sqlite3
import tempfile
import unittest
import zipfile
from contextlib import closing
from datetime import date
from pathlib import Path
from unittest.mock import MagicMock, patch

import pandas as pd
from oeds.crawler.opsd import OpsdCrawler
from oeds.crawler.regelleistung import RegelleistungCrawler
from oeds.crawler.vea_industrial_load_profiles import IndustrialLoadProfileCrawler


class CrawlerRuntimeTests(unittest.TestCase):
    def test_regelleistung_respects_exclusive_end_and_replaces_requested_days(self):
        crawler = RegelleistungCrawler(
            "regelleistung", {"db_uri": "sqlite://", "tables": ["fcr_bedarfe"]}
        )
        requested = []

        def fetch(url, day, table):
            requested.append(day)
            return pd.DataFrame({"date_from": [pd.Timestamp(day)], "value": [12.5]})

        with patch("oeds.crawler.regelleistung.get_df_for_date", side_effect=fetch):
            crawler.crawl_temporal(date(2026, 9, 1), date(2026, 9, 3))
            crawler.crawl_temporal(date(2026, 9, 1), date(2026, 9, 3))
        self.assertEqual(requested, [date(2026, 9, 1), date(2026, 9, 2)] * 2)
        self.assertEqual(len(pd.read_sql_table("fcr_bedarfe", crawler.engine)), 2)

    def test_when2heat_streams_multiple_blocks_and_honors_row_limit(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "when2heat.db"
            frame = pd.DataFrame(
                {
                    "utc_timestamp": pd.date_range(
                        "2020-01-01", periods=1200, freq="h"
                    ).astype(str),
                    "cet_cest_timestamp": "unused",
                    "DE_heat": range(1200),
                }
            )
            with closing(sqlite3.connect(path)) as conn:
                frame.to_sql("when2heat", conn, index=False)
            crawler = OpsdCrawler("opsd", {"db_uri": "sqlite://", "max_rows": 1002})
            crawler.write_when2_heat(path)
            result = pd.read_sql_table("when2heat", crawler.engine)
            self.assertEqual(len(result), 1002)
            self.assertEqual(result.DE_heat.tolist(), list(range(1002)))
            self.assertNotIn("cet_cest_timestamp", result)

    def test_vea_profile_blocks_preserve_values_and_recreate_does_not_duplicate(self):
        payload = io.BytesIO()
        profiles = pd.DataFrame(
            {
                "id": range(6),
                "time0": range(6),
                "time1": range(6),
                "time35135": range(6),
                "Unnamed: 35137": "",
            }
        )
        with zipfile.ZipFile(payload, "w") as archive:
            for filename in ["load_profiles_tabsep.csv", "hlt_profiles_tabsep.csv"]:
                archive.writestr(filename, profiles.to_csv(sep="\t", index=False))
            archive.writestr(
                "master_data_tabsep.csv",
                pd.DataFrame({"id": range(6)}).to_csv(sep="\t", index=False),
            )
        response = MagicMock()
        response.__enter__.return_value = response
        response.iter_content.side_effect = lambda *args: iter([payload.getvalue()])
        crawler = IndustrialLoadProfileCrawler(
            "vea", {"db_uri": "sqlite://", "max_profiles": 5}
        )
        with patch(
            "oeds.crawler.vea_industrial_load_profiles.requests.get",
            return_value=response,
        ):
            crawler.crawl_structural(recreate=True)
            crawler.crawl_structural(recreate=True)
        for table in ["load", "high_load_times"]:
            result = pd.read_sql_table(table, crawler.engine)
            self.assertEqual(len(result), 15)
            self.assertEqual(sorted(result.value.tolist()), sorted(list(range(5)) * 3))
            times = pd.to_datetime(result.timestamp, utc=True)
            self.assertEqual(times.min(), pd.Timestamp("2015-12-31T23:00:00Z"))
            self.assertEqual(times.max(), pd.Timestamp("2016-12-31T22:45:00Z"))


if __name__ == "__main__":
    unittest.main()
