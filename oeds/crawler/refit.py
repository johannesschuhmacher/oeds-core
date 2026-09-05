# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Data from REFIT paper
https://www.nature.com/articles/sdata2016122

REFIT (An electrical load measurements dataset of United Kingdom households from a two-year longitudinal study)

This dataset is typically used for NILM applications (non-intrusive load monitoring).
"""

import logging
from pathlib import Path
from tempfile import TemporaryDirectory

import pandas as pd
import py7zr
import requests
from sqlalchemy import text

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "refit",
    "data_date": "2024-06-12",
    "data_source": "https://pure.strath.ac.uk/ws/portalfiles/portal/52873459/Processed_Data_CSV.7z",
    "license": "CC-BY-4.0",
    "description": "University of Strathclyde household energy usage. Time-stamped data on various household appliances' energy consumption, detailing usage patterns across different homes.",
    "contact": "",
    "temporal_start": "2013-10-09 13:06:17",
    "temporal_end": "2015-07-10 11:56:32",
}


REFIT_URL = (
    "https://pure.strath.ac.uk/ws/portalfiles/portal/52873459/Processed_Data_CSV.7z"
)


class RefitCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from refit limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            log.info("Download refit dataset")
            self.download_refit_data()
            log.info("Finished writing REFIT to Database")
        self.create_hypertable_if_not_exists()

    def create_hypertable_if_not_exists(self):
        self.create_single_hypertable_if_not_exists("refit", "Time")

    def download_refit_data(self):
        with TemporaryDirectory(prefix="oeds-refit-") as directory:
            archive_path = Path(directory) / "refit.7z"
            with requests.get(REFIT_URL, stream=True, timeout=90) as response:
                response.raise_for_status()
                with archive_path.open("wb") as output:
                    for block in response.iter_content(1024 * 1024):
                        output.write(block)
            with py7zr.SevenZipFile(archive_path, mode="r") as archive:
                names = [name for name in archive.getnames() if name.endswith(".csv")]
                if self.config.get("max_houses"):
                    names = names[: int(self.config["max_houses"])]
                archive.extract(path=directory, targets=names)
            for name in names:
                for df in pd.read_csv(
                    Path(directory) / name,
                    index_col="Time",
                    parse_dates=["Time"],
                    chunksize=10000,
                    nrows=self.config.get("max_rows"),
                ):
                    del df["Unix"]
                    df["house"] = name
                    with self.engine.begin() as conn:
                        df.to_sql("refit", conn, if_exists="append", chunksize=10000)


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    craw = RefitCrawler("refit", config)
    craw.crawl_structural()
