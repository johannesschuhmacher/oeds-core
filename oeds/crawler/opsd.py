# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
import sqlite3
from contextlib import closing
from io import StringIO
from pathlib import Path

import pandas as pd
import requests
from sqlalchemy import text

from oeds.base_crawler import (
    DEFAULT_CONFIG_LOCATION,
    DownloadOnceCrawler,
    crawler_data_dir,
    load_config,
)

log = logging.getLogger("opsd")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "opsd",
    "data_date": "2020-12-31",
    "data_source": "https://data.open-power-system-data.org/when2heat/latest/when2heat.sqlite",
    "license": "CC-BY-4.0",
    "description": "Open Power System Data. When to heat dataset, heating profiles for differenz countries & systems.",
    "contact": "",
    "temporal_start": "2007-12-31 22:00:00",
    "temporal_end": "2020-12-31 23:00:00",
}


when2heat_url = (
    "https://data.open-power-system-data.org/when2heat/latest/when2heat.sqlite"
)
national_generation_capacity_url = "https://data.open-power-system-data.org/national_generation_capacity/2020-10-01/national_generation_capacity_stacked.csv"


class OpsdCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from national_generation_capacity limit 1")
            with self.engine.connect() as conn:
                capacities_exists = conn.execute(query).scalar() == 1

            query = text("SELECT 1 from when2heat limit 1")
            with self.engine.connect() as conn:
                when2heat_exists = conn.execute(query).scalar() == 1
            return when2heat_exists and capacities_exists
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            self.crawl_capacities()
            self.write_when2_heat()
        self.create_hypertable_if_not_exists()

    def create_hypertable_if_not_exists(self):
        self.create_single_hypertable_if_not_exists("when2heat", "utc_timestamp")

    def write_when2_heat(self, db_path: Path | None = None):
        """
        efficiency of heat pumps in different countries for different types of heatpumps
        """
        db_path = db_path or crawler_data_dir() / "when2heat.db"
        if db_path.is_file():
            log.info(f"{db_path} already exists")
        else:
            with requests.get(when2heat_url, stream=True, timeout=90) as response:
                response.raise_for_status()
                temporary = db_path.with_suffix(".download")
                with temporary.open("wb") as f:
                    for block in response.iter_content(1024 * 1024):
                        f.write(block)
                temporary.replace(db_path)
            log.info(f"downloaded when2heat.db to {db_path}")

        query = "SELECT * FROM when2heat ORDER BY utc_timestamp"
        if self.config.get("max_rows"):
            query += f" LIMIT {int(self.config['max_rows'])}"
        with closing(sqlite3.connect(db_path)) as source, self.engine.begin() as conn:
            mode = "replace"
            for data in pd.read_sql(query, source, chunksize=1000):
                data.index = pd.to_datetime(data["utc_timestamp"])
                del data["cet_cest_timestamp"]
                del data["utc_timestamp"]
                data.to_sql("when2heat", conn, if_exists=mode, chunksize=1000)
                mode = "append"
        log.info("when2heat data written successfully")

    def crawl_capacities(self):
        log.info("Fetching data from %s", national_generation_capacity_url)
        response = requests.get(national_generation_capacity_url)
        response.raise_for_status()

        log.info("Loading OPSD capacities into DataFrame")
        data = pd.read_csv(StringIO(response.text))

        log.info("Writing OPSD capacities to the database")
        with self.engine.begin() as conn:
            data.to_sql(
                "national_generation_capacity",
                conn,
                if_exists="replace",
            )
        log.info("OPSD capacities written successfully")


if __name__ == "__main__":
    logging.basicConfig()
    from pathlib import Path

    config = load_config(DEFAULT_CONFIG_LOCATION)
    craw = OpsdCrawler("opsd", config)
    craw.crawl_structural(recreate=True)
