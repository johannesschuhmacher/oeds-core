# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Consumption data of various households in london gathered from 2011 to 2014.
A sub-set of 1,100 customers (Dynamic Time of Use or dToU) were given specific times when their electricity tariff would be higher or lower price than normal
High (67.20p/kWh), Low (3.99p/kWh) or normal (11.76p/kWh).
The rest of the sample (around 4,500) were on a flat rate of 14.228p/kWh.
https://data.london.gov.uk/blog/electricity-consumption-in-a-sample-of-london-households/
"""

import io
import logging
import zipfile

import pandas as pd
import requests
from sqlalchemy import text

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config

log = logging.getLogger(__name__)
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "londondatastore",
    "data_date": "2014-02-28",
    "data_source": "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/3527bf39-d93e-4071-8451-df2ade1ea4f2/LCL-FullData.zip",
    "license": "CC-BY-4.0",
    "description": "London energy consumption data. Real consumption data from london, timestamped.",
    "contact": "",
    "temporal_start": "2011-11-23 09:00:00",
    "temporal_end": "2014-02-28 00:00:00",
}

LONDON_FULL_URL = "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/3527bf39-d93e-4071-8451-df2ade1ea4f2/LCL-FullData.zip"
LONDON_PARTITIONED_URL = "https://data.london.gov.uk/download/smartmeter-energy-use-data-in-london-households/04feba67-f1a3-4563-98d0-f3071e3d56d1/Partitioned%20LCL%20Data.zip"


class LondonLoadData(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from consumption limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            self.download_london_data()
        self.create_hypertable_if_not_exists()

    def download_london_data(self):
        log.info("Download london smartmeter energy dataset")
        response = requests.get(LONDON_PARTITIONED_URL, timeout=90)
        response.raise_for_status()
        with zipfile.ZipFile(io.BytesIO(response.content)) as thezip:
            # should be single file only if full_data
            log.info("Write london energy dataset to database")
            files = [
                info for info in thezip.infolist() if info.filename.endswith(".csv")
            ]
            if self.config.get("max_files"):
                files = files[: int(self.config["max_files"])]
            for zipinfo in files:
                with thezip.open(zipinfo) as thefile:
                    df = pd.read_csv(
                        thefile,
                        parse_dates=["DateTime"],
                        index_col="DateTime",
                        nrows=self.config.get("max_rows"),
                    )

                    df.columns = [col.strip() for col in df.columns]
                    df.rename(
                        columns={
                            "KWH/hh (per half hour)": "power",
                            "stdorToU": "tariff",
                        },
                        inplace=True,
                    )
                    with self.engine.begin() as conn:
                        df.to_sql(
                            "consumption", conn, if_exists="append", chunksize=10000
                        )
        log.info("Finished writing london smartmeter energy dataset to Database")

    def create_hypertable_if_not_exists(self) -> None:
        self.create_single_hypertable_if_not_exists("consumption", "DateTime")


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    craw = LondonLoadData("londondatastore", config)
    craw.crawl_structural(recreate=False)
