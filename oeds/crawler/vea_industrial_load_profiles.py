# SPDX-FileCopyrightText: Florian Maurer
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging
import tempfile
import zipfile

import pandas as pd
import requests
from sqlalchemy import text
from tqdm import tqdm

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config

log = logging.getLogger("vea-industrial-load-profiles")


metadata_info = {
    "schema_name": "vea_industrial_load_profiles",
    "data_date": "2016-01-01",
    "data_source": "https://zenodo.org/records/13910298",
    "license": "CC-BY-4.0",
    "description": """The data consists of 5359 one-year quarterhourly industrial load profiles (2016, leap year, 35136 values).
    Each describes the electricity consumption of one industrial commercial site in Germany used for official accounting.
    Local electricity generation was excluded from the data as far as it could be discovered (no guarantee of completeness).
    Together with load profiles comes respective master data of the industrial sites as well as the information wether each quarterhour was a high load time of the connected German grid operator in 2016.
    The data was collected by the VEA.
    The dataset as a whole was assembled by Paul Hendrik Tiemann in 2017 by selecting complete load profiles without effects of renewable generation from a VEA internal database.
    It is a research dataset and was used for master theses and publications.""",
    "contact": "komanns@fh-aachen.de",
    "temporal_start": "2016-01-01 00:00:00",
    "temporal_end": "2016-12-31 23:45:00",
    "concave_hull_geometry": None,
}


class IndustrialLoadProfileCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from high_load_times limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            self.request_extract_zip_archive()
            try:
                # Melt only a small profile block, not 188 million values at once.
                for filename, table in [
                    ("load_profiles_tabsep.csv", "load"),
                    ("hlt_profiles_tabsep.csv", "high_load_times"),
                    ("master_data_tabsep.csv", "master"),
                ]:
                    first = True
                    with self.archive.open(filename) as file:
                        for frame in pd.read_csv(
                            file,
                            sep="\t",
                            chunksize=4,
                            nrows=self.config.get("max_profiles"),
                        ):
                            self.df = frame
                            if table == "master":
                                self.lower_column_names()
                            else:
                                self.create_timestep_datetime_dict(self.df.columns)
                                self.transform_load_hlt_data(name=table)
                            self.write_to_database(name=table, replace=first)
                            first = False
            finally:
                self.archive.close()
                self.archive_file.close()
            self.create_hypertable_if_not_exists()

    def request_extract_zip_archive(self):
        """
        Requests zip archive for industrial load profiles from zenodo.
        """

        url = (
            "https://zenodo.org/records/13910298/files/load-profile-data.zip?download=1"
        )

        log.info("Requesting zip archive from zenodo")

        self.archive_file = tempfile.TemporaryFile()
        try:
            with requests.get(url, stream=True, timeout=90) as response:
                response.raise_for_status()
                for block in response.iter_content(1024 * 1024):
                    self.archive_file.write(block)
            self.archive_file.seek(0)
            self.archive = zipfile.ZipFile(self.archive_file)
        except Exception:
            self.archive_file.close()
            raise

    def read_file(self, filename: str | None = None):
        """Reads the given file and returns contents as pd.DataFrame.

        Args:
            filename (str | None, default: None): The name of the file being read.
        """

        log.info(f"Trying to read file {filename} into pd.DataFrame")

        if filename == "master":
            name = "master_data_tabsep.csv"
        elif filename == "load":
            name = "load_profiles_tabsep.csv"
        elif filename == "hlt":
            name = "hlt_profiles_tabsep.csv"

        with self.archive.open(name) as file:
            self.df = pd.read_csv(file, sep="\t")

        log.info("Succesfully read file into pd.DataFrame")

    def create_timestep_datetime_dict(self, columns: list[str]):
        """Creates a dictionary mapping the timesteps (time0, time1, ...) to pd.Timestamp objects.

        Args:
            columns (list[str]): Columns of either the load or hlt profile dataframe (the timesteps).
        """

        log.info("Creating dictionary for timesteps mapping")

        timesteps = list(columns.difference(["id", "Unnamed: 35137"]))

        timestamps = pd.date_range(
            start="2016-01-01 00:00:00",
            end="2016-12-31 23:45:00",
            freq="15min",
            tz="Europe/Berlin",
        )

        timestamps = timestamps.tz_convert("UTC")

        self.timestep_datetime_map = {}
        for timestep in timesteps:
            idx = int(timestep.split("time")[1])
            self.timestep_datetime_map[timestep] = timestamps[idx]

        log.info("Succesfully created dictionary")

    def transform_load_hlt_data(self, name: str | None = None):
        """Transform dataframe of load or hlt profiles into long format.

        Args:
            name (str | None, default None): a
        """

        log.info(f"Trying to convert {name} dataframe")

        # remove unused column
        self.df.drop(columns="Unnamed: 35137", inplace=True)

        # change to wide format
        self.df = self.df.melt(id_vars="id", var_name="timestamp")

        # map timestamps onto timestamp column
        self.df["timestamp"] = self.df["timestamp"].map(self.timestep_datetime_map)

        log.info("Succesfully converted hlt / load profile")

    def write_to_database(self, name: str, replace: bool = False) -> None:
        """Writes dataframe to database.

        Args:
            name (str): The name of the table to insert data to.
        """

        log.info(f"Trying to write {name} to database")

        rows = 200000
        for start in tqdm(range(0, len(self.df), rows)):
            df = self.df.iloc[start : start + rows]
            df.to_sql(
                name=name,
                con=self.engine,
                if_exists="replace" if replace and start == 0 else "append",
                index=False,
            )

        log.info("Succesfully inserted into databse")

    def lower_column_names(self):
        self.df.columns = [x.lower() for x in self.df.columns]

    def create_hypertable_if_not_exists(self):
        self.create_single_hypertable_if_not_exists("high_load_times", "timestamp")
        self.create_single_hypertable_if_not_exists("load", "timestamp")


if __name__ == "__main__":
    logging.basicConfig(
        level=logging.INFO,
        format="%(asctime)s.%(msecs)03d %(levelname)s %(module)s - %(funcName)s: %(message)s",
        datefmt="%d-%m-%Y %H:%M:%S",
    )

    config = load_config(DEFAULT_CONFIG_LOCATION)
    crawler = IndustrialLoadProfileCrawler("vea_industrial_load_profiles", config)
    crawler.crawl_structural()
    crawler.set_metadata(metadata_info)
