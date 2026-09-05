# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
Synthetic data from OpenEGO project.

More information found here:
https://openenergyplatform.org/database/tables/ego_dp_loadarea

and here: https://github.com/openego/eGon-data
"""

import logging

import pandas as pd
import requests
from sqlalchemy import text

from oeds.base_crawler import (
    DEFAULT_CONFIG_LOCATION,
    DownloadOnceCrawler,
    crawler_data_dir,
    load_config,
)

log = logging.getLogger("oep")
log.setLevel(logging.INFO)

# the file is about 10GB of size
ego_url = "https://openenergyplatform.org/api/v0/tables/ego_dp_loadarea/rows/?form=csv"


class OepCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from ego_demand limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            self.crawl_oep()

    def crawl_oep(self, oep_ego_file_path=None):
        """
        synthetic demand data for German NUTS areas from OpenEGO project
        """
        max_rows = self.config.get("max_rows")
        if max_rows:
            # A bounded request must not become the cache for a later full import.
            with requests.get(
                ego_url, params={"limit": int(max_rows)}, stream=True, timeout=90
            ) as response:
                response.raise_for_status()
                response.raw.decode_content = True
                self.write_demand(
                    pd.read_csv(response.raw, nrows=int(max_rows), chunksize=500)
                )
            return
        oep_ego_file_path = oep_ego_file_path or crawler_data_dir() / "oep_ego.csv"
        if oep_ego_file_path.is_file():
            log.info("%s already exists", oep_ego_file_path)
        else:
            partial = oep_ego_file_path.with_suffix(".download")
            with requests.get(ego_url, stream=True, timeout=90) as response:
                response.raise_for_status()
                with partial.open("wb") as target:
                    for chunk in response.iter_content(1024 * 1024):
                        target.write(chunk)
            partial.replace(oep_ego_file_path)
            log.info("downloaded ego_demand to %s", oep_ego_file_path)
        self.write_demand(pd.read_csv(oep_ego_file_path, chunksize=500))

    def write_demand(self, chunks):
        with self.engine.begin() as conn:
            for index, demand in enumerate(chunks):
                # NUTS geometry is already available in the public schema.
                values = demand.drop(
                    columns=[
                        "geom",
                        "geom_centre",
                        "geom_surfacepoint",
                        "geom_centroid",
                    ]
                )
                values.to_sql(
                    "ego_demand",
                    con=conn,
                    if_exists="replace" if index == 0 else "append",
                    chunksize=500,
                )
        log.info("ego_demand data written successfully")


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    craw = OepCrawler("oep", config)
    craw.crawl_structural(recreate=True)
