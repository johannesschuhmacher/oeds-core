# SPDX-FileCopyrightText: Vassily Aliseyko
#
# SPDX-License-Identifier: AGPL-3.0-or-later

"""
The Charging station map is available at:
https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/Karte/Ladesaeulenkarte.html
One can download the raw file as CSV from this link:
https://www.bundesnetzagentur.de/SharedDocs/Downloads/DE/Sachgebiete/Energie/Unternehmen_Institutionen/E_Mobilitaet/Ladesaeulenregister_CSV.csv?__blob=publicationFile&v=42
"""

import logging
from urllib.parse import urljoin, urlparse

import pandas as pd
import requests
from bs4 import BeautifulSoup
from sqlalchemy import text

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config

log = logging.getLogger("ladesaeulenregister")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "ladesaeulenregister",
    "data_date": "2025-07-18",
    "data_source": "https://www.bundesnetzagentur.de/DE/Fachthemen/ElektrizitaetundGas/E-Mobilitaet/Ladesaeulenkarte/start.html",
    "license": "CC-BY-4.0",
    "description": "Charging stations for EV. Coordinate referenced power usage of individual chargers.",
    "contact": "",
    "temporal_start": None,
    "temporal_end": None,
}


class LadesaeulenregisterCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from ladesaeulenregister limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            log.info("Crawling Ladesäulenregister")
            page = requests.get(metadata_info["data_source"], timeout=45)
            page.raise_for_status()
            links = BeautifulSoup(page.text, "html.parser").select("a[href]")
            url = next(
                urljoin(page.url, link["href"])
                for link in links
                if "Ladesaeulenregister" in link["href"]
                and urlparse(link["href"]).path.endswith(".csv")
            )
            with requests.get(url, stream=True, timeout=90) as response:
                response.raise_for_status()
                response.raw.decode_content = True
                df = pd.read_csv(
                    response.raw,
                    skiprows=10,
                    delimiter=";",
                    encoding="iso-8859-1",
                    index_col=0,
                    decimal=",",
                    low_memory=False,
                    nrows=self.config.get("max_rows"),
                )

            with self.engine.begin() as conn:
                df.to_sql("ladesaeulenregister", conn, if_exists="replace")
            log.info("Finished writing Ladesäulenregister to Database")


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    mastr = LadesaeulenregisterCrawler("ladesaeulenregister", config=config)
    mastr.crawl_structural(recreate=False)
