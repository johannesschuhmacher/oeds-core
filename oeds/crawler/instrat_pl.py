# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later
"""
This retrieves EU ETS prices from a polish website.

The EU Emission Trading System (ETS) sells emission allowances to
companies that emit greenhouse gases.

For this lots of 1000t CO2eq are sold with a given price in €/t CO2eq.
"""

import io
import logging
from datetime import datetime, timedelta

import pandas as pd
import requests
import yfinance as yf
from sqlalchemy import text

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, ContinuousCrawler, load_config

log = logging.getLogger("instrat_pl")
log.setLevel(logging.INFO)

metadata_info = {
    "schema_name": "eu_ets",
    "data_source": "https://energy.instrat.pl/en/prices/eu-ets/",
    "license": "CC-BY-4.0",
    "description": "EU-ETS, coal and gas prices from polish energy provider",
    "contact": "",
    "temporal_start": "2012-01-03",
}

# hint: one can call ?all=1 to get all data
# €/tCO2
EU_ETS_URL = "https://energy-api.instrat.pl/api/prices/co2"
# coal used for electricity generation PLN/GJ or PLN/t
COAL_URL = "https://energy-api.instrat.pl/api/coal/pscmi_1"
# the heat_url switches between Y-m-d and Y-d-m and is not practicable to parse
COAL_HEAT_URL = "https://energy-api.instrat.pl/api/coal/pscmi_2"
# gas used for electricity generation PLN/MWh
GAS_URL = "https://energy-api.instrat.pl/api/prices/gas_price_rdn_daily"
TEMPORAL_START = datetime(2012, 1, 3)


class InstratPlCrawler(ContinuousCrawler):
    TIMEDELTA = timedelta(days=2)

    def get_latest_data(self) -> datetime:
        query = text("SELECT MAX(date) FROM eu_ets")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() + timedelta(hours=12)
        except Exception:
            log.error("No instrat_pl data found")
            return TEMPORAL_START

    def get_first_data(self) -> datetime:
        query = text("SELECT MIN(datum_von) FROM generation")
        try:
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() or TEMPORAL_START
        except Exception:
            log.error("No intrat_pl data found")
            return TEMPORAL_START

    def crawl_from_to(self, begin: datetime, end: datetime):
        """Crawls data from begin (inclusive) until end (exclusive)

        Args:
            begin (datetime): included begin datetime from which to crawl
            end (datetime): exclusive end datetime until which to crawl
        """
        date_from = begin.strftime("%d-%m-%YT%H:%M:%SZ")
        date_to = end.strftime("%d-%m-%YT%H:%M:%SZ")
        params = {
            "date_from": date_from,
            "date_to": date_to,
        }
        headers = {
            "User-Agent": "Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/58.0.3029.110 Safari/537.3"
        }
        log.info(f"Downloading instrat_pl data from {date_from} to {date_to}")

        def download_data(url):
            data = requests.get(url, params=params, headers=headers, timeout=45)
            data.raise_for_status()
            df = pd.read_json(io.StringIO(data.text))
            if df.empty:
                raise ValueError(
                    f"No Instrat data returned for {date_from} to {date_to}: {url}"
                )
            df = df.set_index("date")
            df.index = df.index.tz_localize(None)
            return df

        eu_ets_data_raw = download_data(EU_ETS_URL)

        eu_ets_data = eu_ets_data_raw.resample("D").bfill()
        eu_ets_data.rename(columns={"price": "eur_per_tco2"}, inplace=True)

        ### COAL
        coal_data = download_data(COAL_URL)

        start = coal_data.index[0].strftime("%Y-%m-%d")
        end = coal_data.index[-1].strftime("%Y-%m-%d")
        pln_eur = yf.download("PLNEUR=X", start=start, end=end)["Close"]["PLNEUR=X"]
        # coal_data["pscmi1_pln_per_gj"].plot()
        resample_pln_eur = pln_eur.resample("MS").bfill().ffill()
        resample_pln_eur = resample_pln_eur.reindex(coal_data.index).ffill()
        coal_data["steam_coal_eur_per_gj"] = (
            coal_data["pscmi1_pln_per_gj"] * resample_pln_eur
        )
        coal_data["steam_coal_eur_per_t"] = (
            coal_data["pscmi1_pln_per_t"] * resample_pln_eur
        )
        # 1 GJ = 1e9 Ws = 1e9/3600 Wh = 1e6/3600 kWh
        coal_data["price_eur_per_kwh"] = coal_data["steam_coal_eur_per_gj"] / (
            1e6 / 3600
        )  # GJ to kWh

        ### GAS data
        gas_data = download_data(GAS_URL)

        start = gas_data.index[0].strftime("%Y-%m-%d")
        end = gas_data.index[-1].strftime("%Y-%m-%d")
        pln_eur = yf.download("PLNEUR=X", start=start, end=end)["Close"]["PLNEUR=X"]
        resample_pln_eur = pln_eur.reindex(gas_data.index).bfill().ffill()

        gas_data.rename(columns={"price": "price_pln_per_mwh"}, inplace=True)
        gas_data["price_eur_per_mwh"] = gas_data["price_pln_per_mwh"] * resample_pln_eur
        gas_data["price_eur_per_kwh"] = gas_data["price_eur_per_mwh"] / 1e3
        try:
            with self.engine.begin() as conn:
                eu_ets_data.to_sql(
                    name="eu_ets", con=conn, if_exists="append", index=True
                )
                coal_data.to_sql(
                    name="coal_price", con=conn, if_exists="append", index=True
                )
                gas_data.to_sql(
                    name="gas_price", con=conn, if_exists="append", index=True
                )
        except Exception:
            log.exception("error in instat_pl data download")

    def create_hypertable_if_not_exists(self) -> None:
        for table in ["eu_ets", "coal_price", "gas_price"]:
            self.create_single_hypertable_if_not_exists(table, "date")


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    mastr = InstratPlCrawler("instrat_pl", config=config)
    mastr.crawl_temporal()
