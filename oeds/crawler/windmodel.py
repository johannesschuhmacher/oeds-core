# SPDX-FileCopyrightText: Florian Maurer, Christian Rieke
#
# SPDX-License-Identifier: AGPL-3.0-or-later

import logging

import json5  # parse js-dict to python
import numpy as np
import pandas as pd
import requests
from bs4 import BeautifulSoup  # parse html
from sqlalchemy import text
from tqdm import tqdm  # fancy for loop

from oeds.base_crawler import DEFAULT_CONFIG_LOCATION, DownloadOnceCrawler, load_config

"""
Downloads the available powercurve data from https://www.wind-turbine-models.com/ to a csv file

The raw measured data from https://www.wind-turbine-models.com/powercurves is used.

Interpolated values would be available from the individual wind turbines page are also available but are harder to crawl:

e.g.: https://www.wind-turbine-models.com/turbines/1502-fuhrlaender-llc-wtu3.0-120

Therefore, interpolation from scipy is used.
For the given model, this interpolation is not good, as it produces negative values (which are nulled in the script)

The resulting data is not available under an open-source license and should not be reshared but is available for crawling yourself.
"""


log = logging.getLogger("windmodel")
log.setLevel(logging.INFO)


metadata_info = {
    "schema_name": "windmodel",
    "data_date": "2024-06-12",
    "data_source": "https://www.wind-turbine-models.com/powercurves",
    "license": "https://www.wind-turbine-models.com/terms",
    "description": "Wind turbine performance. Wind turbine test performance data by model.",
}


def get_turbines_with_power_curve():
    # create list of turbines with available powercurves
    page = requests.get("https://www.wind-turbine-models.com/powercurves", timeout=45)
    page.raise_for_status()
    soup = BeautifulSoup(page.text, "html.parser")
    # pull all text from the div
    name_list = soup.find(class_="chosen-select")

    wind_turbines_with_curve = []
    for i in name_list.find_all("option"):
        wind_turbines_with_curve.append(i.get("value"))

    return wind_turbines_with_curve


def download_turbine_curve(turbine_id, start=0, stop=25) -> pd.DataFrame:
    url = "https://www.wind-turbine-models.com/powercurves"
    headers = dict()
    headers["Content-Type"] = "application/x-www-form-urlencoded"
    data = {
        "_action": "compare",
        "turbines[]": turbine_id,
        "windrange[]": [start, stop],
    }

    resp = requests.post(url, headers=headers, data=data, timeout=45)
    resp.raise_for_status()
    strings = resp.json()["result"]
    begin = strings.find("data:")
    end = strings.find('"}]', begin)
    relevant_js = "{" + strings[begin : end + 3] + "}}"
    curve_as_dict = json5.loads(relevant_js)
    x = curve_as_dict["data"]["labels"]
    y = curve_as_dict["data"]["datasets"][0]["data"]
    label = curve_as_dict["data"]["datasets"][0]["label"]
    url = curve_as_dict["data"]["datasets"][0]["url"]
    df = pd.DataFrame(np.asarray(y, dtype=float), index=x, columns=[label])
    try:
        df = df.interpolate(method="polynomial", order=3)
        df = df.fillna(0)
    except Exception as e:
        log.error(f"Error: {e}")
    df.index.name = "wind_speed"
    return df


def download_all_turbines() -> pd.DataFrame:
    wind_turbines = get_turbines_with_power_curve()
    curves = []
    for turbine_id in tqdm(wind_turbines):
        curve = download_turbine_curve(turbine_id)
        curves.append(curve)
    df = pd.concat(curves, axis=1)
    all_turbines_trunc = df[df.any(axis=1)]
    df = all_turbines_trunc.fillna(0)
    df[df < 0] = 0
    # Vestas V27 exists twice in column names
    return df.loc[:, ~df.columns.duplicated()]


class WindTurbineCrawler(DownloadOnceCrawler):
    def structure_exists(self) -> bool:
        try:
            query = text("SELECT 1 from turbine_data limit 1")
            with self.engine.connect() as conn:
                return conn.execute(query).scalar() == 1
        except Exception:
            return False

    def crawl_structural(self, recreate: bool = False):
        if not self.structure_exists() or recreate:
            wind_turbines = download_all_turbines()
            with self.engine.begin() as conn:
                wind_turbines.to_sql("turbine_data", conn, if_exists="replace")


if __name__ == "__main__":
    logging.basicConfig()

    config = load_config(DEFAULT_CONFIG_LOCATION)
    craw = WindTurbineCrawler("windmodel", config)
    craw.crawl_structural(recreate=False)

    # turbine_data = download_all_turbines()
    # with open("turbine_data.csv", "w") as f:
    #     turbine_data.to_csv(f)
    # turbine_data = pd.read_csv("turbine_data.csv", index_col=0)
