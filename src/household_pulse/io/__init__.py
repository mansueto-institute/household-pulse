"""
Subpackage for input/output functions.
"""
import logging
from functools import lru_cache

import pandas as pd

from household_pulse.io.census import Census
from household_pulse.io.s3 import S3Storage

__all__ = ["load_gsheet", "Census", "S3Storage"]

logger = logging.getLogger(__name__)


@lru_cache(maxsize=10)
def load_gsheet(sheetname: str) -> pd.DataFrame:
    """
    Loads one of the three crosstabs used for mapping responses. It has to
    be one of {'question_mapping', 'response_mapping,
    'county_metro_state'}.

    Args:
        sheetname (str): sheetname in the data dictionary google sheet

    Returns:
        pd.DataFrame: loaded crosstab
    """
    baseurl = "https://docs.google.com/spreadsheets/d"
    ssid = "1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo"

    sheetids = {
        "question_mapping": "34639438",
        "response_mapping": "1561671071",
        "county_metro_state": "974836931",
        "numeric_mapping": "1572193173",
    }

    if sheetname not in sheetids:
        raise ValueError(f"{sheetname} not in {sheetids.keys()}")

    logger.info("Loading Google Sheet %s as a csv", sheetname)
    df = pd.read_csv(
        f"{baseurl}/{ssid}/export?format=csv&gid={sheetids[sheetname]}"
    )
    df = df.dropna(how="all")

    return df
