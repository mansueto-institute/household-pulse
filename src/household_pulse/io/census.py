# -*- coding: utf-8 -*-
"""
Created on 2023-01-20 04:56:32-06:00
===============================================================================
@filename:  census.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   This module contains a class whose responsibility is to deal with
            any census data that is needed for the project.
===============================================================================
"""
import logging
import re
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from io import BytesIO
from typing import ClassVar
from zipfile import ZipFile

import pandas as pd
import requests
from bs4 import BeautifulSoup

from household_pulse.io.base import IO

logger = logging.getLogger(__name__)


@dataclass
class Census(IO):
    """
    This class contains all the census data that is needed for the project.
    """

    url: ClassVar[
        str
    ] = "https://www2.census.gov/programs-surveys/demo/datasets/hhp/"

    def download(self) -> pd.DataFrame:
        """
        Downloads the data from the census website and returns a dataframe.

        Returns:
            pd.DataFrame: Dataframe with the data from the census website
        """
        logger.info(
            "Downloading files from the census website for week %s", self.week
        )
        url = "".join((self.url, self._make_data_url()))
        r = requests.get(url, timeout=10)

        with ZipFile(BytesIO(r.content), mode="r") as zipfile:
            with zipfile.open(self._make_data_fname(fname="d")) as datacsv:
                data_df = pd.read_csv(datacsv, dtype={"SCRAM": "string"})

            with zipfile.open(self._make_data_fname(fname="w")) as weightcsv:
                weight_df = pd.read_csv(weightcsv, dtype={"SCRAM": "string"})

        if self.week < 13:
            hwgdf = self._download_hh_weights()
            weight_df = weight_df.merge(
                hwgdf, how="inner", on=["SCRAM", "WEEK"]
            )

        df = data_df.merge(weight_df, how="left", on=["SCRAM", "WEEK"])
        df = df.copy()

        return df

    def _download_hh_weights(self) -> pd.DataFrame:
        """
        For weeks below 13, the household weights are in a separate file. This
        method fetches those weights directly from the census' website.

        Args:
            week (int): The week of data to download

        Returns:
            pd.DataFrame: household weights file
        """
        logger.info("Download household weights for week %s", self.week)
        if self.week >= 13:
            raise ValueError(
                f"This should only be used for weeks < 13. Week is {self.week}"
            )

        hweight_url = "".join(
            (
                self.url,
                self._make_data_url(hweights=True),
            )
        )
        hwgdf = pd.read_csv(hweight_url)
        return hwgdf

    def _make_data_url(self, hweights: bool = False) -> str:
        """
        Helper function to get string for file to download from census api

        Args:
            hweights (bool): make url for household weights that for weeks < 13
                are in a separate file in the census' ftp.

        Returns:
            str: the year/week/file.zip to be downloaded
        """
        if hweights and self.week > 12:
            raise ValueError("hweights can only be passed for weeks 1-12")

        year = self.get_week_year_map()[self.week]

        if hweights:
            return (
                f"{year}/wk{self.week}/pulse{year}_puf_hhwgt_{self.week_str}"
                ".csv"
            )
        return f"{year}/wk{self.week}/HPS_Week{self.week_str}_PUF_CSV.zip"

    def _make_data_fname(self, fname: str) -> str:
        """
        Helper function to get the string names of the files downloaded

        Args:
            week (int): the week of data to download
            fname (str): the file to dowload (d: main data file, w: weights
                file)

        Returns:
            str: name of file downloaded
        """
        if fname not in {"d", "w"}:
            raise ValueError("fname muts be in {'d', 'w'}")

        year = self.get_week_year_map()[self.week]
        # 2023 week 52 is actually 2022 week 52 due to a census bug
        if year == 2023 and self.week == 52:
            year = 2022
        if fname == "d":
            return f"pulse{year}_puf_{self.week_str}.csv"
        return f"pulse{year}_repwgt_puf_{self.week_str}.csv"

    @staticmethod
    @lru_cache(maxsize=10)
    def get_week_year_map() -> dict[int, int]:
        """
        creates a dictionary that maps each week to a year
        """
        logger.info(
            "Scraping the census website to construct the year mapping"
        )
        r = requests.get(Census.url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        yearlinks = soup.find_all("a", {"href": re.compile(r"\d{4}/")})
        years: list[str] = [yearlink.get_text() for yearlink in yearlinks]

        weekyrmap = {}
        for year in years:
            yearint = int(re.sub(r"\D", "", year))
            r = requests.get("".join((Census.url, year)), timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
            weeklinks = soup.find_all("a", {"href": re.compile(r"wk\d{1,2}/")})
            weeks: list[str] = [weeklink.get_text() for weeklink in weeklinks]
            for week in weeks:
                weekint = int(re.sub(r"\D", "", week))
                weekyrmap[weekint] = yearint

        return weekyrmap

    @staticmethod
    def load_collection_dates() -> dict[int, dict[str, date]]:
        """
        Scrapes date range meta data for each release of the Household Pulse
        data

        Returns:
            dict[int, dict[str, date]]]: dictionary with weeks as keys and the
                publication and collection dates.
        """
        logger.info("Scraping the census website for collection dates")

        weekpat = re.compile(r"Week (\d{1,2})")
        monthpat = re.compile(r"[A-z]+ \d{1,2}(?:, \d{4})?")

        url = "/".join(
            (
                "https://www.census.gov",
                "programs-surveys",
                "household-pulse-survey",
                "data.html",
            )
        )
        page = requests.get(url, timeout=10)
        soup = BeautifulSoup(page.content, "html.parser")
        phases = soup.find_all(
            "div", {"class": "data-uscb-list-articles-container"}
        )
        results = {}
        for phase in phases:  # pragma: no branch
            if "Data Tool" in phase.text:
                break

            wektexts = phase.find_all("div", "uscb-default-x-column-title")
            pubtexts = phase.find_all("div", "uscb-default-x-column-date")
            coltexts = phase.find_all("div", "uscb-default-x-column-content")
            for wektext, pubtext, coltext in zip(wektexts, pubtexts, coltexts):
                pubdate = datetime.strptime(pubtext.text.strip(), "%B %d, %Y")

                colstrs = re.findall(monthpat, coltext.text)
                enddate = datetime.strptime(colstrs[1], "%B %d, %Y")
                # if both collections dates happen in the same year they don't
                # put the year on the start date :flip-table:
                try:
                    startdate = datetime.strptime(colstrs[0], "%B %d, %Y")
                except ValueError:
                    startdate = datetime.strptime(
                        ", ".join((colstrs[0], str(enddate.year))), "%B %d, %Y"
                    )

                week = int(re.findall(weekpat, wektext.text)[0])

                results[week] = {
                    "pub_date": pubdate.date(),
                    "start_date": startdate.date(),
                    "end_date": enddate.date(),
                }

        return results
