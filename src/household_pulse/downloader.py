# -*- coding: utf-8 -*-
"""
Created on Monday, 21st March 2022 7:00:00 pm
===============================================================================
@filename:  downloader.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   this module handles downloading the raw files from either the
            census website, from our own backup of raw files in S3, or our
            google sheet.
===============================================================================
"""
import json
import logging
import re
import tarfile
from dataclasses import dataclass, field
from datetime import date, datetime
from functools import lru_cache
from io import BytesIO
from zipfile import ZipFile

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup

logger = logging.getLogger(__name__)


@dataclass(unsafe_hash=True)
class DataLoader:
    """
    This class handles the (down) loading of raw pulse data from either the
    census or our backups from S3
    """

    week: int
    _week: int = field(init=False, repr=False)
    base_census_url: str = field(
        init=False,
        default="https://www2.census.gov/programs-surveys/demo/datasets/hhp/",
        repr=False,
    )
    s3: boto3.client = field(
        init=False, default=boto3.client("s3"), repr=False
    )

    @property  # type: ignore
    def week(self) -> int:
        """
        Gets the census week value.

        Returns:
            int: The census week value.
        """
        return self._week

    @week.setter
    def week(self, value: int) -> None:
        if isinstance(value, property):
            raise TypeError(
                "__init__() missing 1 required positional argument: 'week'"
            )
        self._week = value

    @property
    def week_str(self) -> str:
        """
        Returns the week as a string with leading zeros.

        Returns:
            str: The week as a string with leading zeros.
        """
        return f"{str(self.week).zfill(2)}"

    def load_week(self) -> pd.DataFrame:
        """
        This methods attempts to download the passed week's raw response data.
        It first checks S3, and if the file is not found in S3 it checks the
        Census' website. If it finds it in the Census' website it downloads
        the responses with the available weights into a pandas dataframe and
        uploads a copy to S3 for future use.

        Args:
            week (int): The pulse week value.

        Returns:
            pd.DataFrame: The raw responses with merged weights.
        """
        try:
            df = self.download_from_s3()
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                df = self.download_from_census()
                self.upload_to_s3(df=df)
            else:
                logger.error(e)
                raise e

        return df

    def tar_and_upload_to_s3(
        self, bucket: str, tarname: str, files: dict[str, dict]
    ) -> None:
        """
        this method takes in a dictionary that contain json serializable data
        as values and corresponding file names as keys. all the key-value
        pairs are added to a tar rfile as individual json files, which is then
        uploaded to a location in S3 using the `tarname` as the object key.

        Args:
            bucket (str): target bucket
            tarname (str): object key, or name of the tar file
            files (dict[str, dict]): keys as file names in the archive with
                the data as its values.
        """
        fileobj = BytesIO()
        with tarfile.open(mode="w:gz", fileobj=fileobj) as tar_file:
            for fname, data in files.items():
                logger.info("Compressing cache files for %s", fname)
                with BytesIO() as out_stream:
                    if isinstance(data, dict):
                        out_stream.write(json.dumps(data).encode())
                    else:
                        out_stream.write(data.encode())
                    out_stream.seek(0)
                    finfo = tarfile.TarInfo(fname)
                    finfo.size = len(out_stream.getbuffer())
                    tar_file.addfile(finfo, out_stream)
        logger.info(
            "Uploading compressed file: %s into S3 bucket: %s", tarname, bucket
        )
        self.s3.put_object(Body=fileobj.getvalue(), Bucket=bucket, Key=tarname)
        fileobj.close()

    @lru_cache(maxsize=10)
    def get_week_year_map(self) -> dict[int, int]:
        """
        creates a dictionary that maps each week to a year
        """
        logger.info(
            "Scraping the census website to construct the year mapping"
        )
        r = requests.get(self.base_census_url, timeout=10)
        soup = BeautifulSoup(r.text, "html.parser")

        yearlinks = soup.find_all("a", {"href": re.compile(r"\d{4}/")})
        years: list[str] = [yearlink.get_text() for yearlink in yearlinks]

        weekyrmap = {}
        for year in years:
            yearint = int(re.sub(r"\D", "", year))
            r = requests.get("".join((self.base_census_url, year)), timeout=10)
            soup = BeautifulSoup(r.text, "html.parser")
            weeklinks = soup.find_all("a", {"href": re.compile(r"wk\d{1,2}/")})
            weeks: list[str] = [weeklink.get_text() for weeklink in weeklinks]
            for week in weeks:
                weekint = int(re.sub(r"\D", "", week))
                weekyrmap[weekint] = yearint

        return weekyrmap

    def download_from_s3(
        self,
    ) -> pd.DataFrame:
        """
        Downloads a pulse raw file from S3.

        Returns:
            pd.DataFrame: Raw response data with availaible weights merged
        """
        try:
            logger.info(
                "Downloading parquet file from S3 for week %s", self.week
            )
            s3obj = self.s3.get_object(
                Bucket="household-pulse",
                Key=f"raw-files/pulse-{self.week_str}.parquet",
            )
        except ClientError as e:
            logger.error(e)
            raise e

        df = pd.read_parquet(BytesIO(s3obj["Body"].read()))

        return df

    def download_from_census(self) -> pd.DataFrame:
        """
        Download Census Household Pulse PUF zip file for the given week and
        merge weights and PUF dataframes

        Returns:
            pd.DataFrame: the weeks census household pulse data merged with the
                weights csv
        """
        logger.info(
            "Downloading files from the census website for week %s", self.week
        )
        url = "".join((self.base_census_url, self._make_data_url()))
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

    def upload_to_s3(self, df: pd.DataFrame) -> None:
        """
        Uploads a dataframe that contains the raw responses from a single week
        with its available weights merged as a compressed parquet file into S3

        Args:
            df (pd.DataFrame): Dataframe to upload
            week (int): The pulse survey week. Used for the file key.
        """
        logger.info("Uploading parquet file to S3 for week %s", self.week)
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, compression="gzip")
        self.s3.put_object(
            Bucket="household-pulse",
            Key=f"raw-files/pulse-{self.week_str}.parquet",
            Body=buffer.getvalue(),
        )

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
                self.base_census_url,
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
