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
import re
from io import BytesIO
from zipfile import ZipFile

import boto3
import pandas as pd
import requests
from botocore.exceptions import ClientError
from bs4 import BeautifulSoup
from pkg_resources import resource_filename


class DataLoader:
    """
    This class handles the (down) loading of raw pulse data from either the
    census or our backups from S3
    """

    def __init__(self) -> None:
        self.s3 = boto3.client('s3', **self._load_s3_creds())
        self.base_census_url = (
            'https://www2.census.gov/programs-surveys/demo/datasets/hhp/')
        self._build_week_year_map()

    @property
    def weekyrmap(self):
        return self.__weekyrmap

    def load_week(self, week: int) -> pd.DataFrame:
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
            df = self._download_from_s3(week=week)
        except ClientError as e:
            if e.response['Error']['Code'] == 'NoSuchKey':
                df = self._download_from_census(week=week)
                self._upload_to_s3(df=df, week=week)
            else:
                raise ClientError(e)

        return df

    def _download_from_s3(self, week: int) -> pd.DataFrame:
        """
        Downloads a pulse raw file from S3.

        Args:
            week (int): The survey week index

        Returns:
            pd.DataFrame: Raw response data with availaible weights merged
        """
        s3obj = self.s3.get_object(
            Bucket='household-pulse',
            Key=f'raw-files/pulse-{week}.parquet.gzip')

        df = pd.read_parquet(BytesIO(s3obj['Body'].read()))

        return df

    def _download_from_census(self, week: int) -> pd.DataFrame:
        """
        Download Census Household Pulse PUF zip file for the given week and
        merge weights and PUF dataframes

        Args:
            week (int): The week of data to download

        Returns:
            pd.DataFrame: the weeks census household pulse data merged with the
                weights csv
        """

        url = ''.join((self.base_census_url, self._make_data_url(week)))
        r = requests.get(url)
        read_zip = ZipFile(BytesIO(r.content))

        data_df: pd.DataFrame = pd.read_csv(
            read_zip.open(self._make_data_fname(week, 'd')),
            dtype={'SCRAM': 'string'})
        weight_df: pd.DataFrame = pd.read_csv(
            read_zip.open(self._make_data_fname(week, 'w')),
            dtype={'SCRAM': 'string'})

        if week < 13:
            hweight_url = ''.join((
                self.base_census_url,
                self._make_data_url(week=week, hweights=True)))
            hwgdf = pd.read_csv(hweight_url)
            weight_df = weight_df.merge(
                hwgdf,
                how='inner',
                on=['SCRAM', 'WEEK'])

        df = data_df.merge(weight_df, how='left', on=['SCRAM', 'WEEK'])
        df = df.copy()

        return df

    def _upload_to_s3(self, df: pd.DataFrame, week: int) -> None:
        """
        Uploads a dataframe that contains the raw responses from a single week
        with its available weights merged as a compressed parquet file into S3

        Args:
            df (pd.DataFrame): Dataframe to upload
            week (int): The pulse survey week. Used for the file key.
        """
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, compression='gzip')
        self.s3.put_object(
            Bucket='household-pulse',
            Key=f'raw-files/pulse-{week}.parquet.gzip',
            Body=buffer.getvalue()
        )

    def _build_week_year_map(self) -> None:
        """
        creates a dictionary that maps each week to a year
        """
        r = requests.get(self.base_census_url)
        soup = BeautifulSoup(r.text, 'html.parser')

        yearlinks = soup.find_all('a', {'href': re.compile(r'\d{4}/')})
        years: list[str] = [yearlink.get_text() for yearlink in yearlinks]

        weekyrmap = {}
        for year in years:
            yearint = int(re.sub(r'\D', '', year))
            r = requests.get(''.join((self.base_census_url, year)))
            soup = BeautifulSoup(r.text, 'html.parser')
            weeklinks = soup.find_all('a', {'href': re.compile(r'wk\d{1,2}/')})
            weeks: list[str] = [weeklink.get_text() for weeklink in weeklinks]
            for week in weeks:
                weekint = int(re.sub(r'\D', '', week))
                weekyrmap[weekint] = yearint

        self.__weekyrmap = weekyrmap

    def _make_data_url(self, week: int, hweights: bool = False) -> str:
        """
        Helper function to get string for file to download from census api

        Args:
            week (int): the week of data to download
            hweights (bool): make url for household weights that for weeks < 13
                are in a separate file in the census' ftp.

        Returns:
            str: the year/week/file.zip to be downloaded
        """
        if hweights and week > 12:
            raise ValueError('hweights can only be passed for weeks 1-12')

        year = self.weekyrmap[week]
        weekstr: str = str(week).zfill(2)
        if hweights:
            return f'{year}/wk{week}/pulse{year}_puf_hhwgt_{weekstr}.csv'
        else:
            return f"{year}/wk{week}/HPS_Week{weekstr}_PUF_CSV.zip"

    def _make_data_fname(self, week: int, fname: str) -> str:
        """
        Helper function to get the string names of the files downloaded

        Args:
            week (int): the week of data to download
            fname (str): the file to dowload (d: main data file, w: weights
                file)

        Returns:
            str: name of file downloaded
        """
        if fname not in {'d', 'w'}:
            raise ValueError("fname muts be in {'d', 'w'}")

        year = self.weekyrmap[week]
        weekstr: str = str(week).zfill(2)
        if fname == 'd':
            return f"pulse{year}_puf_{weekstr}.csv"
        else:
            return f"pulse{year}_repwgt_puf_{weekstr}.csv"

    @staticmethod
    def _load_s3_creds() -> dict[str, str]:
        """
        Loads the S3 credentials that have read/write permissions for the
        project bucket only.

        Returns:
            dict[str, str]: IAM credentials.
        """
        fname = resource_filename('household_pulse', 's3.json')
        with open(fname, 'r') as file:
            return json.loads(file.read())

    @staticmethod
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
        baseurl = 'https://docs.google.com/spreadsheets/d'
        ssid = '1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo'

        sheetids = {
            'question_mapping': '34639438',
            'response_mapping': '1561671071',
            'county_metro_state': '974836931',
            'numeric_mapping': '1572193173'
        }

        if sheetname not in sheetids:
            raise ValueError(f'{sheetname} not in {sheetids.keys()}')

        df = pd.read_csv(
            f'{baseurl}/{ssid}/export?format=csv&gid={sheetids[sheetname]}'
        )
        df = df.dropna(how='all')

        return df
