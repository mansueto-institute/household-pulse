# -*- coding: utf-8 -*-
"""
Created on 2023-01-20 05:11:36-06:00
===============================================================================
@filename:  s3.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   module to handle all s3 interactions
===============================================================================
"""
import json
import logging
import re
import tarfile
from dataclasses import dataclass
from datetime import date, datetime
from functools import lru_cache
from io import BytesIO
from typing import ClassVar

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from household_pulse.io import Census
from household_pulse.io.base import IO

logger = logging.getLogger(__name__)


@dataclass(unsafe_hash=True)
class S3Storage(IO):
    """
    This class handles all s3 interactions.
    """

    bucket: ClassVar[str] = "household-pulse"
    allowed_ftypes: ClassVar[set[str]] = {"raw", "processed"}
    s3: ClassVar[boto3.client] = boto3.client("s3")

    @lru_cache(maxsize=5)
    def download_dataframe(self, file_type: str) -> pd.DataFrame:
        """
        Downloads a parquet file from S3 and returns it as a dataframe.

        Args:
            file_type (str): {"raw", "processed"}, whether to download the raw
                pulse data or the processed data.

        Raises:
            ClientError: if the file does not exist in S3.

        Returns:
            pd.DataFrame: The dataframe.
        """
        self._check_file_type(file_type)
        df = self.download_parquet(
            key=f"{file_type}-files/pulse-{self.week_str}.parquet",
        )
        return df

    def upload_dataframe(self, file_type: str, df: pd.DataFrame) -> None:
        """
        Uploads a dataframe to S3 as a compressed parquet file.

        Args:
            file_type (str): {"raw", "processed"}
            df (pd.DataFrame): Dataframe to upload
        """
        self._check_file_type(file_type)
        self.upload_parquet(
            key=f"{file_type}-files/pulse-{self.week_str}.parquet", df=df
        )

    @staticmethod
    def tar_and_upload(tarname: str, files: dict[str, dict]) -> None:
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
        S3Storage._upload(key=tarname, buffer=fileobj)

    @staticmethod
    @lru_cache(maxsize=5)
    def download_all(file_type: str) -> pd.DataFrame:
        """
        Downloads all parquet files from S3 and returns a dataframe.

        Args:
            file_type (str): {"raw", "processed"}, whether to download the raw
                pulse data or the processed data.

        Returns:
            pd.DataFrame: Concatenated dataframe.
        """
        S3Storage._check_file_type(file_type)
        weeks = S3Storage.get_available_weeks(file_type)
        results = []
        for week in weeks:
            results.append(
                S3Storage(week=week).download_dataframe(file_type=file_type)
            )
        df = pd.concat(results, ignore_index=True)
        return df

    @staticmethod
    def download_smoothed_pulse() -> pd.DataFrame:
        """
        Downloads the smoothed pulse data from S3 and returns a dataframe.

        Returns:
            pd.DataFrame: Smoothed pulse dataframe.
        """
        pulsedf = S3Storage.download_all(file_type="processed")
        smoothdf = S3Storage.download_parquet(
            key="smoothed/pulse-smoothed.parquet"
        )
        keepcols = [
            "week",
            "xtab_var",
            "xtab_val",
            "q_var",
            "q_val",
            "pweight_share",
            "pweight_share_smoothed",
        ]

        df = pulsedf.merge(
            smoothdf,
            how="inner",
            on=["week", "xtab_var", "xtab_val", "q_var", "q_val"],
        )
        df = df[keepcols]
        return df

    @staticmethod
    def get_available_weeks(file_type: str) -> set[int]:
        """
        Gets a list of available weeks for the specified file type.

        Args:
            file_type (str): {"raw", "processed"}

        Returns:
            set[int]: List of available weeks.
        """
        S3Storage._check_file_type(file_type)

        try:
            logger.info(
                "Getting available weeks for file type %s from S3", file_type
            )

            weeks: set[int] = set()
            paginator = S3Storage.s3.get_paginator("list_objects_v2")
            response_iterator = paginator.paginate(
                Bucket=S3Storage.bucket, Prefix=f"{file_type}-files/"
            )

            pat = re.compile(r"pulse-(\d{2}).parquet")
            for response in response_iterator:
                for obj in response["Contents"]:
                    if obj["Key"].endswith(".parquet"):
                        search = pat.search(obj["Key"])
                        if search:
                            weeks.add(int(search.group(1)))

        except ClientError as e:
            logger.error(e)
            raise e

        return weeks

    @staticmethod
    @lru_cache(maxsize=5)
    def get_collection_dates() -> dict[int, dict[str, date]]:
        """
        Gets the collection dates for each week.

        Returns:
            dict[int, str]: Dictionary of week number to collection date.
        """
        try:
            logger.info("Getting collection dates from S3")
            results: dict[int, dict[str, date]] = {}
            s3obj = S3Storage.s3.get_object(
                Bucket=S3Storage.bucket, Key="collection-dates.json"
            )
            data = json.loads(s3obj["Body"].read())

            for week, dates in data.items():
                intweek = int(week)
                results[intweek] = {}
                for date_type, date_val in dates.items():
                    results[intweek][date_type] = datetime.strptime(
                        date_val, "%Y-%m-%d"
                    ).date()
            return results

        except ClientError as e:
            logger.error(e)
            raise e

    @staticmethod
    def put_collection_dates() -> None:
        """
        Retrieves the collection dates from the Census API and uploads them to
        S3.
        """
        data = Census.load_collection_dates()
        buffer = BytesIO()
        buffer.write(json.dumps(data, default=str).encode())
        S3Storage._upload(key="collection-dates.json", buffer=buffer)

    @staticmethod
    @lru_cache(maxsize=5)
    def download_parquet(key: str) -> pd.DataFrame:
        try:
            logger.info("Downloading parquet file from S3: %s", key)
            s3obj = S3Storage.s3.get_object(Bucket=S3Storage.bucket, Key=key)
        except ClientError as e:
            logger.error(e)
            raise e
        df = pd.read_parquet(BytesIO(s3obj["Body"].read()))

        return df

    @staticmethod
    def upload_parquet(key: str, df: pd.DataFrame) -> None:
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, compression="gzip")
        S3Storage._upload(key=key, buffer=buffer)

    @staticmethod
    def _upload(key: str, buffer: BytesIO) -> None:
        """
        Uploads a file to S3 using the provided bucket and key.

        Args:
            key (str): The key to use for the uploaded file.
            buffer (BytesIO): The data to upload as a bytes buffer.
        """
        logger.info("Uploading object %s to S3", key)
        S3Storage.s3.put_object(
            Bucket=S3Storage.bucket, Key=key, Body=buffer.getvalue()
        )
        buffer.close()

    @staticmethod
    def _check_file_type(file_type: str) -> None:
        if file_type not in S3Storage.allowed_ftypes:
            raise ValueError("file_type must be either 'raw' or 'processed'.")
