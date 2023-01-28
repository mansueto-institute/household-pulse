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

logger = logging.getLogger(__name__)


@dataclass(unsafe_hash=True)
class S3Storage:
    """
    This class handles all s3 interactions.
    """

    allowed_ftypes: ClassVar[set[str]] = {"raw", "processed"}
    bucket: ClassVar[str] = "household-pulse"
    s3: ClassVar[boto3.client] = boto3.client("s3")

    @lru_cache(maxsize=5)
    def download_parquet(self, key: str) -> pd.DataFrame:
        """
        Download a parquet file from S3 and return it as a pandas dataframe.

        Args:
            key (str): The object key of the parquet file in S3.

        Raises:
            ClientError: If the file does not exist in S3.

        Returns:
            pd.DataFrame: The parquet file as a pandas dataframe.
        """
        try:
            logger.info("Downloading parquet file from S3: %s", key)
            s3obj = self.s3.get_object(Bucket=self.bucket, Key=key)
        except ClientError as e:
            logger.error(e)
            raise e
        df = pd.read_parquet(BytesIO(s3obj["Body"].read()))

        return df

    def upload_parquet(self, key: str, df: pd.DataFrame) -> None:
        """
        Upload a parquet file to S3.

        Args:
            key (str): The object key of the parquet file in S3.
            df (pd.DataFrame): The dataframe to be uploaded.
        """
        buffer = BytesIO()
        df.to_parquet(buffer, index=False, compression="gzip")
        self._upload(key=key, buffer=buffer)

    def tar_and_upload(self, tarname: str, files: dict[str, dict]) -> None:
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
        self._upload(key=tarname, buffer=fileobj)

    @lru_cache(maxsize=5)
    def download_all(self, file_type: str) -> pd.DataFrame:
        """
        Downloads all parquet files from S3 and returns a dataframe.

        Args:
            file_type (str): {"raw", "processed"}, whether to download the raw
                pulse data or the processed data.

        Returns:
            pd.DataFrame: Concatenated dataframe.
        """
        self._check_file_type(file_type)
        weeks = self.get_available_weeks(file_type=file_type)
        results = []
        for week in weeks:
            results.append(
                self.download_parquet(
                    key=f"{file_type}-files/pulse-{str(week).zfill(2)}.parquet"
                )
            )
        df = pd.concat(results, ignore_index=True)
        return df

    def download_smoothed_pulse(self) -> pd.DataFrame:
        """
        Downloads the smoothed pulse data from S3 and returns a dataframe.

        Returns:
            pd.DataFrame: Smoothed pulse dataframe.
        """
        pulsedf = self.download_all(file_type="processed")
        smoothdf = self.download_parquet(key="smoothed/pulse-smoothed.parquet")
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

    def get_available_weeks(self, file_type: str) -> set[int]:
        """
        Gets a list of available weeks for the specified file type.

        Args:
            file_type (str): {"raw", "processed"}

        Returns:
            set[int]: List of available weeks.
        """
        self._check_file_type(file_type)

        try:
            logger.info(
                "Getting available weeks for file type %s from S3", file_type
            )

            weeks: set[int] = set()
            paginator = self.s3.get_paginator("list_objects_v2")
            response_iterator = paginator.paginate(
                Bucket=self.bucket, Prefix=f"{file_type}-files/"
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

    @lru_cache(maxsize=5)
    def get_collection_dates(self) -> dict[int, dict[str, date]]:
        """
        Gets the collection dates for each week.

        Returns:
            dict[int, str]: Dictionary of week number to collection date.
        """
        try:
            logger.info("Getting collection dates from S3")
            results: dict[int, dict[str, date]] = {}
            s3obj = self.s3.get_object(
                Bucket=self.bucket, Key="collection-dates.json"
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

    def put_collection_dates(self) -> None:
        """
        Retrieves the collection dates from the Census API and uploads them to
        S3.
        """
        data = Census.load_collection_dates()
        buffer = BytesIO()
        buffer.write(json.dumps(data, default=str).encode())
        self._upload(key="collection-dates.json", buffer=buffer)

    def _upload(self, key: str, buffer: BytesIO) -> None:
        """
        Uploads a file to S3 using the provided bucket and key.

        Args:
            key (str): The key to use for the uploaded file.
            buffer (BytesIO): The data to upload as a bytes buffer.
        """
        logger.info("Uploading object %s to S3", key)
        self.s3.put_object(Bucket=self.bucket, Key=key, Body=buffer.getvalue())
        buffer.close()

    def _check_file_type(self, file_type: str) -> None:
        if file_type not in self.allowed_ftypes:
            raise ValueError("file_type must be either 'raw' or 'processed'.")
