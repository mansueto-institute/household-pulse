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
import tarfile
from dataclasses import dataclass
from io import BytesIO

import boto3
import pandas as pd
from botocore.exceptions import ClientError

from household_pulse.io.base import IO

logger = logging.getLogger(__name__)


@dataclass
class S3Storage(IO):
    """
    This class handles all s3 interactions.
    """

    def __post_init__(self) -> None:
        self.s3 = boto3.client("s3")

    def download(self) -> pd.DataFrame:
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

    def upload(self, df: pd.DataFrame) -> None:
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

    def tar_and_upload(
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
