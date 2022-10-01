# -*- coding: utf-8 -*-
"""
Created on Tuesday, 27th September 2022 5:13:20 pm
===============================================================================
@filename:  test_downloader.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Unit tests for the downloader.py module.
===============================================================================
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=protected-access

from io import BytesIO
from typing import Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody
from household_pulse.downloader import DataLoader


@pytest.fixture
def dataloader() -> Generator[DataLoader, None, None]:
    with patch("household_pulse.downloader.boto3.client"):
        with patch.object(
            DataLoader, "_load_s3_creds", MagicMock(return_value={})
        ):
            dl = DataLoader()
            yield dl


@pytest.fixture
def mock_parquet() -> dict:
    with open("tests/test.parquet.gzip", "rb") as file:
        filebytes = file.read()
        body_stream = StreamingBody(BytesIO(filebytes), len(filebytes))
        return {"Body": body_stream}


@pytest.fixture
def mock_zip_10() -> bytes:
    with open("tests/HPS_Week10_PUF_CSV.zip", "rb") as file:
        filebytes = file.read()
        return filebytes

@pytest.fixture
def mock_zip_40() -> bytes:
    with open("tests/HPS_Week40_PUF_CSV.zip", "rb") as file:
        filebytes = file.read()
        return filebytes

class TestInstantiation:
    """
    Tests the functionality called within the classes __init__ method.
    """

    @staticmethod
    def test_instantiation(dataloader: DataLoader) -> None:
        assert dataloader


class TestMethods:
    """
    Unit tests for the class methods
    """

    @staticmethod
    def test_weekyrmap(dataloader: DataLoader) -> None:
        weekyrmap = dataloader._get_week_year_map()
        assert isinstance(weekyrmap, dict)
        assert len(weekyrmap) > 0
        assert all(isinstance(week, int) for week in weekyrmap.keys())
        assert all(isinstance(year, int) for year in weekyrmap.values())

    @staticmethod
    @patch.object(DataLoader, "_download_from_s3", MagicMock())
    def test_load_week_in_s3(dataloader: DataLoader) -> None:
        dataloader.load_week(week=40)
        dataloader._download_from_s3.assert_called_once()
        dataloader._download_from_s3.assert_called_with(week=40)

    @staticmethod
    @patch.object(
        DataLoader,
        "_download_from_s3",
        MagicMock(
            side_effect=ClientError(
                error_response={"Error": {"Code": "NoSuchKey"}},
                operation_name="Test",
            )
        ),
    )
    @patch.object(DataLoader, "_download_from_census", MagicMock())
    @patch.object(DataLoader, "_upload_to_s3", MagicMock())
    def test_load_week_not_in_s3(dataloader: DataLoader) -> None:
        dataloader.load_week(week=40)
        dataloader._download_from_s3.assert_called_with(week=40)
        dataloader._download_from_census.assert_called_with(week=40)
        dataloader._upload_to_s3.assert_called_with(
            df=dataloader._download_from_census(week=40), week=40
        )

    @staticmethod
    @patch.object(
        DataLoader,
        "_download_from_s3",
        MagicMock(
            side_effect=ClientError(
                error_response={"Error": {"Code": "Test"}},
                operation_name="Test",
            )
        ),
    )
    def test_load_week_client_error(dataloader: DataLoader) -> None:
        with pytest.raises(ClientError):
            dataloader.load_week(week=40)

    @staticmethod
    @pytest.mark.parametrize(
        "files",
        ({"test": {"one": "two"}}, {"test": "{'test': {'one': 'two'}}"}),
    )
    def test_tar_and_upload_to_s3(files, dataloader: DataLoader) -> None:
        dataloader.tar_and_upload_to_s3(
            bucket="test", tarname="test", files=files
        )
        dataloader.s3.put_object.assert_called_once()

    @staticmethod
    def test_download_from_s3(
        mock_parquet: BytesIO, dataloader: DataLoader
    ) -> None:
        expected = pd.read_parquet("tests/test.parquet.gzip")
        dataloader.s3.get_object.return_value = mock_parquet
        actual = dataloader._download_from_s3(week=40)
        dataloader.s3.get_object.assert_called_once()

        assert expected.equals(actual)

    @staticmethod
    @patch.object(
        DataLoader,
        "_get_week_year_map",
        MagicMock(return_value={10: 2020}),
    )
    def test_download_from_census_early(
        mock_zip_10: bytes, dataloader: DataLoader
    ) -> None:
        with patch("household_pulse.downloader.requests") as mock_requests:
            mock_get = MagicMock()
            mock_requests.get.return_value = mock_get
            mock_get.content = mock_zip_10
            df = dataloader._download_from_census(week=10)
            assert len(df) == 4

    @staticmethod
    @patch.object(
        DataLoader,
        "_get_week_year_map",
        MagicMock(return_value={40: 2021}),
    )
    def test_download_from_census_late(
        mock_zip_40: bytes, dataloader: DataLoader
    ) -> None:
        with patch("household_pulse.downloader.requests") as mock_requests:
            mock_get = MagicMock()
            mock_requests.get.return_value = mock_get
            mock_get.content = mock_zip_40
            df = dataloader._download_from_census(week=40)
            assert len(df) == 4
