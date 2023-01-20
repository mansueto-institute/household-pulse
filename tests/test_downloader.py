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


@pytest.fixture()
def dataloader() -> Generator[DataLoader, None, None]:
    with patch("boto3.client"):
        dl = DataLoader(week=1)
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


@pytest.fixture
def mock_data_site() -> str:
    with open("tests/test_page.txt", "r", encoding="utf-8") as file:
        filestr = file.read()
        return filestr


@pytest.fixture
def mock_df() -> Generator[pd.DataFrame, None, None]:
    df = pd.DataFrame(data={"a": [0, 1, 2, 3], "b": [4, 5, 6, 7]})
    yield df


class TestInstantiation:
    """
    Tests the functionality called within the classes __init__ method.
    """

    @staticmethod
    def test_instantiation(dataloader: DataLoader) -> None:
        assert dataloader

    @staticmethod
    def test_bad_instance() -> None:
        with pytest.raises(TypeError):
            DataLoader()

        with pytest.raises(TypeError):
            DataLoader(week="1")


class TestMethods:
    """
    Unit tests for the class methods
    """

    @staticmethod
    def test_weekyrmap(dataloader: DataLoader) -> None:
        weekyrmap = dataloader.get_week_year_map()
        assert isinstance(weekyrmap, dict)
        assert len(weekyrmap) > 0
        assert all(isinstance(week, int) for week in weekyrmap.keys())
        assert all(isinstance(year, int) for year in weekyrmap.values())

    @staticmethod
    @patch.object(DataLoader, "download_from_s3", MagicMock())
    def test_load_week_in_s3(dataloader: DataLoader) -> None:
        dataloader.load_week()
        dataloader.download_from_s3.assert_called_once()
        dataloader.download_from_s3.assert_called_with()

    @staticmethod
    @patch.object(
        DataLoader,
        "download_from_s3",
        MagicMock(
            side_effect=ClientError(
                error_response={"Error": {"Code": "NoSuchKey"}},
                operation_name="Test",
            )
        ),
    )
    @patch.object(DataLoader, "download_from_census", MagicMock())
    @patch.object(DataLoader, "upload_to_s3", MagicMock())
    def test_load_week_not_in_s3(dataloader: DataLoader) -> None:
        dataloader.load_week()
        dataloader.download_from_s3.assert_called_with()
        dataloader.download_from_census.assert_called_with()
        dataloader.upload_to_s3.assert_called_with(
            df=dataloader.download_from_census(),
        )

    @staticmethod
    @patch.object(
        DataLoader,
        "download_from_s3",
        MagicMock(
            side_effect=ClientError(
                error_response={"Error": {"Code": "Test"}},
                operation_name="Test",
            )
        ),
    )
    def test_load_week_client_error(dataloader: DataLoader) -> None:
        with pytest.raises(ClientError):
            dataloader.load_week()

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
        actual = dataloader.download_from_s3()
        dataloader.s3.get_object.assert_called_once()

        assert expected.equals(actual)

    @staticmethod
    def test_download_from_s3_error(dataloader: DataLoader):
        mockerror = ClientError(
            error_response={"Error": {"Test": "123"}}, operation_name="test"
        )
        dataloader.s3.get_object = MagicMock(side_effect=mockerror)
        with pytest.raises(ClientError):
            dataloader.download_from_s3()

    @staticmethod
    @patch.object(
        DataLoader,
        "get_week_year_map",
        MagicMock(return_value={10: 2020}),
    )
    @patch.object(
        DataLoader,
        "_download_hh_weights",
        MagicMock(
            return_value=pd.read_csv("tests/pulse2020_puf_hhwgt_10.csv")
        ),
    )
    def test_download_from_census_early(mock_zip_10: bytes) -> None:
        with patch("household_pulse.downloader.requests") as mock_requests:
            dataloader = DataLoader(week=10)
            mock_get = MagicMock()
            mock_requests.get.return_value = mock_get
            mock_get.content = mock_zip_10
            df = dataloader.download_from_census()
            assert len(df) == 4

    @staticmethod
    @patch.object(
        DataLoader,
        "get_week_year_map",
        MagicMock(return_value={40: 2021}),
    )
    def test_download_from_census_late(mock_zip_40: bytes) -> None:
        with patch("household_pulse.downloader.requests") as mock_requests:
            dataloader = DataLoader(week=40)
            mock_get = MagicMock()
            mock_requests.get.return_value = mock_get
            mock_get.content = mock_zip_40
            df = dataloader.download_from_census()
            assert len(df) == 4

    @staticmethod
    @patch.object(DataLoader, "_make_data_url", MagicMock(return_value=""))
    @pytest.mark.parametrize("week", (10, 13))
    def test_download_hh_weights(week: int) -> None:
        dataloader = DataLoader(week=week)
        dataloader.base_census_url = "tests/pulse2020_puf_hhwgt_10.csv"
        if week == 13:
            with pytest.raises(ValueError):
                hhwdf = dataloader._download_hh_weights()
        else:
            hhwdf = dataloader._download_hh_weights()
            assert hhwdf.equals(
                pd.read_csv("tests/pulse2020_puf_hhwgt_10.csv")
            )

    @staticmethod
    @patch.object(pd.DataFrame, "to_parquet", MagicMock())
    def test_upload_to_s3(
        dataloader: DataLoader, mock_df: pd.DataFrame
    ) -> None:
        dataloader.upload_to_s3(
            df=mock_df,
        )
        mock_df.to_parquet.assert_called_once()
        dataloader.s3.put_object.assert_called_once()

    @staticmethod
    @patch.object(
        DataLoader,
        "get_week_year_map",
        MagicMock(return_value={10: 2020, 13: 2020}),
    )
    @pytest.mark.parametrize(
        "week,hweights", ((13, True), (13, False), (10, True), (10, False))
    )
    def test_make_data_url(week: int, hweights: bool) -> None:
        dataloader = DataLoader(week=week)
        if week == 13 and hweights:
            with pytest.raises(ValueError):
                dataloader._make_data_url(hweights=hweights)
        else:
            if hweights:
                expected = f"2020/wk{week}/pulse2020_puf_hhwgt_{week}.csv"
            else:
                expected = f"2020/wk{week}/HPS_Week{week}_PUF_CSV.zip"
            assert expected == dataloader._make_data_url(hweights)

    @staticmethod
    @patch.object(
        DataLoader,
        "get_week_year_map",
        MagicMock(return_value={10: 2020, 13: 2020, 52: 2023}),
    )
    @pytest.mark.parametrize(
        "week,fname", ((13, "j"), (13, "d"), (10, "w"), (10, "d"), (52, "d"))
    )
    def test_make_data_fname(week: int, fname: str) -> None:
        dataloader = DataLoader(week=week)
        if fname not in {"d", "w"}:
            with pytest.raises(ValueError):
                dataloader._make_data_fname(fname=fname)
        # this tests for an ad-hoc issue on the census website
        elif week == 52:
            expected = f"pulse2022_puf_{week}.csv"
            assert expected == dataloader._make_data_fname(fname=fname)
        else:
            if fname == "d":
                expected = f"pulse2020_puf_{week}.csv"
            else:
                expected = f"pulse2020_repwgt_puf_{week}.csv"
            assert expected == dataloader._make_data_fname(fname=fname)

    @staticmethod
    @patch.object(pd, "read_csv", MagicMock(return_value=MagicMock()))
    @pytest.mark.parametrize("sheetname", ("badname", "question_mapping"))
    def test_load_gsheet(sheetname: str, dataloader: DataLoader) -> None:
        if sheetname == "badname":
            with pytest.raises(ValueError):
                dataloader.load_gsheet(sheetname=sheetname)
        else:
            dataloader.load_gsheet(sheetname=sheetname)
            pd.read_csv.assert_called_once()

    @staticmethod
    def test_load_collection_dates(
        dataloader: DataLoader, mock_data_site: str
    ) -> None:
        with patch("household_pulse.downloader.requests") as mock_requests:
            mock_get = MagicMock()
            mock_requests.get.return_value = mock_get
            mock_get.content = mock_data_site.encode()
            dates = dataloader.load_collection_dates()
            assert len(dates) > 0
