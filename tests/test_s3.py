# -*- coding: utf-8 -*-
"""
Created on 2023-01-25 03:08:15-06:00
===============================================================================
@filename:  test_s3.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Unit tests for the S3Storage class.
===============================================================================
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=protected-access

from datetime import datetime
from io import BytesIO
from typing import Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError
from botocore.response import StreamingBody

from household_pulse.io import Census, S3Storage


@pytest.fixture()
def s3storage() -> Generator[S3Storage, None, None]:
    with patch.object(S3Storage, "s3", MagicMock(name="s3-client")):
        dl = S3Storage()
        yield dl


@pytest.fixture
def mock_parquet() -> dict:
    with open("tests/testfiles/test.parquet", "rb") as file:
        filebytes = file.read()
        body_stream = StreamingBody(BytesIO(filebytes), len(filebytes))
        return {"Body": body_stream}


@pytest.fixture
def mock_df() -> Generator[pd.DataFrame, None, None]:
    df = pd.DataFrame(data={"a": [0, 1, 2, 3], "b": [4, 5, 6, 7]})
    yield df


@pytest.fixture
def mock_smoothed() -> Generator[pd.DataFrame, None, None]:
    df = pd.read_parquet("tests/testfiles/smoothed-test.parquet")
    yield df


@pytest.fixture
def mock_all() -> Generator[pd.DataFrame, None, None]:
    df = pd.read_parquet("tests/testfiles/all-test.parquet")
    yield df


def test_instance(s3storage: S3Storage) -> None:
    assert s3storage


@pytest.mark.parametrize(
    "files",
    ({"test": {"one": "two"}}, {"test": "{'test': {'one': 'two'}}"}),
)
def test_tar_and_upload_to_s3(files, s3storage: S3Storage) -> None:
    s3storage.tar_and_upload(tarname="test", files=files)
    s3storage.s3.put_object.assert_called_once()


def test_download_parquet(mock_parquet: BytesIO, s3storage: S3Storage) -> None:
    expected = pd.read_parquet("tests/testfiles/test.parquet")
    s3storage.s3.get_object.return_value = mock_parquet
    actual = s3storage.download_parquet(key="123")
    s3storage.s3.get_object.assert_called_once()

    assert expected.equals(actual)


def test_download_parquet_error(s3storage: S3Storage):
    mockerror = ClientError(
        error_response={"Error": {"Test": "123"}}, operation_name="test"
    )
    s3storage.s3.get_object = MagicMock(side_effect=mockerror)
    with pytest.raises(ClientError):
        s3storage.download_parquet(key="456")


@patch.object(pd.DataFrame, "to_parquet", MagicMock())
def test_upload_parquet(s3storage: S3Storage, mock_df: pd.DataFrame) -> None:
    s3storage.upload_parquet(key="123", df=mock_df)
    mock_df.to_parquet.assert_called_once()
    s3storage.s3.put_object.assert_called_once()


@patch.object(
    S3Storage, "get_available_weeks", MagicMock(return_value={1, 2, 3})
)
def test_download_all(s3storage: S3Storage, mock_df: pd.DataFrame) -> None:
    with patch.object(
        S3Storage, "download_parquet", MagicMock(return_value=mock_df)
    ):
        df = s3storage.download_all(file_type="raw")
        assert s3storage.download_parquet.call_count == 3
        assert df.equals(pd.concat([mock_df] * 3, ignore_index=True))


@patch.object(S3Storage, "download_all", MagicMock())
@patch.object(S3Storage, "download_parquet", MagicMock())
def test_download_smoothed_pulse(
    s3storage: S3Storage, mock_smoothed: pd.DataFrame, mock_all: pd.DataFrame
) -> None:
    s3storage.download_all.return_value = mock_all
    s3storage.download_parquet.return_value = mock_smoothed

    df = s3storage.download_smoothed_pulse()
    s3storage.download_all.assert_called_with(file_type="processed")
    s3storage.download_parquet.assert_called_with(
        key="smoothed/pulse-smoothed.parquet"
    )
    assert len(df) == 1


def test_get_available_weeks(s3storage: S3Storage) -> None:
    mock_paginator = MagicMock(name="mock-paginator")
    s3storage.s3.get_paginator.return_value = mock_paginator
    mock_paginator.paginate.return_value = [
        {
            "Contents": [
                {"Key": "i am not a parquet file"},
                {"Key": "pulse.parquet"},
                {"Key": "pulse-01.parquet"},
                {"Key": "pulse-02.parquet"},
            ]
        }
    ]
    result = s3storage.get_available_weeks(file_type="raw")
    assert result == {1, 2}


def test_get_available_weeks_error(s3storage: S3Storage) -> None:
    mockerror = ClientError(
        error_response={"Error": {"Test": "123"}}, operation_name="test"
    )
    mock_paginator = MagicMock(name="mock-paginator", side_effect=mockerror)
    s3storage.s3.get_paginator = mock_paginator
    with pytest.raises(ClientError):
        s3storage.get_available_weeks(file_type="processed")


def test_get_collection_dates(s3storage: S3Storage):
    s3storage.s3.get_object.return_value = {
        "Body": BytesIO(
            (
                b'{"1": {"pub_date": "2020-05-02", "start_date": '
                b'"2020-05-02", "end_date": "2020-05-02"}}'
            )
        )
    }
    mock_date = datetime.strptime("2020-05-02", "%Y-%m-%d").date()
    result = s3storage.get_collection_dates()
    assert result == {
        1: {
            "pub_date": mock_date,
            "start_date": mock_date,
            "end_date": mock_date,
        }
    }


def test_get_collection_dates_error(s3storage: S3Storage):
    mockerror = ClientError(
        error_response={"Error": {"Test": "123"}}, operation_name="test"
    )
    s3storage.s3.get_object = MagicMock(side_effect=mockerror)
    with pytest.raises(ClientError):
        s3storage.get_collection_dates.cache_clear()
        s3storage.get_collection_dates()


@patch.object(Census, "load_collection_dates", MagicMock(name="mock-collect"))
def test_put_collection_dates(s3storage: S3Storage):
    s3storage.s3.put_object = MagicMock(name="put-object")
    s3storage.put_collection_dates()
    s3storage.s3.put_object.assert_called_once()


def test_upload(s3storage: S3Storage):
    s3storage.s3.put_object = MagicMock(name="put-object")
    buffer = BytesIO()
    other_buffer = BytesIO()
    s3storage._upload(key="test", buffer=buffer)
    s3storage.s3.put_object.assert_called_once_with(
        Bucket="household-pulse", Key="test", Body=other_buffer.getvalue()
    )
    other_buffer.close()


def test_check_file_type(s3storage: S3Storage):
    with pytest.raises(ValueError):
        s3storage._check_file_type(file_type="test")
