# -*- coding: utf-8 -*-
"""
Created on Saturday, 15th October 2022 11:34:42 am
===============================================================================
@filename:  test_pulse.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Unit tests for the pulse.py module.
===============================================================================
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=protected-access


import datetime
from typing import Generator
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest
from botocore.exceptions import ClientError

from household_pulse.pulse import Pulse


@pytest.fixture(scope="session")
def cmsdf() -> pd.DataFrame:
    return pd.read_csv("tests/testfiles/county_metro_state.csv")


@pytest.fixture(scope="session")
def qumdf() -> pd.DataFrame:
    return pd.read_csv("tests/testfiles/question_mapping.csv")


@pytest.fixture(scope="session")
def resdf() -> pd.DataFrame:
    return pd.read_csv("tests/testfiles/response_mapping.csv")


@pytest.fixture(scope="session")
def mapdf() -> pd.DataFrame:
    return pd.read_csv("tests/testfiles/numeric_mapping.csv")


@pytest.fixture
def mock_df() -> Generator[pd.DataFrame, None, None]:
    df = pd.read_csv("tests/testfiles/pulse.csv")
    yield df


@pytest.fixture
def pulse(cmsdf, qumdf, resdf, mapdf) -> Generator[Pulse, None, None]:
    def mock_gsheet(sheetname: str) -> pd.DataFrame:
        repository = {
            "question_mapping": qumdf,
            "response_mapping": resdf,
            "county_metro_state": cmsdf,
            "numeric_mapping": mapdf,
        }
        return repository[sheetname]

    with patch("household_pulse.pulse.S3Storage"):
        with patch(
            "household_pulse.pulse.load_gsheet",
            MagicMock(side_effect=mock_gsheet),
        ):
            with patch("household_pulse.pulse.Census"):
                pulse = Pulse(week=10)
                yield pulse


class TestInstantiation:
    """
    Tests the functionality called within the class' __init__ mehthod.
    """

    @staticmethod
    def test_instantiation(pulse: Pulse) -> None:
        assert pulse
        assert pulse.week == 10


class TestMethods:
    """
    Tests the methods of the pulse class.
    """

    @staticmethod
    @patch.object(Pulse, "download_data", MagicMock())
    @patch.object(Pulse, "_coalesce_variables", MagicMock())
    @patch.object(Pulse, "_parse_question_cols", MagicMock())
    @patch.object(Pulse, "_calculate_ages", MagicMock())
    @patch.object(Pulse, "_bucketize_numeric_cols", MagicMock())
    @patch.object(Pulse, "_coalesce_races", MagicMock())
    @patch.object(Pulse, "_reshape_long", MagicMock())
    @patch.object(Pulse, "_drop_missing_responses", MagicMock())
    @patch.object(Pulse, "_recode_values", MagicMock())
    @patch.object(Pulse, "_aggregate", MagicMock())
    @patch.object(Pulse, "_merge_cbsa_info", MagicMock())
    @patch.object(Pulse, "_reorganize_cols", MagicMock())
    def test_process_data(pulse: Pulse) -> None:
        pulse.process_data()  # type: ignore
        pulse.download_data.assert_called_once()  # type: ignore
        pulse._coalesce_variables.assert_called_once()  # type: ignore
        pulse._parse_question_cols.assert_called_once()  # type: ignore
        pulse._calculate_ages.assert_called_once()  # type: ignore
        pulse._bucketize_numeric_cols.assert_called_once()  # type: ignore
        pulse._coalesce_races.assert_called_once()  # type: ignore
        pulse._reshape_long.assert_called_once()  # type: ignore
        pulse._drop_missing_responses.assert_called_once()  # type: ignore
        pulse._recode_values.assert_called_once()  # type: ignore
        pulse._aggregate.assert_called_once()  # type: ignore
        pulse._merge_cbsa_info.assert_called_once()  # type: ignore
        pulse._reorganize_cols.assert_called_once()  # type: ignore

    @staticmethod
    def test_upload_data_no_ctabdf(pulse: Pulse) -> None:
        with pytest.raises(AttributeError):
            pulse.upload_data()

    @staticmethod
    @pytest.mark.parametrize("week", (10, 2))
    def test_upload_data(
        week: int, mock_df: pd.DataFrame, pulse: Pulse
    ) -> None:
        pulse.ctabdf = mock_df
        pulse.week = week
        pulse.s3.get_available_weeks.return_value = {10}
        pulse.upload_data()
        pulse.s3.upload_parquet.assert_called_with(
            key=f"processed-files/pulse-{str(week).zfill(2)}.parquet",
            df=pulse.ctabdf,
        )
        if week == 2:
            pulse.s3.put_collection_dates.assert_called_once()

    @staticmethod
    def test_download_data(mock_df: pd.DataFrame, pulse: Pulse) -> None:
        pulse.s3.download_parquet.return_value = mock_df  # type: ignore
        pulse.download_data()
        assert hasattr(pulse, "df")
        assert (pulse.df["TOPLINE"] == 1).all()

    @staticmethod
    @pytest.mark.parametrize("error_code", ("NoSuchKey", "AccessDenied"))
    def test_download_data_error(error_code: str, pulse: Pulse) -> None:
        mockerror = ClientError(
            error_response={"Error": {"Code": error_code}},
            operation_name="test",
        )
        pulse.s3.download_parquet = MagicMock(side_effect=mockerror)
        if error_code == "NoSuchKey":
            pulse.download_data()
            pulse.census.download.assert_called_once()
            pulse.s3.upload_parquet.assert_called_once()
        else:
            with pytest.raises(ClientError):
                pulse.download_data()

    @staticmethod
    def test_calculate_ages(mock_df: pd.DataFrame, pulse: Pulse) -> None:
        pulse.df = mock_df
        pulse.s3.get_collection_dates.return_value = {
            10: {
                "pub_date": datetime.date(2020, 7, 15),
                "start_date": datetime.date(2020, 7, 2),
                "end_date": datetime.date(2020, 7, 7),
            }
        }
        mock_df["TBIRTH_YEAR"] = 2005
        pulse._calculate_ages()
        assert (mock_df["TBIRTH_YEAR"] == 18).all()

    @staticmethod
    def test_calculate_ages_error(mock_df: pd.DataFrame, pulse: Pulse) -> None:
        pulse.df = mock_df
        pulse.s3.get_collection_dates = MagicMock(side_effect=KeyError)
        mock_df["TBIRTH_YEAR"] = 2005
        with pytest.raises(KeyError):
            pulse._calculate_ages()
            pulse.s3.put_collection_dates.assert_called_once()

    @staticmethod
    def test_parse_question_cols(pulse: Pulse, mock_df: pd.DataFrame) -> None:
        pulse.df = mock_df
        pulse._parse_question_cols()
        assert len(pulse.soneqs) > 0
        assert len(pulse.sallqs) > 0
        assert len(pulse.allqs) > 0
        assert len(pulse.wgtcols) > 0

    @staticmethod
    def test_bucketize_numeric_cols(
        pulse: Pulse, mock_df: pd.DataFrame
    ) -> None:
        pulse.df = mock_df
        pulse.df["TBIRTH_YEAR"] = 18
        pulse._bucketize_numeric_cols()
        assert pulse.df["TBIRTH_YEAR"].max() < 18

    @staticmethod
    def test_bucketize_numeric_cols_error(
        pulse: Pulse, mock_df: pd.DataFrame
    ) -> None:
        pulse.df = mock_df
        pulse.df["TBIRTH_YEAR"] = -18
        with pytest.raises(ValueError):
            pulse._bucketize_numeric_cols()

    @staticmethod
    def test_reshape_long(pulse: Pulse, mock_df: pd.DataFrame) -> None:
        pulse.df = mock_df
        pulse._coalesce_variables()
        pulse._parse_question_cols()
        pulse._reshape_long()
        assert hasattr(pulse, "longdf")
        assert len(pulse.df) < len(pulse.longdf)
        assert pulse.longdf["q_val"].isnull().sum() == 0

    @staticmethod
    def test_drop_missing_responses(
        pulse: Pulse, mock_df: pd.DataFrame
    ) -> None:
        pulse.df = mock_df
        pulse._coalesce_variables()
        pulse._parse_question_cols()
        pulse._reshape_long()
        longdf_len = len(pulse.longdf)
        pulse._drop_missing_responses()
        assert longdf_len > len(pulse.longdf)

    @staticmethod
    def test_recode_values(pulse: Pulse, mock_df: pd.DataFrame) -> None:
        pulse.df = mock_df
        pulse._coalesce_variables()
        pulse._parse_question_cols()
        pulse._reshape_long()
        qvals = pulse.longdf["q_val"].copy()
        pulse._recode_values()
        assert (qvals != pulse.longdf["q_val"]).any()

    @staticmethod
    def test_coalesce_races(pulse: Pulse, mock_df: pd.DataFrame) -> None:
        pulse.df = mock_df
        pulse._coalesce_races()
        assert pulse.df["RRACE"].between(1, 5, inclusive="both").sum() == len(
            pulse.df
        )

    @staticmethod
    def test_merge_cbsa(pulse: Pulse, mock_df: pd.DataFrame) -> None:
        pulse.df = mock_df
        pulse._coalesce_variables()
        pulse._parse_question_cols()
        pulse.df["TBIRTH_YEAR"] = 18
        pulse._bucketize_numeric_cols()
        pulse._coalesce_races()
        pulse._reshape_long()
        pulse._drop_missing_responses()
        pulse._recode_values()
        pulse._aggregate()
        pulse._merge_cbsa_info()
        pulse._reorganize_cols()
        assert hasattr(pulse.ctabdf, "cbsa_title")
