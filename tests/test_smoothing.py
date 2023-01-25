# -*- coding: utf-8 -*-
"""
Created on Saturday, 15th October 2022 4:14:15 pm
===============================================================================
@filename:  test_smoothing.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Unit tests for the smoothing.py module.
===============================================================================
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=protected-access

import warnings
from datetime import datetime
from typing import Generator
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest

from household_pulse import smoothing


@pytest.fixture
def pulsedf() -> Generator[pd.DataFrame, None, None]:
    df = pd.read_parquet("tests/testfiles/all-test.parquet")
    yield df


@pytest.fixture
def collection_dates() -> Generator[dict, None, None]:
    mock_date = datetime.strptime("2020-05-02", "%Y-%m-%d").date()
    data = {
        1: {
            "pub_date": mock_date,
            "start_date": mock_date,
            "end_date": mock_date,
        }
    }

    yield data


@patch("household_pulse.smoothing.S3Storage")
def test_smooth_pulse(mock_s3, pulsedf: pd.DataFrame, collection_dates: dict):
    s3 = mock_s3.return_value
    s3.download_all.return_value = pulsedf
    s3.get_collection_dates.return_value = collection_dates
    smoothing.smooth_pulse()


@patch("household_pulse.smoothing.S3Storage")
@patch("household_pulse.smoothing.os")
def test_smooth_pulse_no_cores(
    mockos, mock_s3, pulsedf: pd.DataFrame, collection_dates: dict
) -> None:
    s3 = mock_s3.return_value
    s3.download_all.return_value = pulsedf
    s3.get_collection_dates.return_value = collection_dates
    mockos.cpu_count.return_value = None
    smoothing.smooth_pulse()
    mockos.cpu_count.assert_called_once()


def test_smooth_group(pulsedf: pd.DataFrame) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pulsedf["end_date"] = pd.to_datetime("2020-05-02")
        cats = ["xtab_var", "xtab_val", "q_var", "q_val"]
        smodf = pulsedf.groupby(by=cats).apply(smoothing.smooth_group)
        smodf.set_index(cats + ["week", "end_date"], inplace=True)
        assert np.allclose(
            smodf.groupby(level=0)["pweight_upper_share_smoothed"].max(),
            1,
            1e-2,
        )


def test_smooth_normalized(pulsedf: pd.DataFrame) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        pulsedf["end_date"] = pd.to_datetime("2020-05-02")
        cats = ["xtab_var", "xtab_val", "q_var", "q_val"]
        smodf = pulsedf.groupby(by=cats).apply(smoothing.smooth_group)
        smodf = smoothing.normalize_smoothed(smodf)
        smodf.set_index(cats + ["week", "end_date"], inplace=True)
        assert (
            smodf.groupby(level=0)["pweight_upper_share_smoothed"]
            .max()
            .iloc[0]
            == 1
        )
