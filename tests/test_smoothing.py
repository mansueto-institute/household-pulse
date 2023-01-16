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
from typing import Generator
from unittest.mock import patch

import numpy as np
import pandas as pd
import pytest
from household_pulse import smoothing


@pytest.fixture
def pulsedf() -> Generator[pd.DataFrame, None, None]:
    df = pd.read_csv("tests/pulse_table.csv")
    df["end_date"] = pd.to_datetime(df["end_date"])
    yield df


@patch("household_pulse.smoothing.PulseSQL")
def test_smooth_pulse(mocksql, pulsedf: pd.DataFrame) -> None:
    sql = mocksql.return_value
    sql.get_pulse_table.return_value = pulsedf
    smoothing.smooth_pulse()


@patch("household_pulse.smoothing.PulseSQL")
@patch("household_pulse.smoothing.os")
def test_smooth_pulse_no_cores(mockos, mocksql, pulsedf: pd.DataFrame) -> None:
    sql = mocksql.return_value
    sql.get_pulse_table.return_value = pulsedf
    mockos.cpu_count.return_value = None
    smoothing.smooth_pulse()
    mockos.cpu_count.assert_called_once()


def test_smooth_group(pulsedf: pd.DataFrame) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cats = ["xtab_var", "xtab_val", "q_var", "q_val"]
        smodf = pulsedf.groupby(by=cats).apply(smoothing.smooth_group)
        smodf.set_index(cats + ["week", "end_date"], inplace=True)
        assert np.allclose(smodf.groupby(level=0).max(), 1, 1e-2)


def test_smooth_normalized(pulsedf: pd.DataFrame) -> None:
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        cats = ["xtab_var", "xtab_val", "q_var", "q_val"]
        smodf = pulsedf.groupby(by=cats).apply(smoothing.smooth_group)
        smodf = smoothing.normalize_smoothed(smodf)
        smodf.set_index(cats + ["week", "end_date"], inplace=True)
        assert (smodf.groupby(level=0).max().values == 1).all()
