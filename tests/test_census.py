# -*- coding: utf-8 -*-
"""
Created on 2023-01-21 05:03:54-06:00
===============================================================================
@filename:  test_census.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Unit tests for the Census class.
===============================================================================
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=protected-access

from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from household_pulse.io import Census


@pytest.fixture
def census() -> Census:
    return Census(week=1)


@pytest.fixture
def mock_data_site() -> str:
    with open("tests/testfiles/test_page.txt", "r", encoding="utf-8") as file:
        filestr = file.read()
        return filestr


@pytest.fixture
def mock_zip_10() -> bytes:
    with open("tests/testfiles/HPS_Week10_PUF_CSV.zip", "rb") as file:
        filebytes = file.read()
        return filebytes


@pytest.fixture
def mock_zip_40() -> bytes:
    with open("tests/testfiles/HPS_Week40_PUF_CSV.zip", "rb") as file:
        filebytes = file.read()
        return filebytes


def test_instantiation():
    assert Census(week=1)


@patch.object(
    Census,
    "get_week_year_map",
    MagicMock(return_value={10: 2020}),
)
@patch.object(
    Census,
    "_download_hh_weights",
    MagicMock(
        return_value=pd.read_csv("tests/testfiles/pulse2020_puf_hhwgt_10.csv")
    ),
)
@patch("household_pulse.io.census.requests")
def test_download_early(mock_requests, mock_zip_10: bytes) -> None:
    census = Census(week=10)
    mock_get = MagicMock()
    mock_requests.get.return_value = mock_get
    mock_get.content = mock_zip_10
    df = census.download()
    assert len(df) == 4


@patch.object(
    Census,
    "get_week_year_map",
    MagicMock(return_value={40: 2021}),
)
@patch("household_pulse.io.census.requests")
def test_download_from_census_late(mock_requests, mock_zip_40: bytes) -> None:
    census = Census(week=40)
    mock_get = MagicMock()
    mock_requests.get.return_value = mock_get
    mock_get.content = mock_zip_40
    df = census.download()
    assert len(df) == 4


@patch.object(Census, "_make_data_url", MagicMock(return_value=""))
@pytest.mark.parametrize("week", (10, 13))
def test_download_hh_weights(week: int) -> None:
    census = Census(week=week)
    census.url = "tests/testfiles/pulse2020_puf_hhwgt_10.csv"
    if week == 13:
        with pytest.raises(ValueError):
            hhwdf = census._download_hh_weights()
    else:
        hhwdf = census._download_hh_weights()
        assert hhwdf.equals(
            pd.read_csv("tests/testfiles/pulse2020_puf_hhwgt_10.csv")
        )


@patch.object(
    Census,
    "get_week_year_map",
    MagicMock(return_value={10: 2020, 13: 2020}),
)
@pytest.mark.parametrize(
    "week,hweights", ((13, True), (13, False), (10, True), (10, False))
)
def test_make_data_url(week: int, hweights: bool) -> None:
    census = Census(week=week)
    if week == 13 and hweights:
        with pytest.raises(ValueError):
            census._make_data_url(hweights=hweights)
    else:
        if hweights:
            expected = f"2020/wk{week}/pulse2020_puf_hhwgt_{week}.csv"
        else:
            expected = f"2020/wk{week}/HPS_Week{week}_PUF_CSV.zip"
        assert expected == census._make_data_url(hweights)


@patch.object(
    Census,
    "get_week_year_map",
    MagicMock(return_value={10: 2020, 13: 2020, 52: 2023}),
)
@pytest.mark.parametrize(
    "week,fname", ((13, "j"), (13, "d"), (10, "w"), (10, "d"), (52, "d"))
)
def test_make_data_fname(week: int, fname: str) -> None:
    census = Census(week=week)
    if fname not in {"d", "w"}:
        with pytest.raises(ValueError):
            census._make_data_fname(fname=fname)
    # this tests for an ad-hoc issue on the census website
    elif week == 52:
        expected = f"pulse2022_puf_{week}.csv"
        assert expected == census._make_data_fname(fname=fname)
    else:
        if fname == "d":
            expected = f"pulse2020_puf_{week}.csv"
        else:
            expected = f"pulse2020_repwgt_puf_{week}.csv"
        assert expected == census._make_data_fname(fname=fname)


def test_load_collection_dates(census: Census, mock_data_site: str) -> None:
    with patch("household_pulse.io.census.requests") as mock_requests:
        mock_get = MagicMock()
        mock_requests.get.return_value = mock_get
        mock_get.content = mock_data_site.encode()
        dates = census.load_collection_dates()
        assert len(dates) > 0


def test_weekyrmap(census: Census) -> None:
    weekyrmap = census.get_week_year_map()
    assert isinstance(weekyrmap, dict)
    assert len(weekyrmap) > 0
    assert all(isinstance(week, int) for week in weekyrmap.keys())
    assert all(isinstance(year, int) for year in weekyrmap.values())
