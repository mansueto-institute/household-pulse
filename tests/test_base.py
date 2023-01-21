# -*- coding: utf-8 -*-
"""
Created on 2023-01-21 04:58:20-06:00
===============================================================================
@filename:  test_base.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   This module contains the unit tests for the io.base.py module.
===============================================================================
"""
# pylint: disable=missing-function-docstring,redefined-outer-name
# pylint: disable=protected-access

import pytest

from household_pulse.io.base import IO


@pytest.fixture
def io():
    return IO(week=1)


def test_instantiation():
    assert IO(week=1)


def test_bad_instantiation_type():
    with pytest.raises(TypeError):
        IO(week="123")


def test_bad_instantiation_missing():
    with pytest.raises(TypeError):
        IO()


def test_week_property(io: IO):
    assert io.week == 1
    io.week = 2
    assert io.week == 2


def test_week_str_property(io: IO):
    assert io.week_str == "01"
    io.week = 2
    assert io.week_str == "02"
