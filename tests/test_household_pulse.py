# -*- coding: utf-8 -*-
"""
Created on Monday, 19th September 2022 7:29:31 pm
===============================================================================
@filename:  test_household_pulse.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Package-level import unit tests.
===============================================================================
"""
# pylint: disable=unused-import,missing-function-docstring,import-error
# pylint: disable=import-outside-toplevel


def test_import_top_level():
    import household_pulse  # noqa: F401


def test_import_pulse():
    import household_pulse.pulse  # noqa: F401


def test_import_io():
    import household_pulse.io  # noqa: F401


def test_import_io_base():
    import household_pulse.io.base  # noqa: F401


def test_import_io_census():
    import household_pulse.io.census  # noqa: F401


def test_import_io_s3():
    import household_pulse.io.s3  # noqa: F401


def test_import_preload_data():
    import household_pulse.preload_data  # noqa: F401


def test_import_preload_cache():
    import household_pulse.preload_data.fetch_and_cache  # noqa: F401


def test_import_preload_cache_utils():
    import household_pulse.preload_data.fetch_and_cache_utils  # noqa: F401
