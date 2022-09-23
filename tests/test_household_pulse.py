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


def test_import_downloader():
    import household_pulse.downloader  # noqa: F401


def test_import_sql_wrapper():
    import household_pulse.mysql_wrapper  # noqa: F401


def test_import_pulse():
    import household_pulse.pulse  # noqa: F401


def test_import_preload_data():
    import household_pulse.preload_data  # noqa: F401


def test_import_preload_cache():
    import household_pulse.preload_data.fetch_and_cache  # noqa: F401


def test_import_preload_cache_utils():
    import household_pulse.preload_data.fetch_and_cache_utils  # noqa: F401
