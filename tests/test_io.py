# -*- coding: utf-8 -*-
"""
Created on 2023-01-25 03:18:54-06:00
===============================================================================
@filename:  test_io.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   Tests for the functions in the io.py module.
===============================================================================
"""
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest

from household_pulse.io import load_gsheet


@patch.object(pd, "read_csv", MagicMock(return_value=MagicMock()))
@pytest.mark.parametrize("sheetname", ("badname", "question_mapping"))
def test_load_gsheet(sheetname: str) -> None:
    if sheetname == "badname":
        with pytest.raises(ValueError):
            load_gsheet(sheetname=sheetname)
    else:
        load_gsheet(sheetname=sheetname)
        pd.read_csv.assert_called_once()
