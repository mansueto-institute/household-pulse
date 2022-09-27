# -*- coding: utf-8 -*-
"""
Created on Friday, 22nd April 2022 5:09:03 pm
===============================================================================
@filename:  smoothing.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household-pulse
@purpose:   this module contains the functionality that takes the raw
            processed timeseries and creates a separate table with smoothed
            values across time.
===============================================================================
"""
import warnings

import pandas as pd
import statsmodels.api as sm
from tqdm import tqdm

from household_pulse.mysql_wrapper import PulseSQL

tqdm.pandas()


def smooth_group(group: pd.DataFrame, frac: float = 0.2) -> pd.DataFrame:
    wcols = [
        "pweight_share",
        "pweight_lower_share",
        "pweight_upper_share",
        "hweight_share",
        "hweight_lower_share",
        "hweight_upper_share",
    ]
    for wcol in wcols:
        smoothed = sm.nonparametric.lowess(
            exog=group["end_date"], endog=group[wcol], frac=frac, is_sorted=True
        )
        group[f"{wcol}_smoothed"] = smoothed[:, 1]
        group.drop(columns=wcol, inplace=True)

    return group


def normalize_smoothed(df: pd.DataFrame) -> pd.DataFrame:
    """
    Normalizes all smoothed pweights and hweights shares so that they add up
    to one within a week-xtab_val-xtab_var-q_var level

    Args:
        df (pd.DataFrame): dataframe with smoothed shares

    Returns:
        pd.DataFrame: dataframe with normalized smoothed shares
    """
    wcols = [
        "pweight_share_smoothed",
        "pweight_lower_share_smoothed",
        "pweight_upper_share_smoothed",
        "hweight_share_smoothed",
        "hweight_lower_share_smoothed",
        "hweight_upper_share_smoothed",
    ]
    grpdf = df.groupby(["week", "xtab_var", "xtab_val", "q_var"])

    for wcol in wcols:
        df[wcol] = df[wcol] / grpdf[wcol].transform("sum")

    return df


def smooth_pulse() -> None:
    """
    smoothes the entire pulse table, creating a new table with the smoothed
    weight_share variables
    """
    sql = PulseSQL()

    query = """
        SELECT week,
            xtab_var,
            xtab_val,
            q_var,
            q_val,
            pweight_share,
            pweight_lower_share,
            pweight_upper_share,
            hweight_share,
            hweight_lower_share,
            hweight_upper_share,
            end_date
        FROM pulse.pulse
        INNER JOIN pulse.collection_dates USING(week)
    """

    df = sql.get_pulse_table(query=query)
    df.sort_values(
        by=["xtab_var", "xtab_val", "q_var", "q_val", "week"], inplace=True
    )
    df["end_date"] = pd.to_datetime(df["end_date"])

    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        df = df.groupby(
            by=["xtab_var", "xtab_val", "q_var", "q_val"], sort=False
        ).progress_apply(smooth_group)
    df.drop(columns="end_date", inplace=True)
    df = normalize_smoothed(df)
    sql.update_values(table="smoothed", df=df)
    sql.close_connection()
