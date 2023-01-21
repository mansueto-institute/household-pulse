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
import logging
import os
import warnings
from multiprocessing import Pool

import pandas as pd
import statsmodels.api as sm
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from household_pulse.io import S3Storage

logger = logging.getLogger(__name__)


def smooth_group(group: pd.DataFrame, frac: float = 0.2) -> pd.DataFrame:
    """
    Runs LOWESS smoothing for a given group across all weeks in order to
    smooth the time series. Each question is smoothed independently.

    Args:
        group (pd.DataFrame): The pandas group to apply the function to
        frac (float, optional): A parameter to the lowess function. Defaults
            to 0.2.

    Returns:
        pd.DataFrame: The smoothed group.
    """
    wcols = [
        "pweight_share",
        "pweight_lower_share",
        "pweight_upper_share",
        "hweight_share",
        "hweight_lower_share",
        "hweight_upper_share",
    ]
    with warnings.catch_warnings():
        warnings.simplefilter("ignore")
        for wcol in wcols:
            smoothed = sm.nonparametric.lowess(
                exog=group["end_date"],
                endog=group[wcol],
                frac=frac,
                is_sorted=True,
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
    logger.info("Normalizing smoothed time series")
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
    df = S3Storage.download_all(file_type="processed")
    datedf = pd.DataFrame.from_dict(
        S3Storage.get_collection_dates(), orient="index"
    )
    df = df.merge(
        datedf["end_date"], how="left", left_on="week", right_index=True
    )
    keepcols = [
        "week",
        "xtab_var",
        "xtab_val",
        "q_var",
        "q_val",
        "pweight_share",
        "pweight_lower_share",
        "pweight_upper_share",
        "hweight_share",
        "hweight_lower_share",
        "hweight_upper_share",
        "end_date",
    ]

    df = df[keepcols]

    df.sort_values(
        by=["xtab_var", "xtab_val", "q_var", "q_val", "week"], inplace=True
    )
    df["end_date"] = pd.to_datetime(df["end_date"])

    groups = df.groupby(["xtab_var", "xtab_val", "q_var", "q_val"])
    group_dfs = (group for _, group in groups)
    ncpu = os.cpu_count()
    if ncpu is None:
        cpus = 1
    else:
        cpus = ncpu - 1
    with logging_redirect_tqdm():
        with Pool(cpus) as p:
            results = tqdm(
                p.imap(smooth_group, group_dfs),
                total=len(groups),
                desc="Smoothing",
            )
            df = pd.concat(results)
    df.drop(columns="end_date", inplace=True)
    df = normalize_smoothed(df)
    S3Storage.upload_parquet(key="smoothed/pulse-smoothed.parquet", df=df)
