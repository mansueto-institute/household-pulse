# -*- coding: utf-8 -*-
"""
Created on Sunday, 10th October 2021 9:04:41 pm
===============================================================================
@filename:  loaders.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household pulse
@purpose:   module for all io utils
===============================================================================
"""
import io
from zipfile import ZipFile

import pandas as pd
import requests


def load_crosstab(sheetname: str) -> pd.DataFrame:
    """
    Loads one of the three crosstabs used for mapping responses. It has to
    be one of {'question_mapping', 'response_mapping, 'county_metro_state'}.

    Args:
        sheetname (str): sheetname in the data dictionary google sheet

    Returns:
        pd.DataFrame: loaded crosstab
    """
    baseurl = 'https://docs.google.com/spreadsheets/d'
    ssid = '1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo'

    sheetids = {
        'question_mapping': '1115503899',
        'response_mapping': '0',
        'county_metro_state': '974836931'
    }

    if sheetname not in sheetids:
        raise ValueError(f'{sheetname} not in {sheetids.keys()}')

    df = pd.read_csv(
        f'{baseurl}/{ssid}/export?format=csv&gid={sheetids[sheetname]}'
    )

    return df


def make_data_url(week: int) -> str:
    """
    Helper function to get string for file to download from census api

    Args:
        week (int): the week of data to download

    Returns:
        str: the year/week/file.zip to be downloaded
    """
    year = '2021' if int(week) > 21 else '2020'
    return f"{year}/wk{week}/HPS_Week{week}_PUF_CSV.zip"


def make_data_fname(week: int, fname: str) -> str:
    """
    Helper function to get the string names of the files downloaded

    Args:
        week (int): the week of data to download
        fname (str): the file to dowload (d: main data file, w: weights file)

    Returns:
        str: name of file downloaded
    """
    if fname not in {'d', 'w'}:
        raise ValueError(f"fname muts be in {'d', 'w'}")

    year = '2021' if int(week) > 21 else '2020'
    if fname == 'd':
        return f"pulse{year}_puf_{week}.csv"
    else:
        return f"pulse{year}_repwgt_puf_{week}.csv"


def make_recode_map(resdf: pd.DataFrame) -> dict:
    """
    Convert question response mapping df into dict to recode labels

    Args:
        resdf (pd.DataFrame): The response_mapping df from the
            household_pulse_data_dictionary google sheet

    Returns:
        dict: {variable: {value: label_recode}}
    """
    resdf = resdf[resdf['do_not_join'] == 0].copy()
    resdf['value'] = resdf['value'].astype('float64')
    result: dict[str, dict] = {}
    for row in resdf.itertuples():
        if row.variable not in result.keys():
            result[row.variable] = {}
        result[row.variable][row.value] = row.label_recode
    return result


def download_puf(week: int) -> pd.DataFrame:
    """
    Download Census Household Pulse PUF zip file for the given week and merge
    weights and PUF dataframes

    Args:
        week (int): the week of data to download

    Returns:
        pd.DataFrame: the weeks census household pulse data merged with the
            weights csv
    """
    base_url = "https://www2.census.gov/programs-surveys/demo/datasets/hhp/"
    url = ''.join((base_url, make_data_url(week)))
    r = requests.get(url)
    read_zip = ZipFile(io.BytesIO(r.content))

    data_df: pd.DataFrame = pd.read_csv(
        read_zip.open(make_data_fname(week, 'd')),
        dtype={'SCRAM': 'string'})
    weight_df: pd.DataFrame = pd.read_csv(
        read_zip.open(make_data_fname(week, 'w')),
        dtype={'SCRAM': 'string'})

    df = data_df.merge(weight_df, how='left', on=['SCRAM', 'WEEK'])

    return df