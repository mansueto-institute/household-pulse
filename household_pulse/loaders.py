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
import re

import pandas as pd
import requests
from bs4 import BeautifulSoup

NUMERIC_COL_BUCKETS = {
    'TBIRTH_YEAR': {
        'bins': [1920, 1957, 1972, 1992, 2003],
        'labels': ['65+', '50-64', '30-49', '18-29']
    },
    'THHLD_NUMPER': {
        'bins': [0, 3, 6, 10, 99],
        'labels': ['1-2', '3-5', '6-9', '10+']
    },
    'THHLD_NUMKID': {
        'bins': [0, 1, 3, 6, 10, 40],
        'labels': ['0', '1-2', '3-5', '6-9', '10+']
    },
    'THHLD_NUMADLT': {
        'bins': [0, 2, 6, 9, 40],
        'labels': ['1-2', '3-5', '6-9', '10+']
    },
    'TSPNDFOOD': {
        'bins': [0, 100, 300, 500, 800, 1000],
        'labels': ['0-99', '100-299', '300-499', '500-799', '800+']
    },
    'TSPNDPRPD': {
        'bins': [0, 100, 200, 300, 400, 1000],
        'labels': ['0-99', '100-199', '200-299', '300-399', '400+']
    },
    'TSTDY_HRS': {
        'bins': [0, 5, 10, 15, 20, 50],
        'labels': ['0-4', '5-9', '10-14', '15-19', '20+']
    },
    'TNUM_PS': {
        'bins': [0, 1, 2, 3, 4, 99],
        'labels': ['0', '1', '2', '3', '4+']
    },
    'TBEDROOMS': {
        'bins': [0, 1, 2, 3, 4, 99],
        'labels': ['0', '1', '2', '3', '4+']
    },
    'TUI_NUMPER': {
        'bins': [0, 1, 2, 3, 4, 99],
        'labels': ['0', '1', '2', '3', '4+']
    },
    'TMNTHSBHND': {
        'bins': [0, 1, 3, 6, 12, 48],
        'labels': ['0', '1-2', '3-5', '6-11', '12+']
    },
    'TENROLLPUB': {
        'bins': [0, 1, 2, 3, 4, 20],
        'labels': ['0', '1', '2', '3', '4+']
    },
    'TENROLLPRV': {
        'bins': [0, 1, 2, 3, 4, 20],
        'labels': ['0', '1', '2', '3', '4+']
    },
    'TENROLLHMSCH': {
        'bins': [0, 1, 2, 3, 4, 20],
        'labels': ['0', '1', '2', '3', '4+']
    },
    'TTCH_HRS': {
        'bins': [0, 10, 20, 30, 48],
        'labels': ['0-9', '10-19', '20-29', '30+']
    },
    'TSCHLHRS': {
        'bins': [0, 10, 20, 30, 48],
        'labels': ['0-9', '10-19', '20-29', '30+']
    }
}


def load_gsheet(sheetname: str) -> pd.DataFrame:
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
        'question_mapping': '34639438',
        'response_mapping': '1561671071',
        'county_metro_state': '974836931'
    }

    if sheetname not in sheetids:
        raise ValueError(f'{sheetname} not in {sheetids.keys()}')

    df = pd.read_csv(
        f'{baseurl}/{ssid}/export?format=csv&gid={sheetids[sheetname]}'
    )

    return df


def load_census_weeks():
    """
    Scrapes date range meta data for each release of the Household Pulse data

    Returns:
        dict[int, str]: dictionary with weeks as keys and the date ranges for
            that week's survey as a string.
    """
    URL = '/'.join(
        (
            'https://www.census.gov',
            'programs-surveys',
            'household-pulse-survey',
            'data.html'
        )
    )
    page = requests.get(URL)
    soup = BeautifulSoup(page.content, 'html.parser')
    week_dict = {}
    for i in soup.find_all("p", class_="uscb-margin-TB-02 uscb-title-3"):
        label = i.text.strip('\n\t\t\t')
        if 'Week' in label:
            kv_pair = label.split(':')
            week_int = int(re.sub("[^0-9]", "", kv_pair[0]))
            if week_int > 21:
                dates = kv_pair[1][1:] + ' 2021'
            else:
                dates = kv_pair[1][1:] + ' 2020'
            week_dict[week_int] = dates
    return week_dict
