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

import requests
from bs4 import BeautifulSoup


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
