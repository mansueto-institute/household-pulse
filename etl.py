from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import zipfile
import io
import re
import os
import requests
import numpy as np
import pandas as pd

from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CROSSWALK_SPREADSHEET_ID = '1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo'
CROSSWALK_SHEET_NAMES = ['question_mapping', 'response_mapping', 'county_metro_state']

def check_housing_file_exists(housing_datafile: Path):
    '''
    check whether housing data csv already exists and set parameters accordingly
    '''
    if housing_datafile.exists():
        file = housing_datafile.read_text().splitlines()
        week = int(file[-1].split(',')[1]) + 1
        mode, header = ('a', False)
        cols = list(file[0].split(','))
    else:
        week = 13
        mode, header, cols  = ('w', True, None)
    return week, mode, header, cols

def data_url_str(w: int, wp: int):
    year = '2021' if int(w) > 21 else '2020'
    return f"{year}/wk{w}/HPS_Week{wp}_PUF_CSV.zip"

def data_file_str(wp: int, f: str):
    year = '2021' if int(wp) > 21 else '2020'
    if f == 'd':
        return f"pulse{year}_puf_{wp}.csv"
    elif f == 'w':
        return f"pulse{year}_repwgt_puf_{wp}.csv"

def get_puf_data(data_str: str, wp: int, 
                 base_url: str = "https://www2.census.gov/programs-surveys/demo/datasets/hhp/"):
    '''
    download puf zip file for the given week and merge weights and puf dataframes
    '''
    url = base_url + data_str
    r = requests.get(url)
    print("Trying: {}".format(url))
    if not r:
        print("URL does not exist: {}".format(url))
        return None
    read_zip = zipfile.ZipFile(io.BytesIO(r.content))
    data_df = pd.read_csv(read_zip.open(data_file_str(wp, 'd')), dtype={'SCRAM': 'string'})
    weight_df = pd.read_csv(read_zip.open(data_file_str(wp, 'w')), dtype={'SCRAM': 'string'})
    return data_df.merge(weight_df, how='left', on=['SCRAM', 'WEEK'])

def get_crosswalk_sheets(service_account_file: Path):
    '''
    download data sheets from houehold_pulse_data_dictionary crosswalks
    '''
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result_input = sheet.values().batchGet(spreadsheetId=CROSSWALK_SPREADSHEET_ID,
                                ranges=CROSSWALK_SHEET_NAMES).execute()
    ranges = result_input.get('valueRanges', [])
    data = []
    for r in ranges:
        values_input = r.get('values', [])
        df = pd.DataFrame(values_input[1:], columns=values_input[0])
        data.append(df)
    return data

def get_label_recode_dict(response_mapping: pd.DataFrame):
    '''
    convert question response mapping df into dict to recode labels 
    '''
    response_mapping_df = response_mapping[response_mapping['do_not_join']=='0']
    response_mapping_df['value'] = response_mapping_df['value'].astype('float64')
    d = {}
    for i, row in response_mapping_df[['variable','value','label_recode']].iterrows():
        if row['variable'] not in d.keys():
            d[row['variable']] = {}
        d[row['variable']][row['value']] = row['label_recode']
    return d

def get_std_err(df, weight):
    #make 1d array of weight col
    obs_wgts = df[weight].to_numpy().reshape(len(df),1)
    #make 80d array of replicate weights
    rep_wgts = df[[i for i in df.columns if weight in i and not i == weight]].to_numpy()
    #return standard error of estimate
    return np.sqrt((np.sum(np.square(rep_wgts-obs_wgts),axis=1)*(4/80)))

def filter_non_weight_cols(cols_list):
    r = re.compile("(?!.*WEIGHT\d+)")
    return list(filter(r.match, cols_list))

def generate_crosstab(df: pd.DataFrame, group_level: str, weight_var: str):
    '''
    generate crosstabs (from Nico's example code)
    '''
    crosstab = df.groupby([group_level]+['question_group', 'question_val', 'xtab_group', 'xtab_val']).agg({weight_var: 'sum'}).reset_index()
    crosstab["weight_total"] = crosstab.groupby([group_level]+['question_group','xtab_group'])[weight_var].transform('sum')
    crosstab["share"] = crosstab[weight_var]/crosstab.weight_total
    crosstab["weight_total_val"] = crosstab.groupby([group_level]+['question_group','xtab_group','xtab_val'])[weight_var].transform('sum')
    crosstab["share_val"] = crosstab[weight_var]/crosstab.weight_total_val
    return crosstab.sort_values(by=[group_level]+['question_group', 'xtab_group'], ascending=True)

