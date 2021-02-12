from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import zipfile
import io
import os
import requests
import pandas as pd

from googleapiclient.discovery import build
from google.oauth2 import service_account

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CROSSWALK_SPREADSHEET_ID = '1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo'
CROSSWALK_SHEET_NAMES = ['question_mapping', 'response_mapping', 'county_metro_state']

DATA_COL_NAMES = ['variable', 'value']

def data_url_str(w: int, wp: int):
    return f"wk{w}/HPS_Week{wp}_PUF_CSV.zip"

def stack_df(df, col_names):
    return pd.DataFrame(df.set_index(['SCRAM', 'WEEK']).stack()).reset_index().rename(
        columns={'level_2':col_names[0], 0: col_names[1]})

def get_puf_data(data_str: str, i: int, base_url: str = "https://www2.census.gov/programs-surveys/demo/datasets/hhp/2020/"):
    '''
    download puf files for the given weeks and concatenate the datasets
    '''
    url = base_url + data_str
    print(url)
    r = requests.get(url)
    if not r:
        print("url does not exist: {}".format(url))
        return None
    read_zip = zipfile.ZipFile(io.BytesIO(r.content))
    data_df = pd.read_csv(read_zip.open("pulse2020_puf_{}.csv".format(i)), dtype={'SCRAM': 'string'})
    weight_df = pd.read_csv(read_zip.open("pulse2020_repwgt_puf_{}.csv".format(i)), dtype={'SCRAM': 'string'})
    return data_df.merge(weight_df, how='left', on=['SCRAM', 'WEEK'])

def get_crosswalk_sheets(service_account_file: Path):
    '''
    downloads data sheets from houehold_pulse_data_dictionary crosswalks
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

def get_feature_lists(question_mapping: pd.DataFrame, col_var: str):
    '''
    returns list of columns for col_var group (where col_var == 1)
    '''
    return list(question_mapping['variable'][question_mapping[col_var]=='1'])


def generate_crosstabs(df, id_vars, value_vars, var_name, value_name):
    '''
    '''
    return df.melt(id_vars=id_vars, 
                    value_vars=value_vars, 
                    var_name=var_name, 
                    value_name=value_name)
