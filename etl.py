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

def data_url_str(w: int, wp: int):
    return f"wk{w}/HPS_Week{wp}_PUF_CSV.zip"

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
    return data_df.merge(weight_df, how='left', on='SCRAM')

def get_crosswalk_sheets(service_account_file: Path):
    '''
    downloads data sheets from crosswalk google sheet
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
