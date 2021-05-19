from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import zipfile
import io
import re
import os
import requests
import numpy as np
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import storage
from pydrive.auth import GoogleAuth
from pydrive.drive import GoogleDrive
from oauth2client.service_account import ServiceAccountCredentials

SHEETS_SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CROSSWALK_SPREADSHEET_ID = '1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo'
CROSSWALK_SHEET_NAMES = ['question_mapping', 'response_mapping', 'county_metro_state']
DRIVE_SCOPES = ['https://www.googleapis.com/auth/drive']
GDRIVE_ID = '14LK-dEay1G9UpBjXw6Kt9eTXwZjx8rj9'

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
    Download data sheets from houehold_pulse_data_dictionary crosswalks
    '''
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=SHEETS_SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    sheet = service.spreadsheets()
    result_input = sheet.values().batchGet(spreadsheetId=CROSSWALK_SPREADSHEET_ID,
                                ranges=CROSSWALK_SHEET_NAMES).execute()
    ranges = result_input.get('valueRanges', [])
    data = []
    for r in ranges[:3]:
        values_input = r.get('values', [])
        df = pd.DataFrame(values_input[1:], columns=values_input[0])
        data.append(df)
    return data

def get_label_recode_dict(response_mapping: pd.DataFrame):
    '''
    Convert question response mapping df into dict to recode labels 
    inputs:
        response_mapping: pd.DataFrame, the response_mapping df from the household_publse_data_dictionary google sheet
    returns: dictionary - {variable: {value: label_recode}}
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
    '''
    Helper function to filter columns in dataframe to just variable columns (removes weight columns)
    inputs:
        cols_list: list of strings, the column names of the dataframe
    returns: list of strings, the column names with the WEIGHT column names removed
    '''
    r = re.compile("(?!.*WEIGHT\d+)")
    return list(filter(r.match, cols_list))

def export_to_sheets(df, sheet_name, service_account_file, workbook_id=CROSSWALK_SPREADSHEET_ID):
    creds = service_account.Credentials.from_service_account_file(service_account_file, scopes=SHEETS_SCOPES)
    service = build('sheets', 'v4', credentials=creds)
    service.spreadsheets().values().update(
        spreadsheetId=workbook_id,
        valueInputOption='RAW',
        range=sheet_name,
        body=dict(
            majorDimension='ROWS',
            values=df.T.reset_index().T.values.tolist())
    ).execute()

def upload_to_cloud_storage(bucket_name: str, df: pd.DataFrame, filename: str):
    '''
    Uploads a dataframe to cloud storage bucket.
    inputs:
        bucket_name: string, name of the bucket to upload to 
        df: pd.DataFrame to be uploaded to cloud storage
        filename: string, name of file in cloud storage
    '''
    storage_client = storage.Client()
    bucket = storage_client.get_bucket(bucket_name)
    blob = bucket.blob(filename)
    blob.upload_from_string(df.to_csv(), 'text/csv', timeout=450)
    print('File uploaded to {}:{}.'.format(bucket_name, filename))

def upload_to_gdrive(service_account_file: Path, upload_filename: str):
    '''
    Uploads crosstabs csv to gdrive folder.
    inputs:
        service_account_file: string, path to service account file 
        upload_filename: string, name of file on gdrive
    '''
    gauth = GoogleAuth()
    gauth.credentials = ServiceAccountCredentials.from_json_keyfile_name(service_account_file, DRIVE_SCOPES[0])
    drive = GoogleDrive(gauth)
    gfile = drive.CreateFile({'parents': [{'id': GDRIVE_ID}], 'title': 'crosstabs.csv'})
    gfile.SetContentFile(upload_filename)
    gfile.Upload()
    print("uploaded file to gdrive")

def week_mapper():
    URL = 'https://www.census.gov/programs-surveys/household-pulse-survey/data.html'
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
