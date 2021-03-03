from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import zipfile
import io
import os
import requests
import pandas as pd

from googleapiclient.discovery import build
from google.oauth2 import service_account

PUF_USECOLS = [
'AHHLD_NUMPER',
 'WHYCHNGD1',
 'SPNDSRC7',
 'HLTHINS5',
 'WHEREFREE7',
 'FORCLOSE',
 'FEWRTRIPS',
 'SSAPGM2',
 'MH_SVCS',
 'WHEREFREE4',
 'HLTHINS6',
 'TSPNDPRPD',
 'SSAPGM1',
 'EXPCTLOSS',
 'INTEREST',
 'SSAEXPCT2',
 'PSWHYCHG3',
 'SSADECISN',
 'INCOME',
 'TNUM_PS',
 'PSCHNG5',
 'PSPLANS5',
 'WHYCHNGD7',
 'CHNGHOW9',
 'CHNGHOW12',
 'PSWHYCHG4',
 'WORRY',
 'WHYCHNGD4',
 'NOTGET',
 'SSAPGM5',
 'PSCHNG4',
 'SPNDSRC6',
 'PRESCRIPT',
 'SSA_APPLY',
 'HLTHINS2',
 'PSCHNG1',
 'SPNDSRC5',
 'SPNDSRC2',
 'SPNDSRC8',
 'WHYCHNGD6',
 'ABIRTH_YEAR',
 'SSAEXPCT5',
 'PSPLANS4',
 'SSAPGM3',
 'WHYCHNGD9',
 'WHEREFREE5',
 'TEACH3',
 'RENTCUR',
 'WHEREFREE3',
 'PUBHLTH',
 'ENROLL1',
 'PSCHNG3',
 'FOODSUFRSN1',
 'SSALIKELY',
 'ENROLL3',
 'WHYCHNGD13',
 'UI_APPLY',
 'HLTHINS7',
 'DELAY',
 'RHISPANIC',
 'PWEIGHT',
 'AHISPANIC',
 'PSWHYCHG5',
 'TW_START',
 'CHNGHOW4',
 'INTRNT3',
 'TEACH1',
 'INTRNT2',
 'FEWRTRANS',
 'SSA_RECV',
 'HWEIGHT',
 'AGENDER',
 'INTRNTAVAIL',
 'SPNDSRC4',
 'MS',
 'CHNGHOW2',
 'TEACH2',
 'PSWHYCHG7',
 'PSWHYCHG1',
 'EST_ST',
 'TSPNDFOOD',
 'WHYCHNGD2',
 'LIVQTR',
 'CHNGHOW8',
 'SPNDSRC3',
 'COMPAVAIL',
 'FOODSUFRSN3',
 'SCRAM',
 'COMP2',
 'PSCHNG2',
 'CHNGHOW10',
 'PSPLANS6',
 'SSAPGM4',
 'ARACE',
 'WHEREFREE1',
 'HLTHINS1',
 'FOODSUFRSN2',
 'CHNGHOW6',
 'EEDUC',
 'SSAEXPCT3',
 'PLNDTRIPS',
 'PSWHYCHG9',
 'UI_RECV',
 'MH_NOTGET',
 'ENROLL2',
 'WHYCHNGD11',
 'HLTHINS3',
 'PSPLANS2',
 'WHYCHNGD10',
 'TBIRTH_YEAR',
 'PSWHYCHG8',
 'PSPLANS3',
 'THHLD_NUMPER',
 'WRKLOSS',
 'SNAP_YN',
 'CHNGHOW11',
 'PSCHNG7',
 'PSCHNG6',
 'MORTCONF',
 'CHNGHOW3',
 'COMP3',
 'ANXIOUS',
 'EVICT',
 'ANYWORK',
 'CURFOODSUF',
 'PSWHYCHG6',
 'SCHLHRS',
 'WHEREFREE6',
 'SSAEXPCT4',
 'FOODSUFRSN5',
 'WHYCHNGD3',
 'THHLD_NUMKID',
 'TEACH4',
 'SSAEXPCT1',
 'TEACH5',
 'CHNGHOW5',
 'PSWHYCHG2',
 'TSTDY_HRS',
 'WHYCHNGD12',
 'MORTCUR',
 'AHHLD_NUMKID',
 'PRIVHLTH',
 'AEDUC',
 'EXPNS_DIF',
 'TENURE',
 'WHYCHNGD8',
 'WHYCHNGD5',
 'HLTHINS8',
 'KINDWORK',
 'TCH_HRS',
 'WHEREFREE2',
 'DOWN',
 'INTRNT1',
 'EGENDER',
 'REGION',
 'RRACE',
 'THHLD_NUMADLT',
 'CHNGHOW1',
 'CHNGHOW7',
 'SPNDSRC1',
 'PSPLANS1',
 'EST_MSA',
 'RSNNOWRK',
 'FREEFOOD',
 'HLTHINS4',
 'WEEK',
 'COMP1',
 'CHILDFOOD',
 'FOODSUFRSN4']

SCOPES = ['https://www.googleapis.com/auth/spreadsheets']
CROSSWALK_SPREADSHEET_ID = '1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo'
CROSSWALK_SHEET_NAMES = ['question_mapping', 'response_mapping', 'county_metro_state']

def check_housing_file_exists(housing_datafile: Path):
    '''
    check whether housing data csv already exists and set parameters accordingly
    '''
    if housing_datafile.exists():
        week = int(housing_datafile.read_text().splitlines()[-1].split(',')[1]) + 1
        mode, header = ('a', False)
    else:
        week = 13
        mode, header = ('w', True)
    return week, mode, header

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
    print("trying: " + url)
    r = requests.get(url)
    if not r:
        print("url does not exist: {}".format(url))
        return None
    read_zip = zipfile.ZipFile(io.BytesIO(r.content))
    data_df = pd.read_csv(read_zip.open(data_file_str(wp, 'd')), dtype={'SCRAM': 'string'}, usecols=PUF_USECOLS)
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

