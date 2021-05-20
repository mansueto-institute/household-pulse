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

NUMERIC_COL_BUCKETS = {
    'TBIRTH_YEAR': {'bins': [1920, 1957, 1972, 1992, 2003],
                    'labels': ['65+','50-64','30-49','18-29']},
    'THHLD_NUMPER': {'bins': [0, 3, 6, 10, 99],
                    'labels': ['1-2','3-5','6-9','10+']},
    'THHLD_NUMKID': {'bins': [0, 1, 3, 6, 10, 40],
                    'labels': ['0','1-2','3-5','6-9','10+']},
    'THHLD_NUMADLT': {'bins': [0, 2, 6, 9, 40],
                     'labels': ['1-2','3-5','6-9','10+']},
    'TSPNDFOOD': {'bins': [0, 100, 300, 500, 800, 1000],
                 'labels': ['0-99','100-299','300-499','500-799','800+']},
    'TSPNDPRPD': {'bins': [0, 100, 200, 300, 400, 1000],
                 'labels': ['0-99','100-199','200-299','300-399','400+']},
    'TSTDY_HRS': {'bins': [0, 5, 10, 15, 20,50],
                 'labels': ['0-4','5-9','10-14','15-19','20+']},
    'TNUM_PS': {'bins': [0, 1, 2, 3, 4, 99],
                'labels': ['0','1','2','3','4+']},
    'TBEDROOMS': {'bins': [0, 1, 2, 3, 4, 99],
                 'labels': ['0','1','2','3','4+']},
    'TUI_NUMPER': {'bins': [0, 1, 2, 3, 4, 99],
                  'labels': ['0','1','2','3','4+']},
}

def bucketize_numeric_cols(df: pd.DataFrame, question_mapping: pd.DataFrame):
    num_cols = list(question_mapping['variable'][question_mapping['type_of_variable'] == 'NUMERIC'])
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.cut(df[col],
                        bins=NUMERIC_COL_BUCKETS[col]['bins'], 
                        labels=NUMERIC_COL_BUCKETS[col]['labels'], right=False)
    return df

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

def get_crosswalk_sheets():
    '''
    N.B. this is the prod version for databricks (auth will not work on local machine - use dev version)
    Download sheets from housing_pulse_data_dictionary into dataframes
    returns:
        list of pandas dataframes (3), the first three sheets in the google sheets  
    '''
    CROSSWALK_SHEET_NAMES = ['question_mapping', 'response_mapping', 'county_metro_state']
    credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/spreadsheets'])
    service = build('sheets', 'v4', credentials=credentials)
    sheet = service.spreadsheets()
    spreadsheet_id = '1xrfmQT7Ub1ayoNe05AQAFDhqL7qcKNSW6Y7XuA8s8uo'
    # Call the Sheets API
    result_input = sheet.values().batchGet(spreadsheetId=spreadsheet_id,
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

def upload_to_gdrive(upload_filename: str):
    '''
    Uploads crosstabs csv to gdrive folder.
    inputs:
        service_account_file: string, path to service account file 
        upload_filename: string, name of file on gdrive
    '''
    gauth = GoogleAuth()
    gauth.credentials, project_id = google.auth.default(scopes=['https://www.googleapis.com/auth/spreadsheets'])
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

def freq_crosstab(df, col_list, weight, critical_val=1):
    pt_estimates = df.groupby(col_list, as_index=True)[[i for i in df.columns if weight in i]].agg('sum')
    pt_estimates['std_err'] = get_std_err(pt_estimates, weight)
    pt_estimates['mrgn_err'] = pt_estimates.std_err * critical_val
    return pt_estimates[[weight, 'std_err','mrgn_err']].reset_index()

def national_crosstabs(df, col_list, weights, critical_val=1):
    rv = pd.DataFrame()
    for i in col_list:
        for w in weights:
            ct = freq_crosstab(df,[i], w,critical_val)
            total = ct[w].sum()
            ct['question'] = i
            ct['proportions'] = ct.apply(lambda x: x[w]/total, axis=1)
            ct['weight'] = w
            ct = ct.rename(columns={i:'response',w:'value'})
            rv = pd.concat([rv,ct])
    return rv

def full_crosstab(df, col_list, weight, proportion_level, critical_val=1):
    df1 = df.copy()
    detail = freq_crosstab(df1, col_list, weight, critical_val)
    top = freq_crosstab(df1, proportion_level, weight, critical_val)
    rv = detail.merge(top,'left',proportion_level,suffixes=('_full','_demo'))
    rv['proportions'] = rv[weight+'_full']/rv[weight+'_demo']
    return rv

def bulk_crosstabs(df, idx_list, ct_list, q_list, select_all_questions, weight='PWEIGHT', critical_val=1):
    rv = pd.DataFrame()
    input_df = df.copy()
    for ct in ct_list:
        for q in q_list:
            full = idx_list + [ct, q]
            abstract = idx_list + [ct]
            temp = input_df[-input_df[full].isna().any(axis=1)]
            if q in select_all_questions:
                all_q = [i for i in select_all_questions if q[:-1] in i]
                temp = temp[-(temp[all_q].iloc[:,:]=='0 - not selected').all(1)]
            curr_bin = full_crosstab(temp,full,
                            weight,
                            abstract,
                            critical_val=critical_val)
            curr_bin.rename(columns={q:'q_val',ct:'ct_val'},inplace=True)
            curr_bin['ct_var'] = ct
            curr_bin['q_var'] = q
            rv = pd.concat([rv,curr_bin])
    rv['weight'] = weight
    return rv

if __name__=="__main__":

    root = Path.cwd().parent

    data_dir = root/"data"
    data_dir.mkdir(exist_ok=True)

    SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")

    ###### download housing data
    raw_housing_datafile = data_dir/"puf_housing_data_raw.csv"
    remapped_housing_datafile = data_dir/"puf_housing_data_remapped_labels.csv"
    week, mode, header, csv_cols = check_housing_file_exists(remapped_housing_datafile)

    # download crosswalk mapping tables
    question_mapping, response_mapping, county_metro_state = get_crosswalk_sheets(SERVICE_ACCOUNT_FILE)
    label_recode_dict = get_label_recode_dict(response_mapping)

    cols_file = data_dir/"list_cols.txt"
    final_cols = cols = cols_file.read_text().split(" ") if cols_file.exists() else csv_cols

    # downloads full set of weeks (from week 13 onwards) or just new weeks if housing_datafile already exists
    r = True
    while r:
        week_pad = str(week).zfill(2)
        data_str = data_url_str(week, week_pad)
        week_df = get_puf_data(data_str, week_pad)

        if week_df is None:
            r = False
        else:
            # making the process robust to variable changes in the data
            if mode == 'w':
                csv_cols = cols = final_cols = week_df.columns
            else:
                missing_csv_vars = list(set(csv_cols) - set(week_df.columns))
                print("\nWeek {} dataset, new variables: \n{}\n".format(week, list(set(week_df.columns) - set(cols))))
                print("Week {} dataset, missing variables: \n{}\n".format(week, list(set(cols) - set(week_df.columns))))
                final_cols = list(set(final_cols).intersection(set(week_df.columns)))
                cols = week_df.columns
                if missing_csv_vars:
                    week_df[missing_csv_vars] = None
                week_df = week_df[csv_cols]
            week_df.replace(label_recode_dict).to_csv(remapped_housing_datafile, mode=mode, header=header, index=False)
            week_df.to_csv(raw_housing_datafile, mode=mode, header=header, index=False)
            header, mode = (False, 'a')
            week += 1
    print("Finished downloading data")

    # save list of cols present in all weeks of data:
    cols_file.write_text(' '.join(final_cols))

    ###### generate crosstabs
    df = pd.read_csv(remapped_housing_datafile, usecols=final_cols)
    df['TOPLINE'] = 1

    numeric_cols = set(df.columns).intersection(set(question_mapping['variable'][question_mapping['type_of_variable']=='NUMERIC']))
    
    question_cols = list(set(question_mapping['variable'][question_mapping.stacked_question_features=='1']).intersection(set(df.columns)))
    question_mapping_usecols = question_mapping[question_mapping['variable'].isin(question_cols)]

    select_all_questions = list(question_mapping_usecols['variable'][question_mapping_usecols['select_all_that_apply'] == '1'].unique())

    index_list = ['EST_MSA', 'WEEK']
    crosstab_list = ['TOPLINE', 'RRACE', 'EEDUC', 'INCOME']
    question_list2 = [x for x in question_cols if x not in index_list and x not in crosstab_list and not (
                    len(df[x].unique())>6 and not x in numeric_cols)]

    df[select_all_questions] = df[select_all_questions].replace('-99','0 - not selected')
    df = bucketize_numeric_cols(df, question_mapping)
    df.replace(['-88','-99',-88,-99],np.nan,inplace=True)
    crosstabs = pd.concat([bulk_crosstabs(df, index_list, crosstab_list,
                                question_list2, select_all_questions,
                                weight='PWEIGHT', critical_val=1.645), 
                                bulk_crosstabs(df, index_list, crosstab_list,
                                question_list2, select_all_questions,
                                weight='HWEIGHT', critical_val=1.645)])
    crosstabs_nat = pd.concat([bulk_crosstabs(df, ['WEEK'], ['TOPLINE'],
                                question_list2, select_all_questions,
                                weight='PWEIGHT', critical_val=1.645),
                                bulk_crosstabs(df, ['WEEK'], ['TOPLINE'],
                                question_list2, select_all_questions,
                                weight='HWEIGHT', critical_val=1.645)])
    # idx one at a time? level of proportions? TODO: Fix proportion calc w/ NA
    # -99 is DNR

    crosstabs['EST_MSA'] = (crosstabs['EST_MSA'].astype(int)).astype(str)
    crosstabs = crosstabs.merge(county_metro_state[['cbsa_title','cbsa_fips']].drop_duplicates(),
                                left_on='EST_MSA',
                                right_on='cbsa_fips').iloc[:, :-1]
    crosstabs = crosstabs.merge(question_mapping[['description_recode', 'variable']],left_on='q_var', right_on='variable').iloc[:,:-1]
    crosstabs_nat['collection_dates'] = crosstabs_nat.WEEK.map(week_mapper())
    crosstabs_nat = crosstabs_nat.merge(question_mapping[['description_recode', 'variable']],left_on='q_var', right_on='variable').iloc[:,:-1]
    crosstabs['collection_dates'] = crosstabs.WEEK.map(week_mapper())
    #natl_level = national_crosstabs(df,question_list2,['PWEIGHT','HWEIGHT'],1.645)
    #natl_level = natl_level.merge(question_mapping[['description_recode', 'variable']],
    #                              left_on='question', right_on='variable')

    upload_to_cloud_storage("household-pulse-bucket", crosstabs, "crosstabs.csv")
    upload_to_cloud_storage("household-pulse-bucket", crosstabs_nat, "crosstabs_national.csv")

