# Databricks notebook source
from typing import Dict, Optional, Sequence, Tuple

import zipfile
import logging
import io
import re
import os
import sys
import requests
import numpy as np
import pandas as pd
import numpy as np
from bs4 import BeautifulSoup

import gcsfs
import google.auth
from googleapiclient.discovery import build
from google.oauth2 import service_account
from google.cloud import storage
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
    '''
    Bucketize numeric columns using the buckets specified above in NUMERIC_COL_BUCKETS dict
    
    inputs:
        df: pd DataFrame, main dataframe with columns to be bucketized
        question_mapping: pd DataFrame, question mapping crosswalk, which specifies the numeric columns
    returns: pd DataFrame with the numeric columns bucketized
    '''
    num_cols = list(question_mapping['variable'][question_mapping['type_of_variable'] == 'NUMERIC'])
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.cut(df[col],
                        bins=NUMERIC_COL_BUCKETS[col]['bins'], 
                        labels=NUMERIC_COL_BUCKETS[col]['labels'], right=False)
    return df

def data_url_str(w: int, wp: int):
    '''
    Helper function to get string for file to download from census api
    
    inputs:
        w: str, the week of data to download
        wp: str, the week of data to download, padded to 2 digits
    returns:
        string, the year/week/file.zip to be downloaded
    '''
    year = '2021' if int(w) > 21 else '2020'
    return f"{year}/wk{w}/HPS_Week{wp}_PUF_CSV.zip"

def data_file_str(wp: int, f: str):
    '''
    Helper function to get the string names of the files downloaded
    
    inputs: 
        wp: str, the week of data downloaded, padded to 2 digits
        f: str, the file to dowload (d: main data file, w: weights file)
    returns: str, name of file downloaded
    '''
    year = '2021' if int(wp) > 21 else '2020'
    if f == 'd':
        return f"pulse{year}_puf_{wp}.csv"
    elif f == 'w':
        return f"pulse{year}_repwgt_puf_{wp}.csv"

def get_puf_data(data_str: str, wp: int):
    '''
    Download Census Household Pulse PUF zip file for the given week and merge weights and PUF dataframes
    
    inputs: 
        data_str: str, the year/month/file.zip string to be downloaded 
        wp: str, the week of data to be downloaded, padded to 2 digits
    returns:
        pd DataFrame, the weeks census household pulse data merged with the weights csv
    '''
    base_url = "https://www2.census.gov/programs-surveys/demo/datasets/hhp/"
    url = base_url + data_str
    logger.info("Trying url: {}".format(url))
    try:
        r = requests.get(url)
        read_zip = zipfile.ZipFile(io.BytesIO(r.content))
        data_df = pd.read_csv(read_zip.open(data_file_str(wp, 'd')), dtype={'SCRAM': 'string'})
        weight_df = pd.read_csv(read_zip.open(data_file_str(wp, 'w')), dtype={'SCRAM': 'string'})
        return data_df.merge(weight_df, how='left', on=['SCRAM', 'WEEK'])
    except:
        logger.info("URL does not exist, stopping download: {}".format(url))
        return None

def get_crosswalk_sheets():
    '''
    Download sheets from housing_pulse_data_dictionary into dataframes
    N.B. this is the prod version for databricks (auth will not work on local machine - use LOCAL version)

    returns:
        list of pandas dataframes (3), the first three sheets in the google sheets crosswalk
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

def LOCAL_get_crosswalk_sheets(service_account_file):
    '''
    USE THIS VERSION WHEN TESTING LOCALLY
    Download data sheets from houehold_pulse_data_dictionary crosswalks

    returns:
        list of pandas dataframes (3), the first three sheets in the google sheets crosswalk
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

def get_std_err(df: pd.DataFrame, weight: str):
    '''
    Calculate standard error of dataframe

    inputs:
        df: pd DataFrame
        weight: str, specify whether person ('PWEIGHT') or household ('HWEIGHT') weight
    returns:
        float, the standard error
    '''
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

def upload_to_cloud_storage(bucket_name: str, filename, data=pd.DataFrame):
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
    if not data.empty:
        blob.upload_from_string(data.to_csv(index=False), 'text/csv', timeout=450)
    else:
        with open('logfile.log', 'rb') as logfile:
            blob.upload_from_file(logfile)

def week_mapper():
    '''
    Scrapes date range meta data for each release of the Household Pulse data

    returns:
        dict, {week int: dates}
    '''
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
    pt_estimates.rename(columns={weight: 'value'},inplace=True)
    return pt_estimates[['value', 'std_err','mrgn_err']].reset_index()

def full_crosstab(df, col_list, weight, proportion_level, critical_val=1):
    df1 = df.copy()
    detail = freq_crosstab(df1, col_list, weight, critical_val)
    top = freq_crosstab(df1, proportion_level, weight, critical_val)
    rv = detail.merge(top,'left',proportion_level,suffixes=('_full','_demo'))
    rv['proportions'] = rv['value_full']/rv['value_demo']
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

def get_file_from_storage(filepath: str):
    '''
    Download csv in google cloud storage bucket as pandas DataFrame

    inputs: 
        filepath: str, bucket/file.csv to be downloaded
    returns: pd DataFrame
    '''
    fs = gcsfs.GCSFileSystem(project='household-pulse') 
    with fs.open(filepath) as f:
        df = pd.read_csv(f)
        return df[[x for x in df.columns if not x.startswith("Unnamed:")]]


if __name__=="__main__":

    ######### Set up logging #########

    logger = logging.getLogger()
    fileHandler = logging.FileHandler("logfile.log", mode='w')
    streamHandler = logging.StreamHandler(sys.stdout)
    formatter = logging.Formatter('\n%(asctime)s - %(message)s')
    streamHandler.setFormatter(formatter)
    fileHandler.setFormatter(formatter)
    logger.addHandler(streamHandler)
    logger.addHandler(fileHandler)
    logger.setLevel(logging.INFO)

    ######### Set up parameters #########

    LOCAL = False # ensure this parameter is set to True when developing locally
    if (len(sys.argv[1:]) == 1) and (sys.argv[1] == 'LOCAL'):
        LOCAL = True

    # Crosstabs variables:
    index_list = ['WEEK']
    crosstab_list = ['TOPLINE', 'RRACE', 'EEDUC', 'EST_MSA']

    # Crosstabs filenames:
    crosstab_filename = "pulse_time_series.csv" 
    gcp_bucket = "household-pulse-bucket"
    
    ######## Download google sheets crosswalk tables #########
    try:
        if LOCAL:
            SERVICE_ACCOUNT_FILE = os.environ.get("GOOGLE_APPLICATION_CREDENTIALS")
            question_mapping, response_mapping, county_metro_state = LOCAL_get_crosswalk_sheets(SERVICE_ACCOUNT_FILE)
        else:
            # download crosswalk mapping tables
            question_mapping, response_mapping, county_metro_state = get_crosswalk_sheets()
        
        label_recode_dict = get_label_recode_dict(response_mapping)
        county_metro_state['cbsa_fips'] = county_metro_state['cbsa_fips'].astype(float).astype(str)


        ######### Check for existing crosstabs file and find latest week of data #########
        logger.info("Checking for existing crosstabs file in cloud storage bucket")
        try:
            existing_crosstabs = get_file_from_storage('household-pulse-bucket/' + crosstab_filename)
            week = int(existing_crosstabs['WEEK'].max()) + 1
            logger.info("Existing {} file, latest week in existing data is week {}".format(crosstab_filename, week-1))
        except:
            existing_crosstabs = pd.DataFrame()
            week = 29
            logger.info("No existing {} file found, generating full dataset starting from week {}".format(crosstab_filename, week))


        ######### Download new Census Household Pulse data and generate crosstab #########

        full_crosstabs = []
        missing_question_vars = {}
        missing_qs_full_list = []
        r = True
        while r:
            logger.info("Downloading data: week {}".format(week))
            week_pad = str(week).zfill(2)
            data_str = data_url_str(week, week_pad)
            week_df = get_puf_data(data_str, week_pad)

            if week_df is None:
                r = False
            else:
                recoded_df = week_df.replace(label_recode_dict)
                recoded_df['TOPLINE'] = 1

                new_questions = filter_non_weight_cols(list(set(recoded_df.columns) - set(question_mapping['variable']))) 
                missing_question_vars["WEEK " + str(week)] = list(set(new_questions) - set(missing_qs_full_list)) 
                missing_qs_full_list.extend(new_questions)
                
                question_cols = list(set(question_mapping['variable'][question_mapping.stacked_question_features=='1']).intersection(set(recoded_df.columns)))
                question_mapping_usecols = question_mapping[question_mapping['variable'].isin(question_cols)]
                select_all_questions = list(question_mapping_usecols['variable'][question_mapping_usecols['select_all_that_apply'] == '1'].unique())
                question_list = [x for x in question_cols if x not in index_list and x not in crosstab_list and not (
                        len(recoded_df[x].unique())>6)]

                recoded_df[select_all_questions] = recoded_df[select_all_questions].replace(['-99', -99],'0 - not selected')
                recoded_df = bucketize_numeric_cols(recoded_df, question_mapping)
                recoded_df.replace(['-88','-99',-88,-99],np.nan,inplace=True)

                logger.info("Generating crosstabs for week {}\n".format(week))
                crosstabs = pd.concat([bulk_crosstabs(recoded_df, index_list, crosstab_list,
                                    question_list, select_all_questions,
                                    weight='PWEIGHT', critical_val=1.645), 
                                    bulk_crosstabs(recoded_df, index_list, crosstab_list,
                                    question_list, select_all_questions,
                                    weight='HWEIGHT', critical_val=1.645)])

                crosstabs['ct_val'] = crosstabs['ct_val'].astype(str)
                
                crosstabs = crosstabs.merge(
                    county_metro_state[['cbsa_title','cbsa_fips']].drop_duplicates(),
                    left_on='ct_val',
                    right_on='cbsa_fips',
                    how='left').iloc[:, :-1]

                crosstabs = crosstabs.merge(
                    question_mapping[['description_recode', 'variable']],
                    left_on='q_var',
                    right_on='variable', 
                    how='left').iloc[:,:-1]
                
                crosstabs['collection_dates'] = crosstabs.WEEK.map(week_mapper())
                
                full_crosstabs.append(crosstabs)
                week += 1
        logger.info("Finished downloading data\n")

        logger.info("*New columns*: columns in pulse dataset not present in data dictionary: \n\n{}".format(missing_question_vars))


        ######### Upload full crosstabs and national crosstabs to cloud storage #########

        if full_crosstabs:
            logger.info("Creating full crosstabs")
            final_ct = pd.concat([existing_crosstabs] + full_crosstabs)
            logger.info("Uploading to cloud storage")
            upload_to_cloud_storage(gcp_bucket, crosstab_filename, final_ct)
            logger.info('File uploaded to {}:{}'.format(gcp_bucket, crosstab_filename))
        else:
            logger.info("Existing crosstabs are already up to date, no new data to add")
    
    except Exception as Argument:
        logger.exception("Error occured:") 

    logger.info('Uploading logfile to gcp storage')
    upload_to_cloud_storage(gcp_bucket, "logfile.log")
