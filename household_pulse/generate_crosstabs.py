# -*- coding: utf-8 -*-
"""
Created on Tuesday, 5th October 2021 1:38:42 pm
===============================================================================
@filename:  generate_crosstabs.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household pulse
@purpose:   enter purpose
===============================================================================
"""
import re

import numpy as np
import pandas as pd
import requests

from household_pulse.loaders import (download_puf, load_crosstab,
                                     make_recode_map)

# from bs4 import BeautifulSoup


NUMERIC_COL_BUCKETS = {
    'TBIRTH_YEAR': {'bins': [1920, 1957, 1972, 1992, 2003],
                    'labels': ['65+', '50-64', '30-49', '18-29']},
    'THHLD_NUMPER': {'bins': [0, 3, 6, 10, 99],
                     'labels': ['1-2', '3-5', '6-9', '10+']},
    'THHLD_NUMKID': {'bins': [0, 1, 3, 6, 10, 40],
                     'labels': ['0', '1-2', '3-5', '6-9', '10+']},
    'THHLD_NUMADLT': {'bins': [0, 2, 6, 9, 40],
                      'labels': ['1-2', '3-5', '6-9', '10+']},
    'TSPNDFOOD': {'bins': [0, 100, 300, 500, 800, 1000],
                  'labels': ['0-99', '100-299', '300-499', '500-799', '800+']},
    'TSPNDPRPD': {'bins': [0, 100, 200, 300, 400, 1000],
                  'labels': ['0-99', '100-199', '200-299', '300-399', '400+']},
    'TSTDY_HRS': {'bins': [0, 5, 10, 15, 20, 50],
                  'labels': ['0-4', '5-9', '10-14', '15-19', '20+']},
    'TNUM_PS': {'bins': [0, 1, 2, 3, 4, 99],
                'labels': ['0', '1', '2', '3', '4+']},
    'TBEDROOMS': {'bins': [0, 1, 2, 3, 4, 99],
                  'labels': ['0', '1', '2', '3', '4+']},
    'TUI_NUMPER': {'bins': [0, 1, 2, 3, 4, 99],
                   'labels': ['0', '1', '2', '3', '4+']}
}


def bucketize_numeric_cols(df: pd.DataFrame,
                           qumdf: pd.DataFrame) -> pd.DataFrame:
    """
    Bucketize numeric columns using the buckets specified above in
    NUMERIC_COL_BUCKETS dict

    Args:
        df (pd.DataFrame): main dataframe with columns to be bucketized
        qumdf (pd.DataFrame): question mapping crosswalk, which specifies the
            numeric columns

    Returns:
        pd.DataFrame: with the numeric columns bucketized
    """
    num_cols = qumdf[qumdf['type_of_variable'] == 'NUMERIC']['variable']
    for col in num_cols:
        if col in df.columns:
            df[col] = pd.cut(
                df[col],
                bins=NUMERIC_COL_BUCKETS[col]['bins'],
                labels=NUMERIC_COL_BUCKETS[col]['labels'],
                right=False)
    return df


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


def get_std_err(df: pd.DataFrame, weight: str):
    '''
    Calculate standard error of dataframe

    inputs:
        df: pd DataFrame
        weight: str, specify whether person ('PWEIGHT') or household ('HWEIGHT') weight
    returns:
        float, the standard error
    '''
    # make 1d array of weight col
    obs_wgts = df[weight].to_numpy().reshape(len(df), 1)
    # make 80d array of replicate weights
    rep_wgts = df[[
        i for i in df.columns if weight in i and not i == weight]].to_numpy()
    # return standard error of estimate
    return np.sqrt((np.sum(np.square(rep_wgts-obs_wgts), axis=1)*(4/80)))


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
    pt_estimates = df.groupby(col_list, as_index=True)[
        [i for i in df.columns if weight in i]].agg('sum')
    pt_estimates['std_err'] = get_std_err(pt_estimates, weight)
    pt_estimates['mrgn_err'] = pt_estimates.std_err * critical_val
    pt_estimates.rename(columns={weight: 'value'}, inplace=True)
    return pt_estimates[['value', 'std_err', 'mrgn_err']].reset_index()


def full_crosstab(df, col_list, weight, proportion_level, critical_val=1):
    df1 = df.copy()
    detail = freq_crosstab(df1, col_list, weight, critical_val)
    top = freq_crosstab(df1, proportion_level, weight, critical_val)
    rv = detail.merge(top, 'left', proportion_level,
                      suffixes=('_full', '_demo'))
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
                temp = temp[-(temp[all_q].iloc[:, :] ==
                              '0 - not selected').all(1)]
            curr_bin = full_crosstab(temp, full,
                                     weight,
                                     abstract,
                                     critical_val=critical_val)
            curr_bin.rename(columns={q: 'q_val', ct: 'ct_val'}, inplace=True)
            curr_bin['ct_var'] = ct
            curr_bin['q_var'] = q
            rv = pd.concat([rv, curr_bin])
    rv['weight'] = weight
    return rv


if __name__ == "__main__":
    # Crosstabs variables:
    index_list = ['WEEK']
    crosstab_list = ['TOPLINE', 'RRACE', 'EEDUC', 'EST_MSA']

    # Crosstabs filenames:
    crosstab_filename = "pulse_time_series.csv"
    gcp_bucket = "household-pulse-bucket"

    qumdf = load_crosstab('question_mapping.csv')
    resdf = load_crosstab('response_mapping.csv')
    cmsdf = load_crosstab('county_metro_state.csv')

    labelmap = make_recode_map(resdf)
    cmsdf['cbsa_fips'] = cmsdf['cbsa_fips'].astype(float).astype(str)

    week = 29
    full_crosstabs = []
    missing_question_vars = {}
    missing_qs_full_list = []

    df = download_puf(week=week)
    df = df.replace(labelmap).copy()
    df['TOPLINE'] = 1

    # get questions in downloaded data but not in crosstab
    new_qs: pd.Index = df.columns[~df.columns.isin(qumdf['variable'])]
    # filter non weight cols
    new_qs = new_qs[~new_qs.str.contains('WEIGHT')]
    missing_question_vars[week] = new_qs[~new_qs.isin(missing_qs_full_list)]
    missing_qs_full_list.extend(new_qs.tolist())

    qcols = qumdf[qumdf['stacked_question_features'] == 1]['variable']
    qcols = qcols[qcols.isin(df.columns)]
    qumdf = qumdf[qumdf['variable'].isin(qcols)].copy()

    allqs = qumdf[qumdf['select_all_that_apply'] == 1]['variable'].unique()

    question_list = []
    for qcol in qcols:
        if qcol in index_list or qcol in crosstab_list:
            continue
        elif df[qcol].nunique() > 6:
            continue
        else:
            question_list.append(qcol)

    df[allqs] = df[allqs].replace(['-99', -99], '0 - not selected')
    df = bucketize_numeric_cols(df, qumdf)
    df.replace(['-88', '-99', -88, -99], np.nan, inplace=True)

    crosstabs = pd.concat([bulk_crosstabs(recoded_df, index_list, crosstab_list,
                                          question_list, select_all_questions,
                                          weight='PWEIGHT', critical_val=1.645),
                           bulk_crosstabs(recoded_df, index_list, crosstab_list,
                                          question_list, select_all_questions,
                                          weight='HWEIGHT', critical_val=1.645)])

    crosstabs['ct_val'] = crosstabs['ct_val'].astype(str)

    crosstabs = crosstabs.merge(
        cmsdf[['cbsa_title', 'cbsa_fips']].drop_duplicates(),
        left_on='ct_val',
        right_on='cbsa_fips',
        how='left').iloc[:, :-1]

    crosstabs = crosstabs.merge(
        qumdf[['description_recode', 'variable']],
        left_on='q_var',
        right_on='variable',
                how='left').iloc[:, :-1]

    crosstabs['collection_dates'] = crosstabs.WEEK.map(week_mapper())

    full_crosstabs.append(crosstabs)
    week += 1

    if full_crosstabs:
        logger.info("Creating full crosstabs")
        final_ct = pd.concat([existing_crosstabs] + full_crosstabs)
        logger.info("Uploading to cloud storage")
        upload_to_cloud_storage(gcp_bucket, crosstab_filename, final_ct)
        logger.info('File uploaded to {}:{}'.format(
            gcp_bucket, crosstab_filename))
        else:
            logger.info(
                "Existing crosstabs are already up to date, no new data to add")

    except Exception as Argument:
        logger.exception("Error occured:")

    logger.info('Uploading logfile to gcp storage')
    upload_to_cloud_storage(gcp_bucket, "logfile.log")
