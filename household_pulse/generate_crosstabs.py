# -*- coding: utf-8 -*-
"""
Created on Tuesday, 5th October 2021 1:38:42 pm
===============================================================================
@filename:  generate_crosstabs.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household pulse
@purpose:   generate all crosstabs for a single week
===============================================================================
"""

import numpy as np
import pandas as pd

from household_pulse.loaders import (download_puf, load_census_weeks,
                                     load_crosstab, make_recode_map)

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
    """
    Helper function to get the string names of the files downloaded

    inputs:
        wp: str, the week of data downloaded, padded to 2 digits
        f: str, the file to dowload (d: main data file, w: weights file)
    returns: str, name of file downloaded
    """
    year = '2021' if int(wp) > 21 else '2020'
    if f == 'd':
        return f"pulse{year}_puf_{wp}.csv"
    elif f == 'w':
        return f"pulse{year}_repwgt_puf_{wp}.csv"


def get_std_err(df: pd.DataFrame, weight_col: str) -> list[float]:
    """
    Calculate standard error of dataframe

    Args:
        df (pd.DataFrame): point estimates from freq_crosstab
        weight_col (str): specify whether person ('PWEIGHT') or household
            ('HWEIGHT') weight

    Returns:
        float: the standard error
    """
    # only keep passed weight types
    df = df.loc[:, df.columns.str.contains(weight_col)].copy()
    # here we subtract the replicate weights from the main weight col
    # broadcasting across the columns
    wgtdf = df.loc[:, df.columns != weight_col].sub(
        df[weight_col],
        axis=0,
        level=2)
    result: pd.Series = (wgtdf.pow(2).sum(axis=1) * (4/80)).pow(1/2)

    return result.values.tolist()


def freq_crosstab(df: pd.DataFrame,
                  col_list: list[str],
                  weight_col: str,
                  critical_val: float = 1.0) -> pd.DataFrame:
    """
    [summary]

    Args:
        df (pd.DataFrame): [description]
        col_list (list[str]): [description]
        weight_col (str): [description]
        critical_val (int, optional): [description]. Defaults to 1.

    Returns:
        pd.DataFrame: [description]
    """
    w_cols = df.columns[df.columns.str.contains(weight_col)]
    pt_estimates = df.groupby(col_list)[w_cols].sum()
    pt_estimates['std_err'] = get_std_err(pt_estimates, weight_col)
    pt_estimates['mrgn_err'] = pt_estimates['std_err'] * critical_val
    pt_estimates.rename(columns={weight_col: 'value'}, inplace=True)
    return pt_estimates[['value', 'std_err', 'mrgn_err']].reset_index()


def full_crosstab(df: pd.DataFrame,
                  col_list: list[str],
                  weight_col: str,
                  abstract: list[str],
                  critical_val: float = 1.0) -> pd.DataFrame:
    """
    [summary]

    Args:
        df (pd.DataFrame): [description]
        col_list (list[str]): [description]
        weight_col (str): [description]
        abstract (list[str]): [description]
        critical_val (int, optional): [description]. Defaults to 1.

    Returns:
        pd.DataFrame: [description]
    """
    detail = freq_crosstab(df, col_list, weight_col, critical_val)
    top = freq_crosstab(df, abstract, weight_col, critical_val)
    rv = detail.merge(
        right=top,
        how='left',
        on=abstract,
        suffixes=('_full', '_demo'))
    rv['proportions'] = rv['value_full']/rv['value_demo']
    return rv


def bulk_crosstabs(df: pd.DataFrame,
                   idxlist: list[str],
                   ctablist: list[str],
                   qstnlist: list[str],
                   sallqs: list[str],
                   weight_col: str = 'PWEIGHT',
                   critical_val: float = 1) -> pd.DataFrame:
    """
    [summary]

    Args:
        df (pd.DataFrame): [description]
        idxlist (list[str]): [description]
        ctablist (list[str]): [description]
        qstnlist (list[str]): [description]
        sallqs (list[str]): [description]
        weight_col (str, optional): [description]. Defaults to 'PWEIGHT'.
        critical_val (float, optional): [description]. Defaults to 1.

    Returns:
        pd.DataFrame: [description]
    """
    auxs = []
    input_df = df.copy()
    for ct in ctablist:
        for q in qstnlist:
            col_list = idxlist + [ct, q]
            abstract = idxlist + [ct]
            tempdf = input_df.dropna(axis=0, how='any', subset=col_list)
            if q in sallqs:
                all_q = [x for x in sallqs if x.startswith(q[:-1])]
                sallmask = (tempdf[all_q] == '0 - not selected').all(axis=1)
                tempdf = tempdf[~sallmask]
            auxdf = full_crosstab(
                df=tempdf,
                col_list=col_list,
                weight_col=weight_col,
                abstract=abstract,
                critical_val=critical_val)
            auxdf.rename(columns={q: 'q_val', ct: 'ct_val'}, inplace=True)
            auxdf['ct_var'] = ct
            auxdf['q_var'] = q
            auxs.append(auxdf)
    rv = pd.concat(auxs)
    rv['weight'] = weight_col
    return rv


if __name__ == "__main__":
    # Crosstabs variables:
    idxlist = ['WEEK']
    ctablist = ['TOPLINE', 'RRACE', 'EEDUC', 'EST_MSA']

    # Crosstabs filenames:
    crosstab_filename = "pulse_time_series.csv"

    qumdf = load_crosstab('question_mapping')
    resdf = load_crosstab('response_mapping')
    cmsdf = load_crosstab('county_metro_state')

    labelmap = make_recode_map(resdf)
    cmsdf['cbsa_fips'] = cmsdf['cbsa_fips'].astype(float).astype(str)

    week = 29
    missing_question_vars = {}
    missing_qs_full_list: list[str] = []

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

    sallqs = (
        qumdf[qumdf['select_all_that_apply'] == 1]['variable']
        .unique()
        .tolist())

    qstnlist = []
    for qcol in qcols:
        if qcol in idxlist or qcol in ctablist:
            continue
        elif df[qcol].nunique() > 6:
            continue
        else:
            qstnlist.append(qcol)

    df[sallqs] = df[sallqs].replace(['-99', -99], '0 - not selected')
    df = bucketize_numeric_cols(df, qumdf)
    df.replace(['-88', '-99', -88, -99], np.nan, inplace=True)

    crtdf1 = bulk_crosstabs(
        df=df,
        idxlist=idxlist,
        ctablist=ctablist,
        qstnlist=qstnlist,
        sallqs=sallqs,
        weight_col='PWEIGHT',
        critical_val=1.645)

    crtdf2 = bulk_crosstabs(
        df=df,
        idxlist=idxlist,
        ctablist=ctablist,
        qstnlist=qstnlist,
        sallqs=sallqs,
        weight_col='HWEIGHT',
        critical_val=1.645)

    ctabdf = pd.concat((crtdf1, crtdf2))
    ctabdf['ct_val'] = ctabdf['ct_val'].astype(str)

    ctabdf = ctabdf.merge(
        cmsdf[['cbsa_title', 'cbsa_fips']].drop_duplicates(),
        left_on='ct_val',
        right_on='cbsa_fips',
        how='left').iloc[:, :-1]

    ctabdf = ctabdf.merge(
        qumdf[['description_recode', 'variable']],
        left_on='q_var',
        right_on='variable',
        how='left').iloc[:, :-1]

    ctabdf['collection_dates'] = ctabdf.WEEK.map(load_census_weeks())
