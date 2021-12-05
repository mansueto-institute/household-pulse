# -*- coding: utf-8 -*-
"""
Created on Saturday, 23rd October 2021 4:54:40 pm
===============================================================================
@filename:  pulse.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   household pulse
@purpose:   this module is the main worker class for the project. it allows
            the processing of the household pulse survey from the census
            and subsequent uploading to the SQL database that serves the front
            end.
===============================================================================
"""
import numpy as np
import pandas as pd

from household_pulse.loaders import (NUMERIC_COL_BUCKETS, download_puf,
                                     load_census_weeks, load_crosstab)
from household_pulse.mysql_wrapper import PulseSQL


class Pulse:
    meltvars = ('SCRAM', 'WEEK')
    xtabs = ('TOPLINE', 'RRACE', 'EEDUC', 'EST_MSA')

    def __init__(self, week: int) -> None:
        """
        init method for the survey class

        Args:
            week (int): specifies which week to run the data for.
        """
        self.week = week
        self.cmsdf = load_crosstab('county_metro_state')
        self.qumdf = load_crosstab('question_mapping')

    def process_data(self) -> None:
        """
        Runs the entire pipeline from downloading data, right until before
        upload
        """
        cmsdf = self.cmsdf
        qumdf = self.qumdf

        self._download_data()
        self._extract_replicate_wgts()
        self._parse_question_cols()
        self._bucketize_numeric_cols()
        self._reshape_long()
        self._drop_missing_responses()
        # self._replace_labels()

        crtdf1 = self._bulk_crosstabs(weight_col='PWEIGHT', critical_val=1.645)
        crtdf2 = self._bulk_crosstabs(weight_col='HWEIGHT', critical_val=1.645)

        ctabdf = pd.concat((crtdf1, crtdf2))
        ctabdf['ct_val'] = ctabdf['ct_val'].astype(str)

        cmsdf['cbsa_fips'] = cmsdf['cbsa_fips'].astype(float).astype(str)
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

        self.ctabdf = ctabdf.copy()

    def upload_data(self) -> None:
        """
        updates the data in the RDS table. by update we mean, delete the old
        data for this particular week if any, and then update. if there is no
        old data for this particular week, it does not delete anything.
        """
        if not hasattr(self, 'ctabdf'):
            raise ValueError(
                'this should be only run after running the '
                '.process_pulse_data() method')
        sql = PulseSQL()
        sql.update_values(table='pulse', df=self.ctabdf)
        sql.close_connection()

    @property
    def _recode_map(self) -> dict:
        """
        Converts the response_mapping df from the
        household_pulse_data_dictionary google sheet into dict to recode labels

        Returns:
            dict: {variable: {value: label_recode}}
        """
        resdf = load_crosstab('response_mapping')
        resdf = resdf[resdf['do_not_join'] == 0].copy()
        resdf['value'] = resdf['value'].astype('float64')
        result: dict[str, dict] = {}
        for row in resdf.itertuples():
            if row.variable not in result.keys():
                result[row.variable] = {}
            result[row.variable][row.value] = row.label_recode
        return result

    def _download_data(self) -> None:
        """
        downloads puf data and stores it into the class' state
        """
        self.df = download_puf(week=self.week)

    def _replace_labels(self) -> None:
        """
        replaces all values in the survey data with the labels from gsheets
        """
        self.df = self.df.replace(self._recode_map).copy()
        self.df[self.sallqs] = self.df[self.sallqs].replace(
            ['-99', -99],
            'Question seen but category not selected')
        self.df['TOPLINE'] = 1

    def _parse_question_cols(self) -> None:
        """
        parses the types of questions in the data (select all vs select one)
        and stores the list of questions of each type for further use
        downstream
        """
        df = self.df
        qumdf = self.qumdf

        # first we get the select one question
        soneqs: pd.Series = qumdf.loc[
            qumdf['question_type'].isin(['Select one', 'Yes / No']),
            'variable']
        soneqs = soneqs[soneqs.isin(df.columns)]
        # here we just drop grouping variables from actual questions
        soneqs = soneqs[~soneqs.isin(self.xtabs + self.meltvars)]

        # next we get the select all questions
        sallqs: pd.Series = qumdf.loc[
            qumdf['question_type'] == 'Select all',
            'variable']
        sallqs = sallqs[sallqs.isin(df.columns)]

        self.soneqs = soneqs.tolist()
        self.sallqs = sallqs.tolist()
        self.allqs = qumdf.loc[
            qumdf['variable'].isin(self.df.columns),
            'variable'].tolist()

    def _bucketize_numeric_cols(self) -> pd.DataFrame:
        """
        Bucketize numeric columns using the buckets specified above in
        NUMERIC_COL_BUCKETS dict

        Returns:
            pd.DataFrame: with the numeric columns bucketized
        """
        df = self.df
        qumdf = load_crosstab('question_mapping')
        num_cols = qumdf[qumdf['question_type'] == 'Input value']['variable']
        for col in num_cols:
            if col in df.columns:
                df[col] = pd.cut(
                    df[col],
                    bins=NUMERIC_COL_BUCKETS[col]['bins'],
                    labels=NUMERIC_COL_BUCKETS[col]['labels'],
                    right=False)

    def _extract_replicate_wgts(self) -> None:
        """
        separates all replicate weights from microdata into its own dataframe
        """
        if hasattr(self, '_wgtdf'):
            pass
        else:
            self._wgtdf = (self.df
                           .set_index('SCRAM')
                           .filter(regex=r'[A-z]WEIGHT\d{1,2}'))
            self.df.drop(columns=self._wgtdf.columns, inplace=True)

    def _reshape_long(self) -> None:
        """
        reshapes the raw microdata into a long format where each row is a
        question/response combination
        """
        self.df['TOPLINE'] = 1
        self.longdf = self.df.melt(
            id_vars=self.meltvars + self.xtabs + ('PWEIGHT', 'HWEIGHT'),
            value_vars=self.allqs,
            var_name='variable',
            value_name='question_val')

    def _drop_missing_responses(self) -> None:
        """
        drops missing responses depending on the type of question (select all
        vs select one) since they are encoded differently.
        """
        longdf = self.longdf
        qumdf = self.qumdf
        longdf = longdf.merge(
            qumdf[['variable', 'question_type']],
            how='left',
            on='variable')

        # drop skipped select all
        longdf = longdf[
            ~((longdf['question_type'] == 'Select all') &
              (longdf['question_val'] == -88))]

        # drop skipped select one
        longdf = longdf[
            ~((longdf['question_type'] == 'Select one') &
              (longdf['question_val'].isin((-88, -99))))]

        # drop skipped yes/no
        longdf = longdf[
            ~((longdf['question_type'] == 'Yes / No') &
              (longdf['question_val'].isin((-88, -99))))]

        # drop skipped input value questions
        longdf = longdf[
            ~((longdf['question_type'] == 'Input value') &
              (longdf['question_val'].isnull()))]

        self.longdf = longdf

    def _freq_crosstab(self,
                       df: pd.DataFrame,
                       col_list: list[str],
                       weight_col: str,
                       critical_val: float = 1.0) -> pd.DataFrame:
        """
        sums across each passed column in col_list and then calculates
        the standard errors for those estimates.

        Args:
            df (pd.DataFrame): pulse data
            col_list (list[str]): the list of columns to group by
            weight_col (str): weight column to use
            critical_val (int, optional): the critical value for
                the confidence intervals. Defaults to 1.

        Returns:
            pd.DataFrame: a dataframe with the grouped estimates of the
                means with their corresponding standard errors
        """
        w_cols = df.columns[df.columns.str.contains(weight_col)]
        pt_estimates = df.groupby(col_list)[w_cols].sum()
        pt_estimates['std_err'] = self._get_std_err(pt_estimates, weight_col)
        pt_estimates['mrgn_err'] = pt_estimates['std_err'] * critical_val
        pt_estimates.rename(columns={weight_col: 'value'}, inplace=True)
        return pt_estimates[['value', 'std_err', 'mrgn_err']].reset_index()

    def _full_crosstab(self,
                       df: pd.DataFrame,
                       col_list: list[str],
                       weight_col: str,
                       abstract: list[str],
                       critical_val: float = 1.0) -> pd.DataFrame:
        """
        performs a frequency crosstab at the col_list level and the
        abstract level

        Args:
            df (pd.DataFrame): temp dataframe
            col_list (list[str]): question columns
            weight_col (str): weight column
            abstract (list[str]): abstract level (aggregation level)
            critical_val (int, optional): critical value for confidence
                intervals. Defaults to 1.

        Returns:
            pd.DataFrame: dataframe that returns the ratio between the
                base level and the abstract level for each of the response
                proportions.
        """
        detail = self._freq_crosstab(df, col_list, weight_col, critical_val)
        top = self._freq_crosstab(df, abstract, weight_col, critical_val)
        rv = detail.merge(
            right=top,
            how='left',
            on=abstract,
            suffixes=('_full', '_demo'))
        rv['proportions'] = rv['value_full'] / rv['value_demo']
        return rv

    def _bulk_crosstabs(self,
                        weight_col: str = 'PWEIGHT',
                        critical_val: float = 1) -> pd.DataFrame:
        """
        performs crosstabs on each of the questions of interest

        Args:
            weight_col (str, optional): the weight column to use for
                estimating the standard errors. Defaults to 'PWEIGHT'.
            critical_val (float, optional): the critical value to use when
                estimating standard errors. Defaults to 1.

        Returns:
            pd.DataFrame: a long format dataframe with each combination of
                question and answer as a row.
        """
        df = self.df
        auxs = []
        input_df = df.copy()
        for ct in Pulse.ctablist:
            for q in self.soneqs:
                col_list = Pulse.idxlist + [ct, q]
                abstract = Pulse.idxlist + [ct]
                tempdf = input_df.dropna(axis=0, how='any', subset=col_list)
                if q in self.sallqs:
                    all_q = [x for x in self.sallqs if x.startswith(q[:-1])]
                    sallmask = (
                        tempdf[all_q] ==
                        'Question seen but category not selected').all(axis=1)
                    tempdf = tempdf[~sallmask]
                auxdf = self._full_crosstab(
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

    @staticmethod
    def _get_std_err(df: pd.DataFrame, weight_col: str) -> list[float]:
        """
        Calculate standard error of dataframe

        Args:
            weight_col (str): specify whether person ('PWEIGHT') or household
                ('HWEIGHT') weight

        Returns:
            list[float]: the standard errors
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
