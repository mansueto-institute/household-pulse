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
import pandas as pd
from dask import dataframe as dd

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
        self.resdf = load_crosstab('response_mapping')

    def process_data(self) -> None:
        """
        Runs the entire pipeline from downloading data, right until before
        upload
        """
        cmsdf = self.cmsdf
        qumdf = self.qumdf

        self._download_data()
        self._parse_question_cols()
        self._bucketize_numeric_cols()
        self._reshape_long()
        self._drop_missing_responses()
        self._recode_values()
        # self._replace_labels()
        self._melt_to_ctab()

        ctabdf = self._aggregate()

        ctabdf['ct_val'] = ctabdf['ct_val'].astype(str)

        cmsdf['cbsa_fips'] = cmsdf['cbsa_fips'].astype(float).astype(str)
        ctabdf = ctabdf.merge(
            cmsdf[['cbsa_title', 'cbsa_fips']].drop_duplicates(),
            left_on='xtab_val',
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

    def _download_data(self) -> None:
        """
        downloads puf data and stores it into the class' state
        """
        self.df = download_puf(week=self.week)
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
            'variable']
        self.allqs = self.allqs[~self.allqs.str.contains('WEIGHT')]
        self.allqs = self.allqs.tolist()

        # finally we get the all the weight columns
        self.wgtcols = self.df.columns[self.df.columns.str.contains('WEIGHT')]
        self.wgtcols = self.wgtcols.tolist()

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

    def _reshape_long(self) -> None:
        """
        reshapes the raw microdata into a long format where each row is a
        question/response combination
        """
        self.longdf = self.df.melt(
            id_vars=self.meltvars + self.xtabs,
            value_vars=self.allqs,
            var_name='q_var',
            value_name='q_val')

    def _drop_missing_responses(self) -> None:
        """
        drops missing responses depending on the type of question (select all
        vs select one) since they are encoded differently.
        """
        longdf = self.longdf
        qumdf = self.qumdf
        longdf = longdf.merge(
            qumdf[['variable', 'question_type']].rename(
                columns={'variable': 'q_var'}
            ),
            how='left',
            on='q_var')

        # drop skipped select all
        longdf = longdf[
            ~((longdf['question_type'] == 'Select all') &
              (longdf['q_val'] == -88))]

        # drop skipped select one
        longdf = longdf[
            ~((longdf['question_type'] == 'Select one') &
              (longdf['q_val'].isin((-88, -99))))]

        # drop skipped yes/no
        longdf = longdf[
            ~((longdf['question_type'] == 'Yes / No') &
              (longdf['q_val'].isin((-88, -99))))]

        # drop skipped input value questions
        longdf = longdf[
            ~((longdf['question_type'] == 'Input value') &
              (longdf['q_val'].isnull()))]

        self.longdf = longdf

    def _melt_to_ctab(self) -> None:
        """
        duplicates each row in `longdf` for each of the crosstab values in
        self.xtabs
        """
        self.longdf = self.longdf.melt(
            id_vars=['SCRAM', 'q_var', 'q_val'],
            value_vars=self.xtabs,
            var_name='xtab_var',
            value_name='xtab_val'
        )

    def _recode_values(self) -> None:
        """
        recodes the numeric values from the original data into new categories
        (fewer) for each question in the data
        """
        resdf = self.resdf
        longdf = self.longdf

        resdf.rename(
            columns={'variable': 'q_var', 'value': 'q_val'},
            inplace=True)
        resdf['q_var'] = resdf['q_var'].astype(str)
        resdf['q_val'] = resdf['q_val'].astype(str)
        resdf['value_recode'] = resdf['value_recode'].astype(str)
        resdf['value_recode'] = resdf['value_recode'].str.split('.').str.get(0)
        longdf['q_var'] = longdf['q_var'].astype(str)
        longdf['q_val'] = longdf['q_val'].astype(str)
        longdf = longdf.merge(
            resdf[['q_var', 'q_val', 'value_recode']],
            how='left',
            on=['q_var', 'q_val'],
            copy=False)
        # coalesce old values and new values
        longdf['value_recode'] = longdf['value_recode'].where(
            longdf['value_recode'].notnull(),
            longdf['q_val'])
        longdf['q_val'] = longdf['value_recode']
        longdf.drop(columns='value_recode', inplace=True)
        self.longdf = longdf

    def _merge_labels(self) -> None:
        """
        merges both the question and response labels from the data dictionary
        """
        ctabdf = self.ctabdf
        resdf = self.resdf
        qumdf = self.qumdf

        ctabdf = ctabdf.merge(
            resdf[['q_var', 'q_val', 'label_recode']],
            how='left',
            on=['q_var', 'q_val'])
        ctabdf.rename(columns={'label_recode': 'q_val_label'}, inplace=True)

        ctabdf = ctabdf.merge(
            qumdf[['variable', 'question_clean']],
            how='left',
            left_on='q_var',
            right_on='variable',
            copy=False)

        ctabdf.drop(columns='variable', inplace=True)
        ctabdf.rename(columns={'question_clean': 'q_var_label'}, inplace=True)

        self.ctabdf = ctabdf

    def _aggregate_counts(self,
                          weight_type: str,
                          as_share: bool = False) -> pd.DataFrame:
        """
        aggregates all weights at the level of `longdf`. that is each
        question by each crosstab and sums the weights within each group.

        Args:
            weight_type (str): {'PWEIGHT', 'HWEIGHT'}
            as_share (bool): normalize the weights by the totals in each

        Returns:
            pd.DataFrame: aggregated weights with confidence intervals
        """
        allowed = {'PWEIGHT', 'HWEIGHT'}
        if weight_type not in allowed:
            raise ValueError(f'{weight_type} must be in {allowed}')

        # we fetch the passed weight type
        wgtdf = self.df.set_index('SCRAM').filter(like=weight_type)
        # we cast into a dask dataframe with a high number of partitions
        # so that we don't run out of memory because of the large number of
        # groups in the groupby operation
        ddf: dd = dd.from_pandas(self.longdf, npartitions=100)
        ddf = ddf.merge(wgtdf, how='left', on='SCRAM')

        sumdf: pd.DataFrame
        sumdf = (ddf
                 .groupby(['q_var', 'q_val', 'xtab_var', 'xtab_val'])
                 [wgtdf.columns]
                 .sum()
                 .compute())

        if as_share:
            totdf = (sumdf
                     .groupby(['q_var', 'xtab_var', 'xtab_val'])
                     .transform('sum'))
            sumdf = sumdf / totdf
            sumdf.columns = sumdf.columns.str.replace(
                weight_type,
                f'{weight_type}_SHARE')
            weight_type = f'{weight_type}_SHARE'

        self._get_conf_intervals(sumdf, weight_type)

        return sumdf

    def _aggregate(self) -> None:
        """
        Aggregates all weights at the crosstab level with their confidence
        intervals for each weight. For each weight type we also calculate the
        weights as shares with their respective confidence intervals.

        Returns:
            pd.DataFrame: aggregated xtabs for all questions and weight types
        """
        aggs = [
            ('PWEIGHT', False),
            ('PWEIGHT', True),
            ('HWEIGHT', False),
            ('HWEIGHT', True)]
        auxs = []
        for weight_type, as_share in aggs:
            auxs.append(self._aggregate_counts(weight_type, as_share))
        ctabdf = pd.concat(auxs, axis=1)
        ctabdf.columns = ctabdf.columns.str.lower()
        ctabdf = ctabdf.round(5)
        ctabdf.reset_index(inplace=True)
        self.ctabdf = ctabdf

    @staticmethod
    def _get_conf_intervals(df: pd.DataFrame,
                            weight_type: str,
                            cval: float = 1.645) -> None:
        """
        Generate the upper and lower confidence intervals of the passed
        weights using the replicate weights to calculate their standard error.
        It edits `df` in place by removing the replicate weights and adding
        the lower and upper confidence level bounds.

        Args:
            df (pd.DataFrame): a dataframe with aggregations as an index
                and weights as columns.
            weight_type (str): {'PWEIGHT', 'HWEIGHT'}
            cval (float): the critical value for the confidence interval.
                Defaults to 1.645,
        """
        # here we subtract the replicate weights from the main weight col
        # broadcasting across the columns
        diffdf = df.filter(regex=fr'{weight_type}.*\d{{1,2}}').sub(
            df[weight_type],
            axis=0)
        df[f'{weight_type}_SE'] = (diffdf.pow(2).sum(axis=1) * (4/80)).pow(1/2)
        df[f'{weight_type}_LOWER'] = (
            df[weight_type] - (cval * df[f'{weight_type}_SE']))
        df[f'{weight_type}_UPPER'] = (
            df[weight_type] + (cval * df[f'{weight_type}_SE']))

        # drop the replicate weights and the standard error column
        repcols = df.columns[df.columns.str.match(r'.*\d{1,2}')]
        df.drop(columns=repcols.tolist() + [f'{weight_type}_SE'], inplace=True)
