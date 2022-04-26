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
from household_pulse.downloader import DataLoader
from household_pulse.mysql_wrapper import PulseSQL


class Pulse:
    meltvars = ('SCRAM', 'WEEK')
    xtabs = (
        'TOPLINE',
        'RRACE',
        'EEDUC',
        'EST_MSA',
        'INCOME',
        'EGENDER_EGENID_BIRTH',
        'TBIRTH_YEAR'
    )

    def __init__(self, week: int) -> None:
        """
        init method for the survey class

        Args:
            week (int): specifies which week to run the data for.
        """
        self.dl = DataLoader()
        self.week = week
        self.cmsdf = self.dl.load_gsheet('county_metro_state')
        self.qumdf = self.dl.load_gsheet('question_mapping')
        self.resdf = self.dl.load_gsheet('response_mapping')
        self.mapdf = self.dl.load_gsheet('numeric_mapping')
        self.ctabdf: pd.DataFrame

    def process_data(self) -> None:
        """
        Runs the entire pipeline from downloading data, right until before
        upload
        """
        self._download_data()
        self._coalesce_variables()
        self._parse_question_cols()
        self._bucketize_numeric_cols()
        self._coalesce_races()
        self._reshape_long()
        self._drop_missing_responses()
        self._recode_values()
        self._aggregate()
        self._merge_cbsa_info()
        self._reorganize_cols()

    def upload_data(self) -> None:
        """
        updates the data in the RDS table. by update we mean, delete the old
        data for this particular week if any, and then update. if there is no
        old data for this particular week, it does not delete anything.
        """
        if not hasattr(self, 'ctabdf'):
            raise AttributeError(
                'this should be only run after running the '
                '.process_pulse_data() method')
        sql = PulseSQL()
        sql.update_values(table='pulse', df=self.ctabdf)

        if self.week not in sql.get_collection_weeks():
            sql.update_collection_dates()

        sql.close_connection()

    def _download_data(self) -> None:
        """
        downloads puf data and stores it into the class' state
        """
        self.df = self.dl.load_week(week=self.week)
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
        mapdf = self.mapdf
        numcols = mapdf['variable'].unique()

        for col in numcols:
            if col not in df.columns:
                continue
            auxdf = mapdf[mapdf['variable'] == col]
            bins = pd.IntervalIndex.from_arrays(
                left=auxdf['min_value'],
                right=auxdf['max_value'],
                closed='both',
            )
            bucketized: pd.Series = pd.cut(df[col], bins=bins)
            if bucketized.isnull().sum() > 0:
                allowed = {-88, -99}
                unmapped = set(df[col][bucketized.isnull()])
                if len(allowed - unmapped) == 0:
                    continue
                else:
                    raise ValueError(
                        f'Unmapped values bining col {col}, {unmapped}')
            df[col] = bucketized.cat.codes

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
        self.longdf['q_val'] = self.longdf['q_val'].astype(int)

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
              (longdf['q_val'].isin((-88, -99))))]

        longdf = longdf[~longdf['INCOME'].isin({-88, -99})]

        for col in self.xtabs:
            if len({-88, -99} & set(longdf[col].unique())) > 0:
                raise ValueError(f'xtab_var {col} has some -99 or -88 values')

        longdf.drop(columns='question_type', inplace=True)
        self.longdf = longdf

    def _recode_values(self) -> None:
        """
        recodes the numeric values from the original data into new categories
        (fewer) for each question in the data
        """
        resdf = self.resdf
        longdf = self.longdf

        auxdf = resdf.drop_duplicates(subset=['variable_recode', 'value'])

        longdf = longdf.merge(
            auxdf[['variable_recode', 'value', 'value_recode']],
            how='left',
            left_on=['q_var', 'q_val'],
            right_on=['variable_recode', 'value'],
            copy=False,
            validate='m:1')
        # coalesce old values and new values
        longdf['value_recode'] = longdf['value_recode'].combine_first(
            longdf['q_val'])
        longdf['q_val'] = longdf['value_recode']
        longdf.drop(
            columns=['variable_recode', 'value', 'value_recode'],
            inplace=True)

        # recode xtabs separately
        for xtab in self.xtabs:
            auxdf = resdf[resdf['variable_recode'] == xtab].copy()
            auxdf['value'] = auxdf['value'].astype(longdf[xtab].dtype)
            auxdf['value_recode'] = auxdf['value_recode'].astype(
                longdf[xtab].dtype)

            valuemap = dict(zip(auxdf['value'], auxdf['value_recode']))
            longdf[xtab] = longdf[xtab].replace(valuemap)

        longdf['q_val'] = longdf['q_val'].astype(int)

        self.longdf = longdf

    def _coalesce_variables(self) -> None:
        """
        coalesces variables that represent the same question but that have
        been edited across the survey waves so that they represent the same
        question in the final time series processed data.
        """
        qumdf = self.qumdf
        auxdf = qumdf[qumdf['variable_recode'].notnull()]
        recodemap = dict(zip(auxdf['variable'], auxdf['variable_recode']))
        self.df = self.df.rename(columns=recodemap)

    def _coalesce_races(self) -> None:
        """
        Coalesces the `RRACE` and `RHISPANIC` variables into a single variable
        called `RRACE` that has a new category for hispanic/latino.
        """
        self.df['RRACE'] = self.df['RRACE'].where(
            cond=self.df['RHISPANIC'] == 1,
            other=5)

    def _merge_cbsa_info(self) -> None:
        """
        Merges core-based statistical area information to the crosstab that
        uses this information.
        """
        ctabdf = self.ctabdf
        cmsdf = self.cmsdf

        cmsdf.drop_duplicates(subset='cbsa_fips', inplace=True)

        ctabdf = ctabdf.merge(
            cmsdf[['cbsa_title', 'cbsa_fips']],
            how='left',
            left_on='xtab_val',
            right_on='cbsa_fips')

        ctabdf.drop(columns='cbsa_fips', inplace=True)
        self.ctabdf = ctabdf

    def _reorganize_cols(self) -> None:
        """
        reorganize columns before upload for easier inspection
        """
        ctabdf = self.ctabdf
        wgtcols = ctabdf.columns[ctabdf.columns.str.contains('weight')]
        ctabdf['week'] = self.week
        colorder = [
            'week',
            'xtab_var',
            'xtab_val',
            'cbsa_title',
            'q_var',
            'q_val',
        ]
        colorder.extend(wgtcols.tolist())
        assert ctabdf.columns.isin(colorder).all(), 'missing a column'
        ctabdf = ctabdf[colorder]
        ctabdf.sort_values(
            by=['xtab_var', 'xtab_val', 'q_var', 'q_val'],
            inplace=True)
        self.ctabdf = ctabdf

    def _aggregate_counts(self, weight_type: str) -> pd.DataFrame:
        """
        aggregates all weights at the level of `longdf`. that is each
        question by each crosstab and sums the weights within each group.

        Args:
            weight_type (str): {'PWEIGHT', 'HWEIGHT'}

        Returns:
            pd.DataFrame: aggregated weights with confidence intervals
        """
        allowed = {'PWEIGHT', 'HWEIGHT'}
        if weight_type not in allowed:
            raise ValueError(f'{weight_type} must be in {allowed}')

        # we fetch the passed weight type
        wgtdf = self.df.set_index('SCRAM').filter(like=weight_type)
        wgtcols = wgtdf.columns

        df = self.longdf.merge(wgtdf, on='SCRAM')

        auxs = []
        for xtab_var in self.xtabs:
            auxdf = df.groupby([xtab_var, 'q_var', 'q_val'])[wgtcols].sum()
            self._get_conf_intervals(auxdf, weight_type)

            # we can get the confidence intervals as shares after aggregating
            sumdf = auxdf.groupby(['q_var', xtab_var]).transform('sum')
            shadf = auxdf / sumdf
            shadf.columns = shadf.columns + '_SHARE'
            xtabdf = auxdf.merge(
                shadf,
                how='left',
                left_index=True,
                right_index=True)

            # here we reformat some data to append the crosstabs together
            xtabdf.reset_index(inplace=True)
            xtabdf['xtab_var'] = xtab_var
            xtabdf['xtab_val'] = xtabdf[xtab_var]
            xtabdf.drop(columns=xtab_var, inplace=True)
            auxs.append(xtabdf)

        resdf = pd.concat(auxs)
        resdf.set_index(
            ['xtab_var', 'xtab_val', 'q_var', 'q_val'],
            inplace=True)

        return resdf

    def _aggregate(self) -> None:
        """
        Aggregates all weights at the crosstab level with their confidence
        intervals for each weight. For each weight type we also calculate the
        weights as shares with their respective confidence intervals.

        Returns:
            pd.DataFrame: aggregated xtabs for all questions and weight types
        """
        weights = ('PWEIGHT', 'HWEIGHT')
        auxs = []
        for weight_type in weights:
            auxs.append(self._aggregate_counts(weight_type))
        ctabdf = pd.concat(auxs, axis=1)
        ctabdf.columns = ctabdf.columns.str.lower()
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
