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
import logging

import numpy as np
import pandas as pd
from botocore.exceptions import ClientError

from household_pulse.io import Census, S3Storage, load_gsheet
from household_pulse.io.base import IO

logger = logging.getLogger(__name__)


class Pulse(IO):
    """
    The pulse class represents a single week's (wave) worth of data.
    """

    meltvars = ("SCRAM", "WEEK")
    xtabs = (
        "TOPLINE",
        "RRACE",
        "EEDUC",
        "EST_MSA",
        "INCOME",
        "EGENDER_EGENID_BIRTH",
        "TBIRTH_YEAR",
    )

    def __init__(self, week: int) -> None:
        """
        init method for the survey class

        Args:
            week (int): specifies which week to run the data for.
        """
        super().__init__(week=week)
        self.cmsdf: pd.DataFrame
        self.qumdf: pd.DataFrame
        self.resdf: pd.DataFrame
        self.mapdf: pd.DataFrame
        self.ctabdf: pd.DataFrame
        self.df: pd.DataFrame
        self.s3 = S3Storage()
        self.census = Census(week=self.week)

    def process_data(self) -> None:
        """
        Runs the entire pipeline from downloading data, right until before
        upload
        """
        self.download_data()
        self._coalesce_variables()
        self._parse_question_cols()
        self._calculate_ages()
        self._bucketize_numeric_cols()
        self._coalesce_races()
        self._reshape_long()
        self._drop_missing_responses()
        self._recode_values()
        self._aggregate()
        self._merge_cbsa_info()
        self._reorganize_cols()

    def download_data(self) -> None:
        """
        downloads puf data and stores it into the class' state
        """

        try:
            df = self.s3.download_parquet(
                key=f"raw-files/pulse-{self.week_str}.parquet",
            )
        except ClientError as e:
            if e.response["Error"]["Code"] == "NoSuchKey":
                logger.info("Data not found in S3. Downloading from census")
                df = self.census.download()
                self.s3.upload_parquet(
                    key=f"raw-files/pulse-{self.week_str}.parquet", df=df
                )
            else:
                logger.error(e)
                raise e

        df["TOPLINE"] = 1
        self.df = df

    def upload_data(self) -> None:
        """
        updates the data in the RDS table. by update we mean, delete the old
        data for this particular week if any, and then update. if there is no
        old data for this particular week, it does not delete anything.
        """
        if not hasattr(self, "ctabdf"):
            raise AttributeError(
                "this should be only run after running the "
                ".process_pulse_data() method"
            )
        self.s3.upload_parquet(
            key=f"processed-files/pulse-{self.week_str}.parquet",
            df=self.ctabdf,
        )

        if self.week not in self.s3.get_available_weeks(file_type="processed"):
            self.s3.put_collection_dates()

    def _calculate_ages(self) -> None:
        """
        calculates the ages of the respondents based on the birth year and
        the date at which the survey was implemented
        """
        logger.info("Calculating ages based on collection dates")

        try:
            dates = self.s3.get_collection_dates()[self.week]
        except KeyError:
            self.s3.put_collection_dates()
            dates = self.s3.get_collection_dates()[self.week]

        df = self.df
        df["TBIRTH_YEAR"] = dates["end_date"].year - df["TBIRTH_YEAR"]
        df["TBIRTH_YEAR"].clip(lower=18, inplace=True)

    def _parse_question_cols(self) -> None:
        """
        parses the types of questions in the data (select all vs select one)
        and stores the list of questions of each type for further use
        downstream
        """
        logger.info("Parsing question categories")
        df = self.df
        qumdf = load_gsheet("question_mapping")

        # first we get the select one question
        soneqs: pd.Series = qumdf.loc[
            qumdf["question_type"].isin(["Select one", "Yes / No"]),
            "variable_recode_final",
        ]
        soneqs = soneqs[soneqs.isin(df.columns)]
        # here we just drop grouping variables from actual questions
        soneqs = soneqs[~soneqs.isin(self.xtabs + self.meltvars)]

        # next we get the select all questions
        sallqs: pd.Series = qumdf.loc[
            qumdf["question_type"] == "Select all", "variable_recode_final"
        ]
        sallqs = sallqs[sallqs.isin(df.columns)]

        self.soneqs = soneqs.tolist()
        self.sallqs = sallqs.tolist()
        self.allqs = qumdf.loc[
            qumdf["variable"].isin(self.df.columns), "variable_recode_final"
        ]
        self.allqs = self.allqs[~self.allqs.str.contains("WEIGHT")]
        self.allqs = self.allqs.tolist()

        # finally we get the all the weight columns
        self.wgtcols = self.df.columns[self.df.columns.str.contains("WEIGHT")]
        self.wgtcols = self.wgtcols.tolist()

    def _bucketize_numeric_cols(self) -> pd.DataFrame:
        """
        Bucketize numeric columns using the buckets specified above in
        NUMERIC_COL_BUCKETS dict

        Returns:
            pd.DataFrame: with the numeric columns bucketized
        """
        df = self.df
        mapdf = load_gsheet("numeric_mapping")
        numcols = mapdf["variable"].unique()

        for col in numcols:
            if col not in df.columns:
                continue
            logger.info("Bucketizing numerical column %s", col)
            auxdf = mapdf[mapdf["variable"] == col]
            bins = pd.IntervalIndex.from_arrays(
                left=auxdf["min_value"],
                right=auxdf["max_value"] + 1,
                closed="left",
            )
            bucketized: pd.Series = pd.cut(df[col], bins=bins)
            if bucketized.isnull().sum() > 0:
                allowed = {-88, -99}
                unmapped = set(df[col][bucketized.isnull()].astype(int))
                if len(unmapped - allowed) != 0:
                    raise ValueError(
                        f"Unmapped values bining col {col}, {unmapped}"
                    )
            # map the category codes if not missing, otherwise keep the missing
            df[col] = np.where(
                bucketized.cat.codes == -1, df[col], bucketized.cat.codes
            )

    def _reshape_long(self) -> None:
        """
        reshapes the raw microdata into a long format where each row is a
        question/response combination
        """
        logger.info("Reshaping all responses from wide to long")
        self.longdf = self.df.melt(
            id_vars=self.meltvars + self.xtabs,
            value_vars=self.allqs,
            var_name="q_var",
            value_name="q_val",
        )
        self.longdf.dropna(subset="q_val", inplace=True)
        self.longdf["q_val"] = self.longdf["q_val"].astype(int)

    def _drop_missing_responses(self) -> None:
        """
        drops missing responses depending on the type of question (select all
        vs select one) since they are encoded differently.
        """
        logger.info("Dropping missing or empty responses")
        longdf = self.longdf
        qumdf = load_gsheet("question_mapping")
        longdf = longdf.merge(
            qumdf[["variable", "question_type"]].rename(
                columns={"variable": "q_var"}
            ),
            how="left",
            on="q_var",
        )

        # drop skipped select all
        longdf = longdf[
            ~(
                (longdf["question_type"] == "Select all")
                & (longdf["q_val"] == -88)
            )
        ]

        # drop skipped select one
        longdf = longdf[
            ~(
                (longdf["question_type"] == "Select one")
                & (longdf["q_val"].isin((-88, -99)))
            )
        ]

        # drop skipped yes/no
        longdf = longdf[
            ~(
                (longdf["question_type"] == "Yes / No")
                & (longdf["q_val"].isin((-88, -99)))
            )
        ]

        # drop skipped input value questions
        longdf = longdf[
            ~(
                (longdf["question_type"] == "Input value")
                & (longdf["q_val"].isin((-88, -99)))
            )
        ]

        longdf = longdf[~longdf["INCOME"].isin({-88, -99})]

        longdf.drop(columns="question_type", inplace=True)
        self.longdf = longdf

    def _recode_values(self) -> None:
        """
        recodes the numeric values from the original data into new categories
        (fewer) for each question in the data
        """
        logger.info("Recoding values based on mapping from Google Sheets")
        resdf = load_gsheet("response_mapping")
        longdf = self.longdf

        auxdf = resdf.drop_duplicates(subset=["variable_recode", "value"])

        longdf = longdf.merge(
            auxdf[["variable_recode", "value", "value_recode"]],
            how="left",
            left_on=["q_var", "q_val"],
            right_on=["variable_recode", "value"],
            copy=False,
            validate="m:1",
        )
        # coalesce old values and new values
        longdf["value_recode"] = longdf["value_recode"].combine_first(
            longdf["q_val"]
        )
        longdf["q_val"] = longdf["value_recode"]
        longdf.drop(
            columns=["variable_recode", "value", "value_recode"], inplace=True
        )

        # recode xtabs separately
        for xtab in self.xtabs:
            auxdf = resdf[resdf["variable_recode"] == xtab].copy()
            auxdf["value"] = auxdf["value"].astype(longdf[xtab].dtype)
            auxdf["value_recode"] = auxdf["value_recode"].astype(
                longdf[xtab].dtype
            )

            valuemap = dict(zip(auxdf["value"], auxdf["value_recode"]))
            longdf[xtab] = longdf[xtab].replace(valuemap)

        longdf["q_val"] = longdf["q_val"].astype(int)

        self.longdf = longdf

    def _coalesce_variables(self) -> None:
        """
        coalesces variables that represent the same question but that have
        been edited across the survey waves so that they represent the same
        question in the final time series processed data.
        """
        logger.info("Coalescing variables that change over time.")
        qumdf = load_gsheet("question_mapping")
        auxdf = qumdf[qumdf["variable_recode"].notnull()]
        recodemap = dict(zip(auxdf["variable"], auxdf["variable_recode"]))
        self.df = self.df.rename(columns=recodemap)

    def _coalesce_races(self) -> None:
        """
        Coalesces the `RRACE` and `RHISPANIC` variables into a single variable
        called `RRACE` that has a new category for hispanic/latino.
        """
        self.df["RRACE"] = self.df["RRACE"].where(
            cond=self.df["RHISPANIC"] == 1, other=5
        )

    def _merge_cbsa_info(self) -> None:
        """
        Merges core-based statistical area information to the crosstab that
        uses this information.
        """
        logger.info("Merging CBSA info to the responses.")
        ctabdf = self.ctabdf
        cmsdf = load_gsheet("country_metro_state")

        cmsdf.drop_duplicates(subset="cbsa_fips", inplace=True)

        ctabdf = ctabdf.merge(
            cmsdf[["cbsa_title", "cbsa_fips"]],
            how="left",
            left_on="xtab_val",
            right_on="cbsa_fips",
        )

        ctabdf.drop(columns="cbsa_fips", inplace=True)
        self.ctabdf = ctabdf

    def _reorganize_cols(self) -> None:
        """
        reorganize columns before upload for easier inspection
        """
        logger.info("Reordering columns for final output.")
        ctabdf = self.ctabdf
        wgtcols = ctabdf.columns[ctabdf.columns.str.contains("weight")]
        ctabdf["week"] = self.week
        colorder = [
            "week",
            "xtab_var",
            "xtab_val",
            "cbsa_title",
            "q_var",
            "q_val",
        ]
        colorder.extend(wgtcols.tolist())
        assert ctabdf.columns.isin(colorder).all(), "missing a column"
        ctabdf = ctabdf[colorder]
        ctabdf.sort_values(
            by=["xtab_var", "xtab_val", "q_var", "q_val"], inplace=True
        )
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
        # we fetch the passed weight type
        wgtdf = self.df.set_index("SCRAM").filter(like=weight_type)
        wgtcols = wgtdf.columns

        df = self.longdf.merge(wgtdf, on="SCRAM")

        auxs = []
        for xtab_var in self.xtabs:
            logger.info(
                "Aggregating weights types %s for the %s xtab_var",
                weight_type,
                xtab_var,
            )
            auxdf = df.groupby([xtab_var, "q_var", "q_val"])[wgtcols].sum()
            self._get_conf_intervals(auxdf, weight_type)

            # we can get the confidence intervals as shares after aggregating
            sumdf = auxdf.groupby(["q_var", xtab_var]).transform("sum")
            shadf = auxdf / sumdf
            shadf.columns = shadf.columns + "_SHARE"
            xtabdf = auxdf.merge(
                shadf, how="left", left_index=True, right_index=True
            )

            # here we reformat some data to append the crosstabs together
            xtabdf.reset_index(inplace=True)
            xtabdf["xtab_var"] = xtab_var
            xtabdf["xtab_val"] = xtabdf[xtab_var]
            xtabdf.drop(columns=xtab_var, inplace=True)
            auxs.append(xtabdf)

        resdf = pd.concat(auxs)
        resdf.set_index(
            ["xtab_var", "xtab_val", "q_var", "q_val"], inplace=True
        )

        return resdf

    def _aggregate(self) -> None:
        """
        Aggregates all weights at the crosstab level with their confidence
        intervals for each weight. For each weight type we also calculate the
        weights as shares with their respective confidence intervals.

        Returns:
            pd.DataFrame: aggregated xtabs for all questions and weight types
        """
        weights = ("PWEIGHT", "HWEIGHT")
        auxs = []
        for weight_type in weights:
            auxs.append(self._aggregate_counts(weight_type))
        ctabdf = pd.concat(auxs, axis=1)
        ctabdf.columns = ctabdf.columns.str.lower()
        ctabdf.reset_index(inplace=True)
        self.ctabdf = ctabdf

    @staticmethod
    def _get_conf_intervals(
        df: pd.DataFrame, weight_type: str, cval: float = 1.645
    ) -> None:
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
        logger.info("Calculating confidence intervals for each question")
        # here we subtract the replicate weights from the main weight col
        # broadcasting across the columns
        diffdf = df.filter(regex=rf"{weight_type}.*\d{{1,2}}").sub(
            df[weight_type], axis=0
        )
        df[f"{weight_type}_SE"] = (diffdf.pow(2).sum(axis=1) * (4 / 80)).pow(
            1 / 2
        )
        df[f"{weight_type}_LOWER"] = df[weight_type] - (
            cval * df[f"{weight_type}_SE"]
        )
        df[f"{weight_type}_UPPER"] = df[weight_type] + (
            cval * df[f"{weight_type}_SE"]
        )

        # drop the replicate weights and the standard error column
        repcols = df.columns[df.columns.str.match(r".*\d{1,2}")]
        df.drop(columns=repcols.tolist() + [f"{weight_type}_SE"], inplace=True)
