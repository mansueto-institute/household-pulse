# -*- coding: utf-8 -*-
"""
Created on Saturday, 29th October 2022 10:21:12 am
===============================================================================
@filename:  fetch_and_cache.py
@author:    Manuel Martinez (manmart@uchicago.edu)
@project:   enter project name
@purpose:   enter purpose
===============================================================================
"""

import logging

import pandas as pd
from tqdm import tqdm
from tqdm.contrib.logging import logging_redirect_tqdm

from household_pulse.io import S3Storage
from household_pulse.preload_data.fetch_and_cache_utils import (
    get_dates,
    get_label_groupings,
    get_question_groupings,
    get_question_order,
    get_questions,
    get_xtab_labels,
    run_query,
)

logger = logging.getLogger(__name__)

MIN_WEEK_FILTER = 6


def get_meta():
    dates = get_dates()
    order = get_question_order()

    questions = get_questions(order, MIN_WEEK_FILTER)
    combined_xtabs = get_xtab_labels()
    question_groupings = get_question_groupings()
    label_groupings = get_label_groupings()

    return {
        "dates": dates,
        "xtabs": combined_xtabs,
        "questions": questions,
        "question_groupings": question_groupings,
        "label_groupings": label_groupings,
    }


def cache_queries(
    df: pd.DataFrame,
    dates,
    combined_xtabs,
    question_groupings,
    label_groupings,
):
    logger.info("Build query cache for the front-end")
    week_range = [int(dates.week.min()), int(dates.week.max())]
    xtabs = combined_xtabs["xtab_var"].unique()
    cached = {}

    for xtab in xtabs:
        xtab_labels = combined_xtabs[combined_xtabs.xtab_var == xtab]
        with logging_redirect_tqdm():
            for row in tqdm(
                question_groupings.itertuples(),
                total=len(question_groupings),
                desc=f"Working on xtab: {xtab}",
            ):

                var_group = row.variable_group
                response_labels = label_groupings[var_group]

                fnamepre = f"{row.variable_group}-{xtab}"

                for smoothed in (True, False):
                    if smoothed:
                        fname = "-".join((fnamepre, "SMOOTHED"))
                    else:
                        fname = fnamepre
                    data = run_query(
                        df=df,
                        question_group=row,
                        response_labels=response_labels,
                        xtab_labels=xtab_labels,
                        xtab=xtab,
                        week_range=week_range,
                        dates=dates,
                        smoothed=smoothed,
                    )

                    cached[".".join((fname, "json"))] = data

    return cached


def build_front_cache():

    df = S3Storage.download_smoothed_pulse()

    meta = get_meta()

    cache = cache_queries(
        df,
        meta["dates"],
        meta["xtabs"],
        meta["question_groupings"],
        meta["label_groupings"],
    )

    S3Storage.tar_and_upload(tarname="output_cache.tar.gz", files=cache)

    for fname, data in meta.items():
        if isinstance(data, pd.DataFrame):
            meta[fname] = meta[fname].to_json(orient="records")
    metacache = {f"{k}.json": v for k, v in meta.items()}
    S3Storage.tar_and_upload(tarname="output_meta.tar.gz", files=metacache)
