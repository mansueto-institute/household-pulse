
import json

import pandas as pd
from household_pulse.mysql_wrapper import PulseSQL
from household_pulse.preload_data.fetch_and_cache_utils import (
    compress_folder, get_dates, get_label_groupings, get_question_groupings,
    get_question_order, get_questions, get_xtab_labels, run_query,
    upload_folder)
from tqdm import tqdm

# PARAMS
MIN_WEEK_FILTER = 6


def get_meta(pulsesql: PulseSQL):
    dates = get_dates(pulsesql=pulsesql)
    order = get_question_order(pulsesql=pulsesql)

    questions = get_questions(order, MIN_WEEK_FILTER)
    combined_xtabs = get_xtab_labels()
    question_groupings = get_question_groupings()
    label_groupings = get_label_groupings()

    return {
        'dates': dates,
        'xtabs': combined_xtabs,
        'questions': questions,
        'question_groupings': question_groupings,
        'label_groupings': label_groupings,
    }


def cache_queries(df: pd.DataFrame,
                  dates,
                  combined_xtabs,
                  question_groupings,
                  label_groupings):

    week_range = [dates.week.min(), dates.week.max()]
    xtabs = combined_xtabs['xtab_var'].unique()
    cached = {}

    for xtab in xtabs:
        xtab_labels = combined_xtabs[combined_xtabs.xtab_var == xtab]
        for row in tqdm(
                question_groupings.itertuples(),
                total=len(question_groupings),
                desc=f'Working on xtab: {xtab}'):

            var_group = row.variable_group
            response_labels = label_groupings[var_group]

            fnamepre = f'{row.variable_group}-{xtab}'

            for smoothed in (True, False):
                if smoothed:
                    fname = '-'.join((fnamepre, 'SMOOTHED'))
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
                    smoothed=smoothed)

                cached['.'.join((fname, 'json'))] = data

    return cached


def compress_and_upload():
    print('Compressing files...')
    compress_folder(path.join(".", "meta"), path.join(
        ".", "meta", "output_meta.tar.gz"))
    compress_folder(path.join(".", "cache"), path.join(
        ".", "cache", "output_cache.tar.gz"))
    print('Uploading to s3...')
    upload_folder('household-pulse', path.join(".", "meta",
                  "output_meta.tar.gz"), 'frontend_cache')
    upload_folder('household-pulse', path.join(".", "cache",
                  "output_cache.tar.gz"), 'frontend_cache')
    print('Uploaded.')


def fetch_meta_and_cache_data():
    pulsesql = PulseSQL()

    df = pulsesql.get_pulse_with_smoothed()

    meta = get_meta(pulsesql=pulsesql)
    pulsesql.close_connection()

    cache = cache_queries(
        df,
        meta['dates'],
        meta['xtabs'],
        meta['question_groupings'],
        meta['label_groupings'])
    compress_and_upload()


if __name__ == "__main__":
    fetch_meta_and_cache_data()
