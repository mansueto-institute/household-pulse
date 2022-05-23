
import json
from os import environ, mkdir, path

import sqlalchemy as db

from fetch_and_cache_utils import (compress_folder, df_to_json, get_dates,
                                   get_label_groupings, get_question_groupings,
                                   get_question_order, get_questions,
                                   get_xtab_labels, run_query, upload_folder,
                                   write_json)

# PARAMS
RDS_HOSTNAME = environ("RDS_HOSTNAME")
RDS_USERNAME = environ("RDS_USERNAME")
RDS_PASSWORD = environ("RDS_PASSWORD")
RDS_PORT = environ("RDS_PORT")
RDS_DATABASE = environ("RDS_DATABASE")
MIN_WEEK_FILTER = 6


def make_directories():
    try:
        path = path.join(".", "meta")
        mkdir(path)
    except:
        print('Meta dir exists...')

    try:
        path = path.join(".", "cache")
        mkdir(path)
    except:
        print('Cache dir exists...')


def initiate_connection():
    # intitiate connection
    engine = db.create_engine(
        f'mysql://{RDS_USERNAME}:{RDS_PASSWORD}@{RDS_HOSTNAME}:{RDS_PORT}/{RDS_DATABASE}')
    metadata = db.MetaData()
    connection = engine.connect()
    print('Connected to mysql database')
    return {
        'engine': engine,
        'metadata': metadata,
        'connection': connection
    }


def get_meta(engine, metadata, connection):
    print('Fetching meta information...')
    ##### metadata #####
    # # # Meta- date range and labels
    dates = get_dates(engine, connection, metadata)
    # format example
    # {
    #     "week":44,
    #     "date":"2022-04-11",
    #     "dates":"2022-03-30 to 2022-04-11"
    # }
    df_to_json(dates, path.join('.', 'meta', 'dates.json'))
    print('dates...')

    # # #  Meta- xtab labels
    combined_xtabs = get_xtab_labels()
    # format example
    # {
    #     "xtab_var":"TBIRTH_YEAR",
    #     "xtab_val":0,
    #     "xtab_label":"18 to 29"
    # }
    df_to_json(combined_xtabs, path.join('.', 'meta', 'xtab.json'))
    print('xtabs...')

    # # get questions
    # Get order based on most recent week and number of weeks present
    order = get_question_order(engine, connection, metadata)
    # format example
    # {
    #     "question":"During the last 7 days, did you..?",
    #     "topic":"Food security",
    #     "subtopic":"Food assistance",
    #     "question_type":"Yes \/ No",
    #     "variable":"FREEFOOD",
    #     "isMultiQuestion":true
    # }
    questions = get_questions(order, MIN_WEEK_FILTER)
    df_to_json(questions, path.join('.', 'meta', 'questions.json'))
    print('questions...')

    # # get question grouping
    # For fetching, the grouping of the question group with relevant variables
    # format example
    # {
    #     "variable_group":"WHYCHNGD",
    #     "kind":"single",
    #     "variables":"[\"WHYCHNGD1\", \"WHYCHNGD2\"...]"
    # }
    question_groupings = get_question_groupings()
    df_to_json(question_groupings, path.join(
        '.', 'meta', 'questionGrouping.json'))
    print('groupings...')

    # # get label grouping
    # Outer tagged label groupings
    # {
    #      "ACTIVITY": {"1": "Yes", "2": "No"},
    #      ...
    # }
    label_groupings = get_label_groupings()
    write_json(json.dumps(label_groupings), path.join(
        '.', 'meta', 'labelGrouping.json'))
    print('labels...')
    print('Done fetching meta.')
    return {
        'dates': dates,
        'xtabs': combined_xtabs,
        'question_groupings': question_groupings,
        'label_groupings': label_groupings,
    }


def cache_queries(engine, metadata, connection, dates, combined_xtabs, question_groupings, label_groupings):
    print('Caching queries...')
    ##### data caching #####
    week_range = [dates.week.min(), dates.week.max()]
    xtabs = combined_xtabs['xtab_var'].unique()

    for xtab in xtabs:
        print(f'Querying xtab {xtab}')
        xtab_labels = combined_xtabs[combined_xtabs.xtab_var == xtab]
        for i in range(0, len(question_groupings)):
            question_group = question_groupings.iloc[i]
            try:
                response_labels = label_groupings[question_group.variable_group]
            except:
                print(f"Missing labels for {question_group.variable_group}")

            try:
                data = run_query(
                    question_group,
                    response_labels,
                    xtab_labels,
                    xtab,
                    week_range,
                    dates,
                    engine,
                    metadata,
                    connection
                )
                write_json(json.dumps(data), path.join(
                    '.', 'cache', f'{question_group.variable_group}-{xtab}.json'))
            except:
                print(f'Error with {question_group.variable_group}')

            try:
                data = run_query(
                    question_group,
                    response_labels,
                    xtab_labels,
                    xtab,
                    week_range,
                    dates,
                    engine,
                    metadata,
                    connection,
                    smoothed=True
                )
                write_json(json.dumps(data), path.join(
                    '.', 'cache', f'{question_group.variable_group}-{xtab}-SMOOTHED.json'))
            except:
                print(f'Error with {question_group.variable_group} smoothed')


def cleanup_connection(connection):
    connection.close()


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
    make_directories()
    # Connection to use to RDS
    connection_obj = initiate_connection()
    engine = connection_obj['engine']
    metadata = connection_obj['metadata']
    connection = connection_obj['connection']
    # Meta info for queries
    meta_obj = get_meta(engine, metadata, connection)
    dates = meta_obj['dates']
    combined_xtabs = meta_obj['xtabs']
    question_groupings = meta_obj['question_groupings']
    label_groupings = meta_obj['label_groupings']
    # Cache data
    cache_queries(
        engine,
        metadata,
        connection,
        dates,
        combined_xtabs,
        question_groupings,
        label_groupings
    )
    # Wrap it up.
    cleanup_connection(connection)
    compress_and_upload()


if __name__ == "__main__":
    fetch_meta_and_cache_data()
