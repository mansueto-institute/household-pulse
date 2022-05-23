import json
import tarfile
import warnings
from datetime import datetime
from os import environ, path

import boto3
import pandas as pd
import sqlalchemy as db
from botocore.exceptions import ClientError

warnings.simplefilter(action='ignore', category=FutureWarning)

BASE_SHEET_URL = environ("GOOGLE_SHEET_BASE")
SHEET_MAPPING = {
    "question_mapping": 34639438,
    "response_mapping": 1561671071,
    "numeric_mapping": 1572193173,
    "xtab_labels": 1576761217,
    "county_metro_state": 974836931,
}

ACCESS_KEY = environ("S3_ACCESS_KEY")
SECRET_KEY = environ("S3_SECRET_KEY")

# CONVENIENCE


def write_json(json, fpath):
    """
    Write a json file to disk.

    Returns:
        void
    """
    with open(fpath, 'w') as outfile:
        outfile.write(json)


def df_to_json(df, fpath: str):
    """
    Convert a pandas dataframe to a json string, then writes it.

    Returns:
        void
    """

    json_df = df.to_json(orient="records")
    write_json(json_df, fpath)


def get_sheet(sheet_name: str):
    """
    Fetches a google sheet with with the sheet name provided

    Returns:
        dataframe
    """
    return pd.read_csv(f"{BASE_SHEET_URL}&gid={SHEET_MAPPING[sheet_name]}")


def reconcile(str1: any, str2: any):
    """
    Returns the first string if it is not None, otherwise returns the second string

    Returns:
        string
    """
    if type(str1) == str:
        if len(str1) > 0:
            return str1
        else:
            return str2
    else:
        if str1 is None:
            return str2
        else:
            return str1


def compress_folder(input_path: str, output_path: str):
    """
    Compress a folder into a tar.gz file.

    Returns:
        void
    """
    tar = tarfile.open(output_path, "w:gz")
    tar.add(input_path, arcname="TarName")
    tar.close()


def upload_folder(bucket, path_to_file, prefix):
    s3 = boto3.client(
        's3',
        aws_access_key_id=ACCESS_KEY,
        aws_secret_access_key=SECRET_KEY
    )

    try:
        response = s3.upload_file(path_to_file, bucket, prefix+path_to_file)
    except ClientError as e:
        logging.error(e)
        return False
    return True


# DB UTILS

def group_to_json(group, labels):
    """
    Convert a grouped df to packed json, and fill in any missing labels.
    This convenience method is most frequently applied as a lambda fn on a
    grouped DF

    Returns:
        Packed json
        {
            "label": value or null if missing
        }
    """
    temp_dict = {
        "week": int(group.iloc[0].week)
    }

    for i in range(0, len(group)):
        temp_dict[group.iloc[i].q_val] = group.iloc[i].proportion

    curr_group = list(group.q_val.unique())
    curr_labels = list(labels.keys())

    if (len(curr_labels) != len(curr_group)):
        for i in range(0, len(curr_labels)):
            if curr_labels[i] not in temp_dict:
                temp_dict[curr_labels[i]] = None

    return temp_dict


def generate_dummy_obj(week, labels):
    """
    Generate a dummy object for a given week, and fill in relevant labels.

    Returns:
        Packed json
        {
            "week": week,
            "label": null
        }
    """
    temp_dict = {
        "week": int(week)
    }
    for key in labels.keys():
        temp_dict[key] = None
    return temp_dict


def run_query(question_group, response_labels, xtab_labels, xtab, week_range, dates, engine, metadata, connection, smoothed=False):
    """
    Run a query for a given question group, response labels, xtab labels, xtab,
    week range, and dates.

    Returns:
        Frontend compatible json schema
        {
            'qid': question id,
            'ct': cross tab,
            'labels': dictionary of labels ({'1': 'Yes', '2': 'No'}),
            'ctLabels': dictionary of xtab lables ({'1': 'high income', '2': 'low income'}),
            'response': an array of data responses, for each cross tab value
                {
                    'ct': cross tab value,
                    'value': any array of weekly values
                        [
                            {
                                'week': week,
                                'label': value,
                                'label2': value2,
                            }
                        ]
                }
            'available_weeks': array of weeks available, as ints
        }
    """
    variables = json.loads(question_group['variables'])
    if smoothed:
        table_name = "smoothed"
    else:
        table_name = "pulse"

    if smoothed:
        value_col = "pweight_share_smoothed"
    else:
        value_col = "pweight_share"

    PULSE = db.Table(table_name, metadata, autoload=True, autoload_with=engine)
    query = db.select(
        [
            PULSE.c.week,
            # PULSE.c.q_var,
            PULSE.c.q_val,
            # PULSE.c.xtab_var,
            PULSE.c.xtab_val,
            PULSE.c[value_col]
        ]
    ) \
        .where(PULSE.c.q_var.in_(variables)) \
        .where(PULSE.c.xtab_var == xtab) \
        .order_by(PULSE.c.week)

    result_proxy = connection.execute(query)
    result_keys = result_proxy.keys()
    result_data = result_proxy.fetchall()

    return_dict = {
        'qid': question_group.variable_group,
        'ct': xtab,
        'labels': response_labels,
        'ctLabels': xtab_labels[['xtab_val', 'xtab_label']].to_json(orient="records"),
        "response": [],
        "available_weeks": []
    }

    df = pd.DataFrame(result_data)

    if len(df) == 0:
        return return_dict

    df.columns = list(result_keys)
    available_weeks = df.week.unique().astype(int).tolist()
    return_dict['available_weeks'] = available_weeks

    for xtab in df.xtab_val.unique():
        temp_df = df[df.xtab_val == xtab]
        temp_df['week'] = temp_df.week.astype(int)
        temp_df['proportion'] = temp_df[value_col].astype(float)
        temp_df = temp_df.sort_values(by=['week'])
        grouped_dict = temp_df.groupby('week').apply(
            lambda x: group_to_json(x, response_labels))
        values = []
        for week in range(week_range[0], week_range[1]+1):
            if week in grouped_dict.index:
                values.append(grouped_dict.loc[week])
                values[week-1]['dateRange'] = dates[dates.week ==
                                                    week].dates.values[0]
            else:
                values.append(generate_dummy_obj(week, response_labels))
                values[week-1]['dateRange'] = dates[dates.week ==
                                                    week].dates.values[0]
        try:
            return_dict['response'].append({
                "ct": xtab_labels[xtab_labels.xtab_val == xtab].xtab_label.values[0],
                "values": values
            })
        except:
            print(f"Missing xtab {xtab} from label dict")
            print(xtab_labels)

    return return_dict


# FETCHERS AND DATA PARSING
def get_dates(engine, connection, metadata):
    collection_dates = db.Table(
        'collection_dates', metadata, autoload=True, autoload_with=engine)
    query = db.select([collection_dates]).order_by(
        db.desc(collection_dates.columns.week))
    # query
    result_proxy = connection.execute(query)
    result_keys = result_proxy.keys()
    result_data = result_proxy.fetchall()
    # cleanup and output
    df = pd.DataFrame(result_data)
    df.columns = list(result_keys)
    df['dates'] = df.apply(
        lambda x: f"{datetime.strftime(x['start_date'], '%Y-%m-%d')} to {datetime.strftime(x['end_date'], '%Y-%m-%d')}", axis=1)
    df['date'] = df['end_date'].apply(
        lambda x:  datetime.strftime(x, '%Y-%m-%d'))
    return df[['week', 'date', 'dates']]


def get_xtab_labels():
    xtab_labels = get_sheet("xtab_labels").fillna('')
    df_to_json(xtab_labels, '../meta/xtab_labels.json')

    numeric_xtab = get_sheet("numeric_mapping").fillna('')
    numeric_xtab = numeric_xtab.query("variable == 'TBIRTH_YEAR'")[
        ['variable', 'bin', 'label']]
    numeric_xtab.columns = ['xtab_var', 'xtab_val', 'xtab_label']

    topline_xtab = pd.DataFrame(
        [{'xtab_var': "TOPLINE", 'xtab_val': 1, 'xtab_label': "Summary"}])

    msa_xtabs = get_sheet("county_metro_state")[
        ["cbsa_fips", "cbsa_title"]].fillna('')
    msa_xtabs.columns = ['xtab_val', 'xtab_label']
    msa_xtabs['xtab_var'] = "EST_MSA"

    text_xtab = get_sheet("response_mapping").fillna('')
    text_xtab = text_xtab[text_xtab.variable.isin(xtab_labels.query_value)][[
        "variable", "value", "label"]]
    text_xtab.columns = ["xtab_var", "xtab_val", "xtab_label"]

    combined_xtabs = pd.concat(
        [numeric_xtab, topline_xtab, msa_xtabs, text_xtab])
    return combined_xtabs


def get_question_order(engine, connection, metadata):
    PULSE = db.Table('pulse', metadata, autoload=True, autoload_with=engine)
    query = db.select([
        PULSE.c.q_var,
        PULSE.c.week
    ]).order_by(db.desc(PULSE.columns.week)).distinct()
    result_proxy = connection.execute(query)
    result_data = result_proxy.fetchall()

    df = pd.DataFrame(result_data)
    df.columns = list(result_proxy.keys())
    count = df.groupby("q_var").count()
    count.columns = ["count_of_weeks"]
    most_recent = df.sort_values(
        "week", ascending=False).groupby("q_var").first()
    most_recent.columns = ["most_recent_week"]

    combined = count.merge(most_recent, on="q_var").reset_index()
    return combined


def get_questions(order_df, MIN_WEEK_FILTER):
    columns = ['variable_recode_final', 'variable_group_recode', 'variable_group',
               'question_clean', 'exclude', 'topic_area', 'subtopic_area', 'drop_question', 'question_type']
    questions = get_sheet("question_mapping")[columns].fillna('')

    questions = questions.merge(
        order_df, left_on="variable_recode_final", right_on="q_var", how="left")
    questions.sort_values(by=["count_of_weeks", "most_recent_week"], ascending=[
                          False, False], inplace=True)
    questions = questions[questions.count_of_weeks > MIN_WEEK_FILTER]
    questions.drop(
        columns=["q_var", "count_of_weeks",
                 "most_recent_week", "variable_recode_final"],
        inplace=True
    )

    questions = questions.fillna('')
    questions['variable'] = questions.apply(lambda x: reconcile(
        x['variable_group_recode'], x['variable_group']), axis=1)
    questions.rename(columns={
        'question_clean': 'question',
        'exclude': 'exclude',
        'topic_area': 'topic',
        'subtopic_area': 'subtopic',
    }, inplace=True)

    questions['isMultiQuestion'] = questions['question_type'].apply(
        lambda x: x != "Select all")
    questions = questions[(questions['exclude'] != 1) &
                          (questions['drop_question'] != 1)]
    questions.drop(
        columns=["exclude", "drop_question",
                 "variable_group_recode", "variable_group"],
        inplace=True
    )
    return questions.drop_duplicates()


def handle_question_kind(question_type):
    if question_type == "Select all":
        return "single"
    else:
        return "multi"  # eg. multiple simultaneous time series


def get_question_groupings():
    columns = ["variable_recode_final", 'variable_group_recode',
               'variable_group', 'question_type']
    questions = get_sheet("question_mapping")[columns].fillna('')

    questions['kind'] = questions['question_type'].apply(
        lambda x: handle_question_kind(x))
    questions['variable_group'] = questions.apply(lambda x: reconcile(
        x['variable_group_recode'], x['variable_group']), axis=1)
    questions = questions[["variable_recode_final", "variable_group", "kind"]]
    questions['variables'] = questions.apply(lambda x: json.dumps(list(
        questions[questions.variable_group == x.variable_group].variable_recode_final)), axis=1)
    questions = questions[["variable_group", "kind", "variables"]]
    return questions.drop_duplicates()


def get_label_groupings():
    columns = ["variable_recode_final",
               'variable_group_recode', 'variable_group']
    questions = get_sheet("question_mapping")[columns].fillna('')
    questions['variable_group'] = questions.apply(lambda x: reconcile(
        x['variable_group_recode'], x['variable_group']), axis=1)
    questions = questions[["variable_recode_final", "variable_group"]]

    response_mapping = get_sheet("response_mapping")[
        ["variable", "variable_recode", "label", "label_recode", "value", "value_recode"]].fillna('')
    response_mapping["label"] = response_mapping.apply(
        lambda x: reconcile(x['label_recode'], x['label']), axis=1)
    response_mapping["variable"] = response_mapping.apply(
        lambda x: reconcile(x['variable_recode'], x['variable']), axis=1)
    response_mapping["value"] = response_mapping.apply(
        lambda x: reconcile(x['value_recode'], x['value']), axis=1)
    response_mapping = response_mapping[["variable", "value", "label"]]

    numeric_mapping = get_sheet("numeric_mapping")[
        ["variable", "bin", "label"]].fillna('')
    numeric_mapping.columns = ["variable", "value", "label"]

    combined_mappings = pd.concat([response_mapping, numeric_mapping])

    merged_mappings = combined_mappings.merge(
        questions, how="left", left_on="variable", right_on="variable_recode_final")
    merged_mappings = merged_mappings[(
        merged_mappings.value != -99) & (merged_mappings.value != -88)]

    combined_labels = {}

    for variable_group in merged_mappings.variable_group.unique():
        temp_obj = {}
        sub_labels = merged_mappings[merged_mappings.variable_group == variable_group][[
            "value", "label"]]
        for i in range(0, len(sub_labels)):
            temp_obj[f"{int(sub_labels.value.iloc[i])}"] = f"{sub_labels.label.iloc[i]}"
        combined_labels[variable_group] = temp_obj

    return combined_labels
