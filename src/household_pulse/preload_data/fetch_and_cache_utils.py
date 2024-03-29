import logging
import tarfile
from glob import glob
from typing import Optional

import pandas as pd
import numpy as np

from household_pulse.io import S3Storage

logger = logging.getLogger(__name__)

BASE_SHEET_URL = (
    "https://docs.google.com/spreadsheets/d/e/"
    "2PACX-1vQ3z_mffa5UDBgny2E6mrQcbrXcrebM22sjz54GZ6CwGpwlvSCkGUXZdutNQgoBbg_"
    "ztZhLKImi9Ju6/pub?output=csv"
)
SHEET_MAPPING = {
    "question_mapping": 34639438,
    "response_mapping": 1561671071,
    "numeric_mapping": 1572193173,
    "xtab_labels": 1576761217,
    "county_metro_state": 974836931,
}

# CONVENIENCE


def write_json(json, fpath):
    """
    Write a json file to disk.

    Returns:
        void
    """
    with open(fpath, "w") as outfile:
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


def reconcile(str1: Optional[str], str2: Optional[str]):
    """
    Returns the first string if it is not None, otherwise returns the second
    string

    Returns:
        string
    """
    if isinstance(str1, str):
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
    files = glob(input_path + "/*")
    for file in files:
        tar.add(file, arcname=file.split("/")[-1])
    tar.close()


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
    temp_dict = {"week": int(group.iloc[0].week)}

    for i in range(0, len(group)):
        temp_dict[str(group.iloc[i].q_val)] = group.iloc[i].proportion

    curr_group = list(group.q_val.unique())
    curr_labels = list(labels.keys())

    if len(curr_labels) != len(curr_group):
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
    temp_dict = {"week": int(week)}
    for key in labels.keys():
        temp_dict[key] = None
    return temp_dict


def run_query(
    df: pd.DataFrame,
    question_group,
    response_labels,
    xtab_labels,
    xtab,
    week_range,
    dates,
    smoothed=False,
):
    """
    Run a query for a given question group, response labels, xtab labels, xtab,
    week range, and dates.

    Returns:
        Frontend compatible json schema
        {
            'qid': question id,
            'ct': cross tab,
            'labels': dictionary of labels ({'1': 'Yes', '2': 'No'}),
            'ctLabels': dictionary of xtab lables ({
                '1': 'high income',
                '2': 'low income'}
                ),
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
    if smoothed:
        value_col = "pweight_share_smoothed"
    else:
        value_col = "pweight_share"

    return_dict = {
        "qid": question_group.variable_group,
        "ct": xtab,
        "labels": response_labels,
        "ctLabels": (xtab_labels[["xtab_val", "xtab_label"]]).to_json(
            orient="records"
        ),
        "response": [],
        "available_weeks": [],
    }

    resdf = df[
        (df["xtab_var"] == xtab) & (df["q_var"].isin(question_group.variables))
    ]

    available_weeks = resdf.week.unique().astype(int).tolist()
    return_dict["available_weeks"] = available_weeks

    for resdftab in resdf.xtab_val.unique():
        temp_resdf = resdf[resdf.xtab_val == resdftab].copy()
        temp_resdf["week"] = temp_resdf.week.astype(int).tolist()
        temp_resdf["proportion"] = temp_resdf[value_col].astype(float)
        temp_resdf = temp_resdf.sort_values(by=["week"])
        grouped_dict = temp_resdf.groupby("week").apply(
            lambda resdf: group_to_json(resdf, response_labels)
        )
        values = []
        for week in range(week_range[0], week_range[1] + 1):
            if week in grouped_dict.index:
                grouped_results = grouped_dict.loc[week]
                grouped_res_fix = {}
                for k, v in grouped_results.items():
                    if isinstance(k, np.integer):
                        k = str(k)
                    grouped_res_fix[k] = v
                values.append(grouped_res_fix)
                values[week - 1]["dateRange"] = dates[
                    dates.week == week
                ].dates.values[0]
            else:
                values.append(generate_dummy_obj(week, response_labels))
                values[week - 1]["dateRange"] = dates[
                    dates.week == week
                ].dates.values[0]
        try:
            return_dict["response"].append(
                {
                    "ct": (
                        xtab_labels[xtab_labels.xtab_val == resdftab]
                    ).xtab_label.values[0],
                    "values": values,
                }
            )
        except KeyError:
            print(f"Missing resdftab {resdftab} from label dict")
            print(xtab_labels)
        except IndexError as error:
            error_msg = (
                f"Missing resdftab {resdftab} in week {week} on xtab "
                f"{xtab}"
            )
            raise IndexError(error_msg) from error

    return return_dict


# FETCHERS AND DATA PARSING
def get_dates():
    df = pd.DataFrame.from_dict(
        S3Storage().get_collection_dates(), orient="index"
    )
    df.reset_index(names="week", inplace=True)
    df["dates"] = (
        df["start_date"].astype(str) + " to " + df["end_date"].astype(str)
    )
    df["date"] = df["end_date"].astype(str)
    return df[["week", "date", "dates"]]


def get_xtab_labels():
    xtab_labels = get_sheet("xtab_labels").fillna("")
    numeric_xtab = get_sheet("numeric_mapping").fillna("")
    numeric_xtab = numeric_xtab.query("variable == 'TBIRTH_YEAR'")[
        ["variable", "bin", "label"]
    ]
    numeric_xtab.columns = ["xtab_var", "xtab_val", "xtab_label"]

    topline_xtab = pd.DataFrame(
        [{"xtab_var": "TOPLINE", "xtab_val": 1, "xtab_label": "Summary"}]
    )

    msa_xtabs = get_sheet("county_metro_state")[
        ["cbsa_fips", "cbsa_title"]
    ].fillna("")
    msa_xtabs.columns = ["xtab_val", "xtab_label"]
    msa_xtabs["xtab_var"] = "EST_MSA"

    text_xtab = get_sheet("response_mapping").fillna("")
    text_xtab["label"] = text_xtab.apply(
        lambda x: reconcile(x["label_recode"], x["label"]), axis=1
    )
    text_xtab["variable"] = text_xtab.apply(
        lambda x: reconcile(x["variable_recode"], x["variable"]), axis=1
    )
    text_xtab["value"] = text_xtab.apply(
        lambda x: reconcile(x["value_recode"], x["value"]), axis=1
    )
    text_xtab = text_xtab[
        text_xtab.variable_recode.isin(xtab_labels.query_value)
    ][["variable", "value", "label"]]
    text_xtab.columns = ["xtab_var", "xtab_val", "xtab_label"]

    combined_xtabs = pd.concat(
        [numeric_xtab, topline_xtab, msa_xtabs, text_xtab]
    )
    return combined_xtabs


def get_question_order():
    result_data = S3Storage().download_all(file_type="processed")
    df = result_data.groupby(["q_var", "week"]).size().reset_index()
    df.drop(columns=0, inplace=True)

    count = df.groupby("q_var").count()
    count.columns = ["count_of_weeks"]
    most_recent = (
        df.sort_values("week", ascending=False).groupby("q_var").first()
    )
    most_recent.columns = ["most_recent_week"]

    combined = count.merge(most_recent, on="q_var").reset_index()
    return combined


def get_questions(order_df, MIN_WEEK_FILTER):
    columns = [
        "variable_recode_final",
        "variable_group_recode",
        "variable_group",
        "question_clean",
        "exclude",
        "topic_area",
        "subtopic_area",
        "drop_question",
        "question_type",
    ]
    questions = get_sheet("question_mapping")[columns].fillna("")

    questions = questions.merge(
        order_df, left_on="variable_recode_final", right_on="q_var", how="left"
    )
    questions.sort_values(
        by=["count_of_weeks", "most_recent_week"],
        ascending=[False, False],
        inplace=True,
    )
    questions = questions[questions.count_of_weeks > MIN_WEEK_FILTER]
    questions.drop(
        columns=[
            "q_var",
            "count_of_weeks",
            "most_recent_week",
            "variable_recode_final",
        ],
        inplace=True,
    )

    questions = questions.fillna("")
    questions["variable"] = questions.apply(
        lambda x: reconcile(x["variable_group_recode"], x["variable_group"]),
        axis=1,
    )
    questions.rename(
        columns={
            "question_clean": "question",
            "exclude": "exclude",
            "topic_area": "topic",
            "subtopic_area": "subtopic",
        },
        inplace=True,
    )

    questions["isMultiQuestion"] = questions["question_type"].apply(
        lambda x: x != "Select all"
    )
    questions = questions[
        (questions["exclude"] != 1) & (questions["drop_question"] != 1)
    ]
    questions.drop(
        columns=[
            "exclude",
            "drop_question",
            "variable_group_recode",
            "variable_group",
        ],
        inplace=True,
    )
    return questions.drop_duplicates()


def handle_question_kind(question_type):
    if question_type == "Select all":
        return "single"
    else:
        return "multi"  # eg. multiple simultaneous time series


def get_question_groupings():
    columns = [
        "variable_recode_final",
        "variable_group_recode",
        "variable_group",
        "question_type",
    ]
    questions = get_sheet("question_mapping")[columns].fillna("")

    questions["kind"] = questions["question_type"].apply(
        lambda x: handle_question_kind(x)
    )
    questions["variable_group"] = questions.apply(
        lambda x: reconcile(x["variable_group_recode"], x["variable_group"]),
        axis=1,
    )
    questions = questions[["variable_recode_final", "variable_group", "kind"]]
    questions = (
        questions.groupby(["variable_group", "kind"])["variable_recode_final"]
        .unique()
        .reset_index()
    )
    questions.rename(
        columns={"variable_recode_final": "variables"}, inplace=True
    )

    return questions


def get_label_groupings():
    columns = [
        "variable_recode_final",
        "variable_group_recode",
        "variable_group",
    ]
    questions = get_sheet("question_mapping")[columns].fillna("")
    questions["variable_group"] = questions.apply(
        lambda x: reconcile(x["variable_group_recode"], x["variable_group"]),
        axis=1,
    )
    questions = questions[["variable_recode_final", "variable_group"]]

    response_mapping = get_sheet("response_mapping")[
        [
            "variable",
            "variable_recode",
            "label",
            "label_recode",
            "value",
            "value_recode",
        ]
    ].fillna("")
    response_mapping["label"] = response_mapping.apply(
        lambda x: reconcile(x["label_recode"], x["label"]), axis=1
    )
    response_mapping["variable"] = response_mapping.apply(
        lambda x: reconcile(x["variable_recode"], x["variable"]), axis=1
    )
    response_mapping["value"] = response_mapping.apply(
        lambda x: reconcile(x["value_recode"], x["value"]), axis=1
    )
    response_mapping = response_mapping[["variable", "value", "label"]]

    numeric_mapping = get_sheet("numeric_mapping")[
        ["variable", "bin", "label"]
    ].fillna("")
    numeric_mapping.columns = ["variable", "value", "label"]

    combined_mappings = pd.concat([response_mapping, numeric_mapping])

    merged_mappings = combined_mappings.merge(
        questions,
        how="left",
        left_on="variable",
        right_on="variable_recode_final",
    )
    merged_mappings = merged_mappings[
        (merged_mappings.value != -99) & (merged_mappings.value != -88)
    ]

    combined_labels = {}

    for variable_group in merged_mappings.variable_group.unique():
        temp_obj = {}
        sub_labels = merged_mappings[
            merged_mappings.variable_group == variable_group
        ][["value", "label"]]
        for i in range(0, len(sub_labels)):
            temp_obj[
                f"{int(sub_labels.value.iloc[i])}"
            ] = f"{sub_labels.label.iloc[i]}"
        combined_labels[variable_group] = temp_obj

    return combined_labels
