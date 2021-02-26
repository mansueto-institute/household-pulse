from pathlib import Path

from etl import *

# to be replaced with Ryan's functions
def get_crosstabs(vars_dict):
    df1 = df.melt(id_vars = vars_dict['multi_index_vars'] + vars_dict['stacked_crosstab_vars'] + vars_dict['weight_var'], 
                  value_vars = vars_dict['question_vars'],
                  var_name = 'question_group',
                  value_name = 'question_val')
    df2 = df1.melt(id_vars = vars_dict['multi_index_vars'] + ['question_group','question_val'] + vars_dict['weight_var'],
                   value_vars = vars_dict['stacked_crosstab_vars'],
                   var_name = 'xtab_group',
                   value_name = 'xtab_val')
    return generate_crosstab(df2, vars_dict['group_level'], vars_dict['weight_var'])


if __name__=="__main__":

    root = Path.cwd()

    data_dir = root/"data"
    data_dir.mkdir(exist_ok=True)

    service_account_file = root/"credentials.json"

    ###### download housing data
    raw_housing_datafile = data_dir/"puf_housing_data_raw.csv"
    remapped_housing_datafile = data_dir/"puf_housing_data_remapped_labels.csv"
    week, mode, header, csv_cols = check_housing_file_exists(raw_housing_datafile)

    # download crosswalk mapping tables
    question_mapping, response_mapping, county_metro_state = get_crosswalk_sheets(service_account_file)
    label_recode_dict = get_label_recode_dict(response_mapping)

    cols_file = data_dir/"list_cols.txt"
    if cols_file.exists():
        cols = cols_file.read_text().split(" ")
    else:
        cols = csv_cols

    # downloads full set of weeks (from week 13 onwards) or just new weeks if housing_datafile already exists
    r = True
    while r:
        week_pad = str(week).zfill(2)
        data_str = data_url_str(week, week_pad)
        week_df = get_puf_data(data_str, week_pad)
        if week_df is None:
            r = False
        else:
            # making the process robust to variable changes in the data
            if mode == 'a':
                missing_vars = list(set(cols) - set(week_df.columns))
                new_vars = list(set(week_df.columns) - set(cols))
                print("Week {} dataset, new variables: {}".format(week, new_vars))
                print("Week {} dataset, missing variables: {}".format(week, missing_vars))
                cols = list(set(cols).intersection(set(week_df.columns)))
                if missing_vars:
                    week_df[missing_vars] = None
                week_df = week_df[csv_cols]
            week_df.to_csv(raw_housing_datafile, mode=mode, header=header, index=False)
            week_df.replace(label_recode_dict).to_csv(remapped_housing_datafile, mode=mode, header=header, index=False)
            header, mode = (False, 'a')
            week += 1
    print("Finished downloading data")

    # save list of cols present in all weeks of data:
    cols_file.write_text(' '.join(cols))

    ###### generate crosstabs
    df = pd.read_csv(remapped_housing_datafile, usecols=cols)
    df['TOPLINE'] = 1

    # to be replaced by Ryan's code
    crosstab_vars_dict = {
        'weight_var': 'PWEIGHT',
        'group_level': 'EST_MSA',
        'multi_index_vars': ['SCRAM', 'EST_MSA'],
        'stacked_crosstab_vars': ['TOPLINE', 'EEDUC','EGENDER'],
        'question_vars': ['RENTCUR', 'MORTCONF', 'EVICT']
    }

    crosstab = get_crosstabs(crosstab_vars_dict)


