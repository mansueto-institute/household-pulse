from pathlib import Path

from etl import *

root = Path.cwd()

data_dir = root/"data"
data_dir.mkdir(exist_ok=True)

service_account_file = root/"credentials.json"

###### download housing data
raw_housing_datafile = data_dir/"puf_housing_data_raw.csv"
remapped_housing_datafile = data_dir/"puf_housing_data_remapped_labels.csv"
week, mode, header = check_housing_file_exists(raw_housing_datafile)

# download crosswalk mapping tables
question_mapping, response_mapping, county_metro_state = get_crosswalk_sheets(service_account_file)
label_recode_dict = get_label_recode_dict(response_mapping)

# downloads full set of weeks (from week 13 onwards) or just new weeks if housing_datafile already exists
r = True
while r:
    week_pad = str(week).zfill(2)
    data_str = data_url_str(week, week_pad)
    week_df = get_puf_data(data_str, week_pad)
    if week_df is None:
        r = False
    else:
        week_df.to_csv(raw_housing_datafile, mode=mode, header=header, index=False)
        week_df.replace(label_recode_dict).to_csv(remapped_housing_datafile, mode=mode, header=header, index=False)
        header, mode = (False, 'a')
        week += 1

print("finished downloading data")


###### generate crosstabs
# replicating Nico's crosstab example

# load full wide table
df = pd.read_csv(remapped_housing_datafile)
df['topline'] = 1

weight_var = 'PWEIGHT'
group_level = 'EST_MSA'
multi_index_vars =  ['SCRAM'] + [group_level] 
stacked_crosstab_vars = ['topline', 'EEDUC','EGENDER']
question_vars = ['RENTCUR', 'MORTCONF', 'EVICT']

df1 = df.melt(id_vars=multi_index_vars+stacked_crosstab_vars+[weight_var], 
                       value_vars=question_vars,
                       var_name='question_group',
                       value_name='question_val')

df2 = df1.melt(id_vars=multi_index_vars+['question_group','question_val']+[weight_var],
               value_vars=stacked_crosstab_vars,
               var_name='xtab_group',
               value_name='xtab_val')

crosstab = generate_crosstab(df2, group_level, weight_var)


# remove missing: data_df[-data_df.isin([-88,-99]).any(axis=1)]