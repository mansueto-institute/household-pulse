from pathlib import Path

from etl import *

root = Path.cwd()
data_dir = root/"data"

data_dir.mkdir(exist_ok=True)

###### download housing data
# downloads full set of weeks (from week 13 onwards) or just new weeks if housing_datafile already exists
housing_datafile = data_dir/"puf_housing_data_raw.csv"
week, mode, header = check_housing_file_exists(housing_datafile)
download_housing_data(housing_datafile, week, mode, header)
print("finished downloading data")

###### load full wide table
df = pd.read_csv(housing_datafile)

###### map in response labels into wide table
service_account_file = root/"credentials.json"

question_mapping, response_mapping, county_metro_state = get_crosswalk_sheets(service_account_file)
label_recode_dict = get_label_recode_dict(response_mapping)

df_replaced = df.replace(label_recode_dict)
df_replaced['topline'] = 1
# save mapped labels version as csv
df_replaced.to_csv(data_dir/"puf_housing_data_remapped_labels.csv")

###### generate crosstabs
# replicating Nico's crosstab example
weight_var = 'PWEIGHT'
group_level = 'EST_MSA'
multi_index_vars =  ['SCRAM'] + [group_level] 
stacked_crosstab_vars = ['topline', 'EEDUC','EGENDER']
question_vars = ['RENTCUR', 'MORTCONF', 'EVICT']

df1 = df_replaced.melt(id_vars=multi_index_vars+stacked_crosstab_vars+[weight_var], 
                       value_vars=question_vars,
                       var_name='question_group',
                       value_name='question_val')

df2 = df1.melt(id_vars=multi_index_vars+['question_group','question_val']+[weight_var],
               value_vars=stacked_crosstab_vars,
               var_name='xtab_group',
               value_name='xtab_val')

crosstab = generate_crosstab(df2, group_level, weight_var)

