from pathlib import Path

from etl import *

root = Path.cwd()
data_dir = root/"data"

data_dir.mkdir(exist_ok=True)

service_account_file = root/"credentials.json"

data = []
r = True
week = 13

while r:
    week_pad = str(week).zfill(2)
    data_str = data_url_str(week, week_pad)
    week_df = get_puf_data(data_str, week_pad)
    if week_df is None:
        r = False
    else:
        data.append(week_df)
        week += 1

df = pd.concat(data)

# map in response labels into wide table
question_mapping, response_mapping, county_metro_state = get_crosswalk_sheets(service_account_file)
label_recode_dict = get_label_recode_dict(response_mapping)

df_replaced = df.replace(label_recode_dict)
df_replaced['topline'] = 1


# column groups lists:
stacked_crosstab_features = get_feature_lists(question_mapping, 'stacked_crosstab_features')
multi_index_cols = get_feature_lists(question_mapping, 'multi_index_cols')
stacked_question_features = get_feature_lists(question_mapping, 'stacked_question_features')


#####################################
# replicating Nico's crosstab example
weight_var = 'PWEIGHT'
group_level = 'EST_MSA'
multi_index_vars =  ['SCRAM'] + [group_level] + [weight_var]
stacked_crosstab_vars = ['topline', 'EEDUC','EGENDER']
question_vars = ['RENTCUR', 'MORTCONF', 'EVICT']

df1 = df_replaced.melt(id_vars=multi_index_vars+stacked_crosstab_vars, 
                       value_vars=question_vars,
                       var_name='question_group',
                       value_name='question_val')

df2 = df1.melt(id_vars=multi_index_vars+['question_group','question_val'],
               value_vars=stacked_crosstab_vars,
               var_name='xtab_group',
               value_name='xtab_val')

crosstab = generate_crosstab(df2, group_level, weight_var)

# df_replaced.to_csv(data_dir/"puf_data.csv", index=False)
