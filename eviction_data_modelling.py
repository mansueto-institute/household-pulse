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

question_mapping, response_mapping, county_metro_state = get_crosswalk_sheets(service_account_file)
label_recode_dict = get_label_recode_dict(response_mapping)

df_replaced = df.replace(label_recode_dict)
df_replaced['topline'] = 1

stacked_crosstab_features = get_feature_lists(question_mapping, 'stacked_crosstab_features')
multi_index_cols = get_feature_lists(question_mapping, 'multi_index_cols')
stacked_question_features = get_feature_lists(question_mapping, 'stacked_question_features')


# replicating Nico's crosstab example
weight_var = 'PWEIGHT'
id_vars = ['SCRAM', 'EST_MSA', 'topline', 'EEDUC','EGENDER']

df1 = generate_crosstabs(df_replaced, ['SCRAM', 'EST_MSA', 'topline', 'EEDUC','EGENDER']+[weight_var], 
                        ['RENTCUR', 'MORTCONF', 'EVICT'], 'question_group', 'question_val')

df2 = generate_crosstabs(df1, ['SCRAM','EST_MSA','question_group','question_val']+[weight_var],
                         ['topline','EEDUC', 'EGENDER'], 'xtab_group', 'xtab_val')

df3 = df2.groupby(['EST_MSA', 'question_group', 'question_val', 'xtab_group', 'xtab_val']).agg({weight_var: 'sum'}).reset_index()

df3["weight_total"] = df3.groupby(['EST_MSA','question_group','xtab_group'])[weight_var].transform('sum')
df3["share"] = df3[weight_var]/df3.weight_total

df3["weight_total_val"] = df3.groupby(['EST_MSA','question_group','xtab_group','xtab_val'])[weight_var].transform('sum')
df3["share_val"] = df3[weight_var]/df3.weight_total_val
df3.sort_values(by= ['EST_MSA', 'question_group', 'xtab_group'], inplace=True, ascending=True)
df3

# df_replaced.to_csv(data_dir/"puf_data.csv", index=False)
