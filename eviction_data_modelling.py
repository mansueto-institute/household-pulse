from pathlib import Path

from etl import *

root = Path.cwd()
data_dir = root/"data"

data_dir.mkdir(exist_ok=True)

SERVICE_ACCOUNT_FILE = root/"credentials.json"

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

df.to_csv(data_dir/"puf_data.csv", index=False)

question_mapping_df, response_mapping, county_metro_state = get_crosswalk_sheets(SERVICE_ACCOUNT_FILE)

