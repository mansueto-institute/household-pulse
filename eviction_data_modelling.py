from pathlib import Path

from etl import *

root = Path("/Users/caitlinloftus/Projects/eviction-defense-map/")
data_dir = root/"data"

data_dir.mkdir(exist_ok=True)

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

