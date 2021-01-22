from pathlib import Path

from etl import *

root = Path("/Users/caitlinloftus/Projects/eviction-defense-map/")
data_dir = root/"data"

data_dir.mkdir(exist_ok=True)

data = []
for week in range(13, 14):
    week_pad = str(week).zfill(2)
    data_str = data_url_str(week, week_pad)
    week_df = get_puf_data(data_str, week_pad)
    data.append(week_df)

df = pd.concat(data)

df.to_csv(data_dir/"puf_data_1_to_21.csv", index=False)
weights_df.to_csv(data_dir/"puf_weights_1_to_21.csv", index=False)

