from pathlib import Path
from typing import Dict, Optional, Sequence, Tuple

import zipfile
import io
import requests
import numpy as np
import pandas as pd

def data_url_str(w: int, wp):
    return f"wk{w}/HPS_Week{wp}_PUF_CSV.zip"

def data_file_str(i: int):
    return f"pulse2020_puf_{i}.csv", f"pulse2020_repwgt_puf_{i}.csv"

def get_puf_data(data_str: str, i: int, base_url: str = "https://www2.census.gov/programs-surveys/demo/datasets/hhp/2020/"):
    '''
    download puf files for the given weeks and concatenate the datasets
    '''
    data_filename, weights_filename = data_file_str(i)
    url = base_url + data_str
    print(url)
    r = requests.get(url)
    read_zip = zipfile.ZipFile(io.BytesIO(r.content))
    data_df = pd.read_csv(read_zip.open(data_filename), dtype={'SCRAM': 'string'})
    weight_df = pd.read_csv(read_zip.open(weights_filename), dtype={'SCRAM': 'string'})
    return data_df.merge(weight_df, how='left', on='SCRAM')


