import zipfile
import tempfile
import requests
import pandas as pd

def download_url(url, save_path, chunk_size=128):
    r = requests.get(url, stream=True)
    with open(save_path, 'wb') as fd:
        for chunk in r.iter_content(chunk_size=chunk_size):
            fd.write(chunk)

file_str = "pulse2020_puf_{}.csv".format(i)
url_str = "https://www2.census.gov/programs-surveys/demo/datasets/\
    hhp/2020/wk{w}/HPS_Week{w}_PUF_CSV.zip".format(w=i)
temp_dir_name = 'tmp'
with tempfile.TemporaryDirectory() as tmpdirname:
     print('created temporary directory', tmpdirname)
    with zipfile.ZipFile("file.zip","r") as zip_ref:
        zip_ref.extractall("tmp")