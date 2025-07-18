'''
This file chunks the 10K entries from 2_get_10k_text.py into 72 subfiles to be processed in parallel.

Input: 'scopeProject/data/raw/10k/extracted_text_linked.parquet'; not included here. Gives cleaned and 
    extracted 10K items, with gvkey, fiscal year, firm name (from Compustat and SEC). 

Output: (items1_a_7 chunk0 - chunk72)
'''
import scopeUtils as su
import pandas as pd 
import numpy as np
import pyarrow as pa
import os
import re



if __name__ == "main":
    raw_data = su.get_data_path('raw_data_dir') # load path
    file = pd.read_parquet(raw_data / '10k/extracted_text_linked.parquet')