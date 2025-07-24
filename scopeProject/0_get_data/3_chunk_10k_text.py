'''
This file chunks the 10K entries from 2_get_10k_text.py into 73 subfiles to be processed in parallel.

Input: 'scopeProject/data/raw/10k/extracted_text_linked.parquet'; not included here. Cleaned and 
    extracted 10K items, with gvkey, fiscal year, firm name (from Compustat and SEC). 

Output: 'scopeProject/data/raw/10k/items1_a_7/' chunk0.parquet - chunk72.parquet. This is just
    the input file chunked into 73 parts.

Note: I chose to allocate 1516 rows per file through experimenting on the Princeton della cluster
and finding that this led to an acceptable trade-off. Too many rows per job -> lon[pg wait for
sufficiently large individual node. Too few rows -> will hit constraint on maximum # of
requested nodes.
'''
import scopeutils as su
import pandas as pd 
import numpy as np
import pyarrow as pa

if __name__ == "main":
    raw_data = su.get_data_path('raw_data_dir') # load path
    file = pd.read_parquet(raw_data / '10k/extracted_text_linked.parquet')

    rows_per_chunk = 1516
    n_chunks = np.ceil(len(file)/rows_per_chunk)

    for i in range(n_chunks):
        start = i * rows_per_chunk
        end = min((i + 1) * rows_per_chunk, len(file))
        if start >= len(file):
            break  # no more rows to write
        chunk = file.iloc[start:end]
        chunk.to_parquet(raw_data / f'10k/items1_a_7/chunk{i}.parquet', index=False)