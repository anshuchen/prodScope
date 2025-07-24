'''
This file embeds 10K text in parallel jobs.

Input: 'scopeProject/data/raw/10k/items1_a_7/chunk0.parquet' to '.../chunk72.parquet'; not included here. 
    Cleaned and extracted 10K items, with gvkey, fiscal year, firm name (from Compustat and SEC), divided 
    into 73 parts.

Output: 'scopeProject/data/processed/embedded/items1_a_7/chunk0.parquet' to '.../chunk72.parquet'; embedded 
    versions of the iput.

Run this file on an HPC cluster using the other file in this folder, emb_items.slurm.
'''

#from fcns_paths import *
import scopeutils as su

import pandas as pd
import os
import sys
import re
import time
from itertools import islice
import datetime
import torch.multiprocessing as mp
from functools import partial
from sentence_transformers import SentenceTransformer, util, LoggingHandler

if __name__ == "__main__": 
    idx = int(os.environ["SLURM_ARRAY_TASK_ID"])
    
    # Specify the directory
    raw_data = su.get_data_path('raw_data_dir') # load paths
    processed_data = su.get_data_path('processed_data')

    infiles = os.listdir(raw_data / '10k/items1_a_7')
    # remove files if they're already in the list of processed files.

    outfiles = os.listdir(processed_data / 'embedded/items1_a_7')
    touse = [file for file in infiles if file not in outfiles]
    
    files = [os.path.join(raw_data / '10k/items1_a_7', file) for file in touse]
    
    # Sort the files by creation date
    files = sorted(files, key=os.path.getctime)
    
    name = re.search(r'items1_a_7/(.*)',files[idx]).group(1)
    raw = pd.read_parquet(os.path.join(directory, name))

    # since we embed 'text' and 'item1', 
    # require that each item have length > 0.
    raw = raw[raw['text'].apply(len) > 0]
    raw = raw[raw['item1'].apply(len) > 0]
    
    mp.set_start_method('spawn', force=True)

    # I have to define these functions inside the __name__ guard
    # so that, as the file goes through SLURM,
    # we only spawn a multiprocessing pool right before we need
    # to use it (we don't want to trigger cascading processes)

    def process_series(series, max):
        '''
        Take a Pandas column (string type) and convert each string into a list of the string's constituent
        paragraphs (or sentences, if the paragraph length exceed the max).
        '''
        num_processes = mp.cpu_count()
        chunk_text_partial = partial(chunk_text, max=max)
        with mp.Pool(num_processes) as first_pool:
            return first_pool.map(chunk_text_partial, series)

    def embed_series(series, model):
        num_processes = mp.cpu_count()
        maximum = int(model.max_seq_length)
        maximum = max(maximum, 1024) # I think I added this limit because the cluster runs out of memory
        chunks = process_series(series, maximum) # cut text into para- or sentence-size chunks
        chunks = [[item for item in sublist if re.search(r'\S', item)] for sublist in chunks] # cut out non-sentences
        
        # embed the chunks
        tokens_list = [item for sublist in chunks for item in sublist]
        pool = model.start_multi_process_pool()
        embdescs = model.encode_multi_process(tokens_list, pool, normalize_embeddings=True).tolist() #, batch_size=8, precision='int8').tolist()
        model.stop_multi_process_pool(pool)
    
        # reconstitute list of lists
        nchunks = [len(list) for list in chunks]
            
        embdescs_list = []
        start = 0
        for n in nchunks:
            embdescs_list.append(embdescs[start:start+n])
            start += n
                    
        # calculate weighted average embeddings 
        chunk_lens = [[len(piece) for piece in lst] for lst in chunks]
                    
        args = list(zip(embdescs_list, chunk_lens))
    
        with mp.Pool(num_processes) as last_pool:
            embs =  last_pool.starmap(avg_embed_vecs, args)
        return embs
    
    mpath = su.get_data_path('model_dir')
    model = SentenceTransformer(mpath) 

    raw['embfull'] = embed_series(raw['text'], model)
    raw['emb1'] = embed_series(raw['item1'], model)
    raw.reset_index(drop=True)
    
    raw.to_parquet(processed_data / f'embedded/items1_a_7/{name}.parquet', compression='zstd')