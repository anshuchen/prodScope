import pandas as pd
import numpy as np
import os
import sys
import re
import time
from itertools import islice
import datetime
import ast
import csv
import json
import nltk
import torch.multiprocessing as mp
from functools import partial
from sentence_transformers import SentenceTransformer, util, LoggingHandler

# -----------------------------------------------------------------
# Directories
# -----------------------------------------------------------------
DIR = "/scratch/gpfs/anshuc"
RAW_DATA_DIR = f"{DIR}/to_embed"
PROCESSED_DATA = f"{DIR}/processed_data"
TEMP = f"{DIR}/temp"

# -----------------------------------------------------------------
# Embedding functions
# -----------------------------------------------------------------
# def chunk_text(text, max):
#         segments = text.split('[newline]')
#         chunk_list = []
#         for segment in segments:
#             segment = re.sub(r'\s{2,}', ' ', segment)
#             cap = max*2
#             if len(segment) > cap:
#                 sentences = nltk.sent_tokenize(segment)
#                 chunk_list += sentences
#             else:
#                 chunk_list.append(segment)
#         return chunk_list
       
# def process_series(series, max):
#     num_processes = mp.cpu_count()
#     chunk_text_partial = partial(chunk_text, max=max)
#     with mp.Pool(num_processes) as first_pool:
#         return first_pool.map(chunk_text_partial, series)

# def avg_embed_vecs(chunk_embeddings, chunk_lens):
#     # returns the json dump of the embedding vector, for easy parquet storage
#     chunk_embeddings = np.average(chunk_embeddings, axis=0, weights=chunk_lens)
#     chunk_embeddings = chunk_embeddings / np.linalg.norm(chunk_embeddings)
#     chunk_embeddings = chunk_embeddings.tolist()
#     return chunk_embeddings

# def embed_series(series, model):
#     num_processes = mp.cpu_count()
#     max = int(model.max_seq_length)
#     chunks = process_series(series, max) # cut text into para- or sentence-size chunks
#     chunks = [[item for item in sublist if re.search(r'\S', item)] for sublist in chunks]
    
#     # embed the chunks
#     tokens_list = [item for sublist in chunks for item in sublist]
#     pool = model.start_multi_process_pool()
#     embdescs = model.encode_multi_process(tokens_list, pool, normalize_embeddings=True).tolist()
#     model.stop_multi_process_pool(pool)

#     # reconstitute list of lists
#     nchunks = [len(list) for list in chunks]
        
#     embdescs_list = []
#     start = 0
#     for n in nchunks:
#         embdescs_list.append(embdescs[start:start+n])
#         start += n
                
#     # calculate weighted average embeddings 
#     chunk_lens = [[len(piece) for piece in lst] for lst in chunks]
                
#     args = list(zip(embdescs_list, chunk_lens))

#     with mp.Pool(num_processes) as last_pool:
#         embs =  last_pool.starmap(avg_embed_vecs, args)
#     return embs

#def cosine_similarity(a, b):
#    return np.dot(a, b) / (np.linalg.norm(a) * np.linalg.norm(b))
