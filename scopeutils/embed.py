import pandas as pd
import numpy as np
import re
import nltk
import sys

def chunk_text(text, max):
    '''
    Takes text, splits it into paragraphs; if the paragraph length exceeds the maximum specified length,
    break the paragraph into sentences. Return a list of these paragraphs/sentences.
    '''
    segments = text.split('\n')
    chunk_list = []
    for segment in segments:
        segment = re.sub(r'\s{2,}', ' ', segment)
        cap = max*2
        if len(segment) > cap:
            sentences = nltk.sent_tokenize(segment)
            chunk_list += sentences
        else:
            chunk_list.append(segment)
    return chunk_list

def avg_embed_vecs(chunk_embeddings, chunk_lens):
    '''
    Take length-weighted averages of text embedding vectors. Normalize the avg
    and return a vector of length 1.
    '''
    chunk_embeddings = np.average(chunk_embeddings, axis=0, weights=chunk_lens)
    chunk_embeddings = chunk_embeddings / np.linalg.norm(chunk_embeddings)
    chunk_embeddings = chunk_embeddings.tolist()
    return chunk_embeddings

