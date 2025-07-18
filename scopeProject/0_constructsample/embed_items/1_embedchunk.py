from fcns_paths import *
import sys

def chunk_text(text, max):
    segments = text.split('[newline]')
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
    chunk_embeddings = np.average(chunk_embeddings, axis=0, weights=chunk_lens)
    chunk_embeddings = chunk_embeddings / np.linalg.norm(chunk_embeddings)
    chunk_embeddings = chunk_embeddings.tolist()
    return chunk_embeddings

if __name__ == "__main__": 
    #if len(sys.argv) > 1:
    #    n = sys.argv[1]
    #else:
    #    n = 0
    idx = int(os.environ["SLURM_ARRAY_TASK_ID"])
    #idx = idx + 1001
    
    # Specify the directory
    directory = f"{RAW_DATA_DIR}/10ks/test100"
    infiles = os.listdir(directory)
    # remove files if they're already in the list of processed files.
    outdir = f'{PROCESSED_DATA}/10ks/10pct/100rows'
    outfiles = os.listdir(outdir)
    touse = [file for file in infiles if file not in outfiles]
    
    files = [os.path.join(directory, file) for file in touse]
    
    # Sort the files by creation date
    files = sorted(files, key=os.path.getctime)
    
    name = re.search(r'test100/(.*)',files[idx]).group(1)

    raw = pd.read_parquet(os.path.join(directory, name))
    
    mp.set_start_method('spawn', force=True)
    
    def process_series(series, max):
        num_processes = mp.cpu_count()
        chunk_text_partial = partial(chunk_text, max=max)
        with mp.Pool(num_processes) as first_pool:
            return first_pool.map(chunk_text_partial, series)

    def embed_series(series, model):
        num_processes = mp.cpu_count()
        maximum = int(model.max_seq_length)
        maximum = max(maximum, 1024)
        # my HYPOTHESIS is that the line below consumes a lot of memory with large csvs
        # let's try splitting it 
        chunks = process_series(series, maximum) # cut text into para- or sentence-size chunks
        chunks = [[item for item in sublist if re.search(r'\S', item)] for sublist in chunks]
        
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
    
    mname = 'UAE-Large-V1'
    model = SentenceTransformer(f"{DIR}/" + mname)

    #cnaics2['embfull'] = embed_series(cnaics2['text'], model)
    raw['embfull'] = embed_series(raw['text'], model)
    raw.reset_index(drop=True)
    
    raw.to_parquet(f'{PROCESSED_DATA}/10ks/10pct/100rows/{name}', compression='zstd')

# instead of writing to an archive, let's write to parquet. 