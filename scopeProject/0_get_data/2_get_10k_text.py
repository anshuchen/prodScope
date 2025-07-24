'''
This file uses the SEC-API to scrape items 1, 1A, and 7 from 10K filings. It requires a paid subscription. 
The code takes ~12 hours to run.

Input: 'scopeProject/data/processed/pre_retrieval/full5mil.csv'; not included here. This is a subsample of 
    Compustat Fundamentals Annual + CRSP returns + CIK-GVKEY link containing firm-year observations 
    where the firm's revenue ('revt') exceeded 5 million USD. 

Intermediate files: 'scopeProject/data/raw/10k/extracted_text.parquet'; not included here. Gives extracted 
    10K items for the firm-years from full5mil.csv. 
    'scopeProject/data/raw/10k/errors.csv'; not included here. Lists 10K links that threw an error when the
    function attempted to scrape.

Output:'scopeProject/data/raw/10k/extracted_text_linked.parquet'; not included here. Gives cleaned and 
    extracted 10K items, with gvkey, fiscal year, firm name (from Compustat and SEC). 
'''

import scopeutils as su
import pandas as pd
import os
from sec_api import ExtractorApi

def get_10k(key, links):
    '''
    Use ExtractorApi (from SEC-API) to get items 1, 1a, and 7 from a list of 10K filing links.
    '''
    linklist = links['fname'].unique() # list of unique 10K URLs

    raw_data = su.get_data_path('raw_data_dir') # load path

    # Load existing results if available
    if os.path.exists(raw_data / '10k/extracted_text.parquet'):
        df = pd.read_parquet(raw_data / '10k/extracted_text.parquet')
        item1s = df['item1'].tolist()
        item1as = df['item1a'].tolist()
        item7s = df['item7'].tolist()
        start_idx = len(df)
    else:
        item1s = []
        item1as = []
        item7s = []
        start_idx = 0
    
    if os.path.exists(raw_data / '10k/errors.csv'):
        dfe = pd.read_csv(raw_data / '10k/errors.csv')
        errors = dfe['link'].tolist()
    else:
        errors = []

    total_iterations = len(linklist)
    n = 1000  # Save every n iterations

    extractorApi = ExtractorApi(key)

    # Long computation loop
    for i in range(start_idx, total_iterations):  # Start from where we left off
        filing_10_k_url = 'https://www.sec.gov/Archives/' + linklist[i]
        try:
            item1 = extractorApi.get_section(filing_10_k_url, '1', 'text')
            item1s.append(item1)
            item7 = extractorApi.get_section(filing_10_k_url, '7', 'text')
            item7s.append(item7)
            item1a = extractorApi.get_section(filing_10_k_url, '1A', 'text')
            item1as.append(item1a)
        except:
            errors.append(linklist[i])
            item1s.append('error')
            item7s.append('error')
            item1as.append('error')

        # Save every n-th iteration
        if (i + 1) % n == 0:
            pd.DataFrame({'link': linklist[0:(i+1)], 'item1': item1s, 'item1a': item1as, 'item7': item7s}).to_parquet(raw_data / '10k/extracted_text.parquet', index=False)
            print(f"Saved results at iteration {i + 1}.")
            pd.DataFrame({'link': errors}).to_csv(raw_data / '10k/errors.csv', index=False)

    text = pd.DataFrame({'link': linklist, 'item1': item1s, 'item1a': item1as, 'item7': item7s})
    errors = pd.DataFrame({'link': errors})
    return text, errors

def clean_10ks(df):
    '''
    Adds a 'missing' flag for entries where the text is shorter than 10,000 characters.
    Approximately 6% of item 1 entries are flagged missing.
    Create new variable concatenating available text fields. 
    '''
    df['item1_na'] = 0
    df.loc[df['item1'].apply(len) < 10000, 'item1_na'] = 1

    df['item7_na'] = 0
    df.loc[df['item7'].apply(len) < 10000, 'item7_na'] = 1

    df['item1a_na'] = 0
    df.loc[df['item1a'].apply(len) < 10000, 'item1a_na'] = 1

    # concatenate test from the different fields
    df['text'] = df['item1'] + df['item1a'] + df['item7']

    df.loc[(df['item1_na'] == 0) & (df['item1a_na'] == 0) & (df['item7_na'] == 1), 'text'] = df['item1'] + df['item1a']
    df.loc[(df['item1_na'] == 0) & (df['item1a_na'] == 1) & (df['item7_na'] == 0), 'text'] = df['item1'] + df['item7']
    df.loc[(df['item1_na'] == 0) & (df['item1a_na'] == 1) & (df['item7_na'] == 1), 'text'] = df['item1'] 
    df = df.rename(columns = {'link': 'fname'})
    return df 

if __name__ == 'main':
    raw_data = su.get_data_path('raw_data_dir') # define paths
    processed = su.get_data_path('processed_data') 

    links = pd.read_csv(processed / 'pre_retrieval/full5mil.csv')
    
    key = 'string' # insert sec-API key here
    extracted, errors = get_10k(key, links)
    extracted.to_parquet(raw_data / '10k/extracted_text.parquet', index=False) # intermediate save 
    errors.to_csv(raw_data / '10k/errors.csv', index=False)
    print("Intermediate save completed.")

    filtered = clean_10ks(extracted)

    fintext = pd.merge(links, filtered, on='fname') # add back compustat vars (name, gvkey, fyear, dates)
    fintext.to_parquet(raw_data / '10k/extracted_text_linked.parquet', index=False)