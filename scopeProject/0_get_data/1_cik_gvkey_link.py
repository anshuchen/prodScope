'''
This file downloads the WRDS dataset of 10K filings, cleans the CIK-gvkey links file, merges the links file to 10Ks,
and finally merges to Compustat. 

**To be run on WRDS JupyterHub. The file paths will still work if you upload the project with its data directories
to WRDS Cloud.**

Input: 'scopeProject/data/processed/compustat_with_returns.csv'; not included here. Created from 0_clean_cs_crsp.py.
    'scopeProject/data/raw/compustat/WCIKLINK_GVKEY.csv'; not included here. List of CIK-GVKEY links from WRDS. GVKEY is
    Compustat's unique firm identifier, while CIK is the SEC's firm identifier (10Ks). The links are labeled with 
    start/end dates. 
    
    **To download the CIK-GVKEY link file from WRDS, run this files from scopeProject/0_constructsample/download_files:
    extract_cikgvkey.sas in SAS Studio on WRDS.** 

Output: 'scopeProject/data/processed/pre_retrieval/full5mil.csv'. Not included here. Will be used to extract 10k text.

'''

import scopeUtils as su
import saspy
import pandas as pd
import os
import re
import numpy as np

def get_10k_companies():
    # Initialize SAS library
    sas = saspy.SASsession()
    sas.submit('libname secprod "/wrds/sec/sasdata";');

    # get list of ciks in the universe of 10K filers

    wrds_forms_company = sas.sasdata('wrds_forms', 'secprod', dsopts={'where':
                                                                    'form like "10-K%"' })
    wrds_forms_company_df = wrds_forms_company.to_df()
    companies = wrds_forms_company_df[wrds_forms_company_df['form'].isin(['10-K', '10-K405'])]

    companies['year'] = companies['rdate'].dt.year
    companies['rdate'] = pd.to_datetime(companies['rdate'])

    companies = companies.sort_values(by=['cik', 'year', 'rdate'])

    # keep the first row in each group of the same cik/rdate
    companies = companies.drop_duplicates(subset=['cik', 'rdate'])

    companies['cik'] = companies['cik'].astype(int)
    return companies

def clean_linkfile(df):
    df['link_start_date'] = df['link_start_date'].str.strip().replace(['0'], np.NaN)
    # 'B' means that the link dates before the sample's coverage period
    df['link_start_date'] = df['link_start_date'].str.strip().replace(['B'], '17000101') # arbitrarily early date for link start
    df['link_end_date'] = df['link_end_date'].str.strip().replace(['0'], np.NaN)
    # 'E' means that the link dates after the sample's coverage period
    df['link_end_date'] = df['link_end_date'].str.strip().replace(['E'], '22600101') # arbitrarily late date for link end
        
    df = df[['cik', 'gvkey', 'source', 'link_desc', 'sec_company_name', 'link_company_name',
                            'link_start_date', 'link_end_date', 'N10K']]
        
    df = df[df['gvkey'].notna() & df['cik'].notna()]
    df['gvkey'] = df['gvkey'].astype('int32')
        
    df['DATADATE1'] = pd.to_datetime(df['link_start_date'])
    df['DATADATE2'] = pd.to_datetime(df['link_end_date'])
    return df

def read_comp(compa):
    compa = compa.drop(columns=['cik'])
    compa['naics3'] = compa['naicsh'].astype(str).str[0:3]
    compa['naics2'] = compa['naicsh'].astype(str).str[0:2]

    compa = compa[~compa['at'].isna()]
    return compa

if __name__ == 'main':
    raw_data = su.get_data_path('raw_data_dir') # define paths
    processed = su.get_data_path('processed_data') 

    companies = get_10k_companies()
    cikgvkey = pd.read_csv(raw_data / 'compustat/WCIKLINK_GVKEY.csv', encoding='utf-8', encoding_errors='replace')
    cikgvkey = clean_linkfile(cikgvkey)
    temp = pd.merge(companies, cikgvkey, on='cik')

    # filter for valid links: record's date falls between the gvkey link's start/end dates
    temp_filtered = temp.loc[(temp['rdate'] >= temp['DATADATE1']) & (temp['rdate'] <= temp['DATADATE2'])]
    # drop duplicates
    dropped_dups = temp_filtered.drop_duplicates(subset=['cik', 'gvkey', 'rdate'])

    trimmed = dropped_dups[['fdate', 'cik', 'form', 'coname', 'wrdsfname', 'fname', 'rdate', 'year', 'gvkey', 'source', 
              'link_desc', 'sec_company_name', 'link_company_name', 'link_start_date', 'link_end_date',
              'DATADATE1', 'DATADATE2']]
    
    # If a 10K was filed within the first 5 months of a given year, categorize it to the previous fiscal year
    trimmed.loc[(trimmed['rdate'].dt.month < 6), 'year'] -= 1
    trimmed = trimmed.rename(columns = {'year':'fyear'})
    trimmed['fyear'] = trimmed['fyear'].astype(int)

    compa = pd.read_csv(processed / 'compustat/compustat_with_returns.csv')
    compa = read_comp(compa)

    pre_filter = pd.merge(trimmed, compa, on=['gvkey', 'fyear'])

    # impose 5 million minimum revenue 

    f5mil = pre_filter[pre_filter['revt'] > 5]

    f5mil.to_csv(processed / 'pre_retrieval/full5mil.csv', index=False)