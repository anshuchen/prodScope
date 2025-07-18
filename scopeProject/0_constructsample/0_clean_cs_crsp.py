'''
This file cleans and merges Compustat with CRSP to create a dataset of firm characteristics and returns (firm-year panel).
In particular, I use (hierarchial ranking) IPO date/first date in CRSP/first date in Compustat to create firm age variable.

Input: 'scopeProject/data/raw/compustat/compa_for_crspmerge.csv'; not included here. This is a download (as of 2025) of
    Compustat Fundamentals Annual merged with Compustat Company from WRDS. Follow below instructions to get the file.
    It contains the columns year, fyear, datadate, cusip, naicsh, revt, at, sale, cogs, xopr, ib, emp, ipodate, 
    prcc_f, dvpsx_f as div, csho, xrd, capx, ppegt, and ppent.
    'scopeProject/data/raw/compustat/compustat_crsp_link.csv'; not included here. This is a download (as of 2025)
    of the Compustat-CRSP link table from WRDS.
    'scopeProject/data/raw/crsp/crsp_monthly.csv'; not included here. This is a download (as of 2025) of CRSP
    monthly returns from WRDS.
    'scopeProject/data/raw/ritterIPO/IPO-age.xlsx'; included. This is Jay Ritter's dataset documenting IPO
    founding dates from 1975 - 2024 (updated 2025 as of this writing).
    Download here: https://site.warrington.ufl.edu/ritter/files/founding-dates.pdf 

    **To download the first three input files from WRDS, run these two files from scopeProject/0_constructsample/download_files:
    extract_comp_and_crsp.sas and filter_CCM_links.sas in SAS Studio on WRDS.** 

Output: 'scopeProject/data/processed/compustat/compustat_with_returns.csv'. Not included here. Will be used in forming the 
final sample for analysis.
'''

import scopeUtils as su
import pandas as pd
import numpy as np
import re

if __name__ == 'main':
    raw_data = su.get_data_path('raw_data_dir') # define paths
    processed = su.get_data_path('processed_data') 

    comp = pd.read_csv(raw_data / 'compustat/compa_for_crspmerge.csv')

    ccm = pd.read_csv(raw_data / 'compustat/compustat_crsp_link.csv')
    ccm = ccm[['GVKEY', 'permno', 'permco', 'LINKDT', 'LINKENDDT', 'datadate', 'fyear']]
    ccm.columns = ['gvkey', 'permno', 'permco', 'linkdt', 'linenddt', 'datadate', 'fyear']
    cs_with_link = pd.merge(comp, ccm, on=['gvkey', 'datadate', 'fyear'], how='left')

    crsp = pd.read_csv(raw_data / 'crsp/crsp_monthly.csv')
    crsp_str = crsp.groupby('PERMCO')['date'].min()
    crsp_str = crsp_str.rename('crsp_first_date').reset_index()

    crsp = pd.merge(crsp, crsp_str, on=['PERMCO'])
    crsp['date'] = pd.to_datetime(crsp['date'])
    crsp['year'] = crsp['date'].dt.year
    crsp['ret'] = crsp['ret'].replace([-66.0, -77.0, -88.0, -99.0], np.nan)

    def calculate_annual_return(group):
        # If any 'ret' is NaN, return NaN for the entire year
        if group['ret'].isna().any():
            return np.nan
        # Otherwise, compute the cumulative return
        return np.prod(1 + group['ret']) - 1

    # Apply the function to each group
    annual_returns = (
        crsp[['PERMNO', 'date', 'ret', 'year', 'crsp_first_date']].sort_values(['PERMNO', 'date'])
        .groupby(['PERMNO', 'year', 'crsp_first_date'])
        .apply(calculate_annual_return)
        .reset_index(name='ret')
    )
    
    annual_returns.columns = ['permno', 'fyear', 'crsp_first_date', 'ret']
    cs_and_ret = pd.merge(cs_with_link, annual_returns, how='left')

    # calculate firm founding date
    # step 1: earliest Compustat observation
    comp_str = comp.groupby('gvkey')['datadate'].min()
    comp_str = pd.DataFrame(comp_str.rename('comp_first_date'))
    comp_str = comp_str[comp_str['comp_first_date'].astype(str).apply(len) == 8]
    comp_str['comp_first_date'] = pd.to_datetime(comp_str['comp_first_date'], format='%Y%m%d')

    comp  = pd.merge(cs_and_ret, comp_str.reset_index(), how='left')
    # step 2: IPO date (from Compustat and Fields-Ritter)
    comp['ipodate'].astype('Int64').astype(str)
    comp['ipodate'] = pd.to_datetime(comp['ipodate'], format='%Y%m%d')

    # import Fields-Ritter founding dates

    fr = pd.read_excel(raw_data / 'ritterIPO/IPO-age.xlsx')
    fr = fr[[col for col in fr.columns if not re.match('Unnamed', col)]]
    fr['Founding']=np.where(fr.Founding==-99, np.nan, fr.Founding)
    fr['Founding']=np.where(fr.Founding==-9, np.nan, fr.Founding)
    fr['Founding']=np.where(fr.Founding==201, 2013, fr.Founding)
    fr['Founding']=np.where(fr.Founding==0, np.nan, fr.Founding)

    fr.dropna(inplace=True)
    fr['Founding']=fr.Founding.astype('int32')

    fr = fr[(fr['CRSP perm'] != '.') & (~fr['CRSP perm'].isna())]
    fr['permco'] = fr['CRSP perm'].astype('float64')

    fr = fr.rename(columns={'CUSIP':'cusip'})

    t = pd.merge(comp, fr[['permco', 'cusip', 'Founding']], how='left')
    # Hierarchial ranking: IPO date > CRSP first obs > Compustat first OBS
    t['ipodate_2'] = t['ipodate']
    t['ipodate_2'] = t['ipodate_2'].mask(t['ipodate_2'].isna(), t['crsp_first_date'])
    t['ipodate_2'] = t['ipodate_2'].mask(t['ipodate_2'].isna(), t['comp_first_date'])

    t['firstyear'] = t['Founding']
    t['firstyear'] = t['firstyear'].mask(t['firstyear'].isna(), t['ipodate_2'].dt.year)

    t['age'] = t['year'] - t['firstyear']
    t['age'] = t['age'].clip(lower=0)

    # remove rows with missing assets or sales
    t = t[(~t['at'].isna()) & (~t['sale'].isna())]
    t.to_csv(processed / 'compustat/compustat_with_returns.csv', index=False)