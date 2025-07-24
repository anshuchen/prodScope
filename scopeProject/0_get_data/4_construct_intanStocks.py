'''
This file uses the Ewens, Peter, and Wang (2020) methodology to calculate intangible stocks based on cumulated
R&D and SG&A spending. This is largely a copy of Dijun Liu's code 
(https://github.com/michaelewens/Intangible-capital-stocks/blob/master/intangibes_cleaned.py). I have supplemented
with Jay Ritter's IPO founding data. 

Input: 'scopeProject/data/raw/compustat/funda.csv'; not included here. This is a full download (as of 2025) of
    Compustat Fundamentals Annual. Usual filters apply.
    'scopeProject/data/raw/compustat/company.csv'; not included here. This is a full download (as of 2025) of
    Compustat's 'Company' table.
    'scopeProject/data/raw/ritterIPO/IPO-age.xlsx'; included. This is Jay Ritter's dataset documenting IPO
    founding dates from 1975 - 2024 (updated 2025 as of this writing).
    Download here: https://site.warrington.ufl.edu/ritter/files/founding-dates.pdf 

Output: 'scopeProject/data/raw/epwIntans/intan_updated_2024.csv'. Contains columns 'gvkey', 'fyear' (fiscal 
year), 'kcap_v2' (knowledge capital; cumulated R&D), and 'ocap_v2' (organizational capital; cumulated SG&A)
'''

import scopeUtils as su
import pandas as pd
import numpy as np
import os
import gc, math, matplotlib.pyplot as plt, pickletools
from tqdm import tqdm
from scipy.stats import kurtosis, skew
import statsmodels.formula.api as smf #it ignores np.nan automatically
import warnings

gc.collect()  #release the space

# datasets needed:
# Compustat FUNDA
# Compustat Company
# Ritter IPO

if __name__ == 'main':
    raw_data = su.get_data_path('raw_data_dir') # load path

    funda = pd.read_csv(raw_data / 'compustat/funda.csv')
    company = pd.read_csv( raw_data/ 'compustat/company.csv')

    funda=pd.merge(funda,company, how="left",on=["gvkey"]) 

    # cleaning
    funda['sich']=np.where(np.isnan(funda.sich),funda.sic, funda.sich)
    funda['gvkey']=funda.gvkey.astype(int)
    funda=funda[funda.gvkey!=175650] # this firm has data issues
    funda=funda[(funda.indfmt.values=="INDL") & (funda.datafmt.values=="STD")]
    funda['ipodate']=pd.to_datetime(funda.ipodate)
    funda['datadate']=pd.to_datetime(funda.datadate)
    funda['firstcomp']=funda.groupby('gvkey',as_index=False).fyear.transform(min)
    funda=funda.dropna(subset=['fyear'],axis=0)
    #xsga sometimes is negative, but still keep these records
    funda[['gvkey','sich']]=funda[['gvkey','sich']].astype(int)
    funda=funda.sort_values(by=['gvkey','datadate'])
    funda['atnan']=np.where(np.isnan(funda['at']),1,0)

    # Address missing SG&A
    # We set xsga, xrd, and rdip to zero when missing. 
    # For R&D and SG&A, we make exceptions in years when the firm’s assets are also missing. 
    # For these years, we interpolate these two variables using their nearest non-missing values. 
    # We use these interpolated values to compute capital stocks but not regressions’ dependent variables
    # for xsga, interpolate over all periods.

    fxsga=[]
    funda=funda.reset_index(drop=True)
    funda['indexl']=funda.index.values
    for i in funda.gvkey.unique():
        #gvkey==1065,1072, especially 1072 is a good example to check this for loop.
        a=funda[funda.gvkey==i].copy()
        a['xsgaf']=a.xsga
        if a.atnan.sum()>=1:
            for idx in a[(a.atnan==1)&np.isnan(a.xsga)].indexl:
                idx2=a[a.indexl<=idx].xsga.last_valid_index()
                if idx2 is None:
                    b=np.nan
                else:
                    b=a[a.indexl==idx2].xsga.values
                    distb=idx-idx2 ##need to make sure the index is continuous
                    
                idx3=a[a.indexl>idx].xsga.first_valid_index()
                if idx3 is None:
                    c=np.nan
                else:
                    c=a[a.indexl==idx3].xsga.values
                    distc=idx3-idx
                try:
                    a['xsgaf']=np.where((a.indexl==idx)&(distb<=distc),b,a.xsgaf)
                    a['xsgaf']=np.where((a.indexl==idx)&(np.isnan(a.xsgaf)),c,a.xsgaf)
                except NameError:
                    a['xsgaf']=np.where(a.indexl==idx,b,a.xsgaf)
                    a['xsgaf']=np.where((a.indexl==idx)&(np.isnan(a.xsgaf)),c,a.xsgaf)                
        else:
            a=a
        fxsga.extend(a.xsgaf)
    funda['xsga']=fxsga  

    # Address missing R&D
    ##for xrd, interpolate after 1977
    fxrd=[] 
    fundalater=funda[funda.fyear>=1977]
    funda=funda.drop(funda[funda.fyear>=1977].index)
    fundalater=fundalater.reset_index(drop=True)
    fundalater['indexl']=fundalater.index.values
    for i in fundalater.gvkey.unique():
        
        a=fundalater[fundalater.gvkey==i].copy()
        a['xrdf']=a.xrd
        if a.atnan.sum()>=1:
            #idx=a[a.atnan==1].indexl.tail(1)
            for idx in a[(a.atnan==1)&np.isnan(a.xrd)].indexl:
                idx2=a[a.indexl<=idx].xrd.last_valid_index()
                if idx2 is None:
                    b=np.nan
                else:
                    b=a[a.indexl==idx2].xrd.values
                    distb=idx-idx2
                idx3=a[a.indexl>idx].xrd.first_valid_index()
                if idx3 is None:
                    c=np.nan
                else:
                    c=a[a.indexl==idx3].xrd.values
                    distc=idx3-idx
                try:
                    a['xrdf']=np.where((a.indexl==idx)&(distb<=distc),b,a.xrdf)
                    a['xrdf']=np.where((a.indexl==idx)&(np.isnan(a.xrdf)),c,a.xrdf)
                except NameError:
                    a['xrdf']=np.where(a.indexl==idx,b,a.xrdf)
                    a['xrdf']=np.where((a.indexl==idx)&(np.isnan(a.xrdf)),c,a.xrdf)
        else:
            a=a
        fxrd.extend(a.xrdf)
    fundalater['xrd']=fxrd 
    funda=pd.concat([funda,fundalater],ignore_index=True)

    funda=funda.sort_values(by=['gvkey','fyear'])
    funda=funda.reset_index(drop=True)
    funda['indexl']=funda.index.values
    ###We start in 1977 to give firms two years to comply with FASB’s 1975 R&D reporting requirement. If we see a firm with R&D equal to zero or missing in 1977, we assume the firm was typically not an R&D spender before 1977, so we set any missing R&D values before 1977 to zero. Otherwise, before 1977, we either interpolate between the most recent nonmissing R&D values (if such observations exist) or we use the method in Appendix A (if those observations do not exist). Starting in 1977, we make exceptions in cases in which the firm’s assets are also missing. These are likely years when the firm was privately owned. In such cases, we interpolate R&D values using the nearest non-missing values.
    def xrd1977(g):
        ##if 1977 xrd is 0, all previous is 0 or missing, make values before 1977 is also 0. 
        a=g[g.fyear==1977].xrd
        if a.shape[0]!=0:
            if np.isnan(a.values[0]):
                b=np.where((g.fyear<1977)&(np.isnan(g.xrd)),0,g.xrd)
                b=np.where((g.fyear>=1977)&(np.isnan(g.xrd)),0,b)
            elif a.values[0]==0:
                b=np.where((g.fyear<1977)&(np.isnan(g.xrd)),0,g.xrd)
                b=np.where((g.fyear>=1977)&(np.isnan(g.xrd)),0,b)
            else:
                b=np.where((g.fyear>=1977)&(np.isnan(g.xrd)),0,g.xrd)
    #    else:#either interpolating or backward filling later.
    #after 1977, missing xrd is set to be 0 because previously we have interpolated xrd after 1977 when at is also missing.
    #            else:
    #                b=np.where((g.fyear>=1977)&(np.isnan(g.xrd)),0,g.xrd)
        else:
            b=np.where((g.fyear>=1977)&(np.isnan(g.xrd)),0,g.xrd)
        return b
    c=[]
    for i in tqdm(sorted(funda.gvkey.unique())):
        #gvkey=1010,1000,65499 are good examples
        g=funda[funda.gvkey==i]
        c.extend(xrd1977(g))
    funda['xrd']=c

    funda['xsga1']=np.where(np.isnan(funda.xsga),0,funda.xsga)-np.where(np.isnan(funda.xrd),0,funda.xrd)-np.where(np.isnan(funda.rdip),0,funda.rdip)
    funda['xsga2']=np.where((np.where(np.isnan(funda.cogs),0,funda.cogs)>np.where(np.isnan(funda.xrd),0,funda.xrd)) & (np.where(np.isnan(funda.xrd),0,funda.xrd)>np.where(np.isnan(funda.xsga),0,funda.xsga)),np.where(np.isnan(funda.xsga),0,funda.xsga),funda.xsga1) 
    funda['xsga3']=np.where(np.isnan(funda.xsga),np.where(np.isnan(funda.xsga),0,funda.xsga),funda.xsga2)
    funda['xsga']=funda.xsga3

    # Calculate age
    funda['count']=funda.groupby(['gvkey']).cumcount()
    funda['ageipo']=funda.fyear-funda.ipodate.dt.year

    # Calculate growth rates
    #negative values exist, say gvkey=23978, year=1999
    def step1(variablen):
        growthrates=pd.DataFrame(columns=['grate'])
        for i in range(1,int(funda.ageipo.max())+1):
            a=funda[funda.ageipo==i]
            b=funda[funda.ageipo==(i-1)]
            c=pd.merge(a,b[['gvkey',variablen,'ageipo']],on=['gvkey'],how='left')
            d=c[(c[variablen+'_x']>0)&(c[variablen+'_y']>0)]
            growthrates.loc[i,'grate']=(np.log(d[variablen+'_x'])-np.log(d[variablen+'_y'])).mean()
        return growthrates        
    step1g_xsga=step1('xsga')
    step1g_xrd=step1('xrd')
    def step2(variablen):
        growthrates=pd.DataFrame(columns=['grate'])
        for i in range(1,3):
            a=funda[funda.ageipo==(i-2)]
            b=funda[funda.ageipo==(i-3)]
            c=pd.merge(a,b[['gvkey',variablen,'ageipo']],on=['gvkey'],how='left')
            d=c[(c[variablen+'_x']>0)&(c[variablen+'_y']>0)]
            growthrates.loc[i,'grate']=(np.log(d[variablen+'_x'])-np.log(d[variablen+'_y'])).mean()
        return growthrates   
    step2g_xsga=step2('xsga').mean()
    step2g_xrd=step2('xrd').mean()

    # interpolating or filling missing R&D observations.
    funda=funda.sort_values(by=['gvkey','fyear'])
    funda=funda.reset_index(drop=True)
    funda['indexl']=funda.index.values
    fxrd=[] 
    fundabefore=funda[funda.fyear<=1977]
    funda=funda.drop(funda[funda.fyear<=1977].index)

    for i in fundabefore.gvkey.unique():
        g=fundabefore[fundabefore.gvkey==i].copy()
        g['xrdf']=g.xrd
        a=g[g.fyear==1977].xrd
        if a.shape[0]!=0:
            if a.values[0]>0:
                for idx in g[np.isnan(g.xrd)].indexl:
                    idx2=g[g.indexl<=idx].xrd.last_valid_index()
                    if idx2 is None:
                        b=np.nan
                    else:
                        b=g[g.indexl==idx2].xrd.values
                        distb=idx-idx2
                    idx3=g[g.indexl>idx].xrd.first_valid_index()
                    if idx3 is None:
                        c=np.nan
                    else:
                        c=g[g.indexl==idx3].xrd.values
                    distc=idx3-idx
                    try:
                        g['xrdf']=np.where((g.indexl==idx)&(distb<=distc),b,g.xrdf)
                        g['xrdf']=np.where((g.indexl==idx)&(np.isnan(g.xrdf)),c,g.xrdf)
                    except NameError:
                        g['xrdf']=np.where(g.indexl==idx,b,g.xrdf)
                        g['xrdf']=np.where((g.indexl==idx)&(np.isnan(g.xrdf)),c,g.xrdf)          
        else:
            g=g
        fxrd.extend(g.xrdf.values)
    fundabefore['xrd']=fxrd 
    funda=pd.concat([funda,fundabefore],ignore_index=True)

    # Cumulate R&D
    ###estimate R&D in step1
    fundadj=funda[(funda.firstcomp<1977)&(np.isnan(funda.xrd))]
    allnan=fundadj.groupby('gvkey').xrd.all(np.nan) ##for these, all xrd are empty, no need to do the adjustment to r&d step1.

    funda['xsga']=np.where(np.isnan(funda.xsga),0,funda.xsga)
    funda['xrd']=np.where(np.isnan(funda.xrd),0,funda.xrd)
    funda=funda.drop(columns=['indexl','xsga1','xsga2','xsga3'])

    #adding estimated values between IPO year and first compustat, step 4
    gv=funda[funda.ageipo>0].gvkey.unique()        
    fy=funda[funda.ageipo>0].groupby('gvkey',as_index=False).ipodate.apply(lambda x: x.dt.year.unique()[0])
    cusip=funda[funda.ageipo>0].groupby('gvkey').cusip.unique().apply(lambda x: list(set(x))[0])
    a=pd.DataFrame({'gvkey':gv,'fyear':fy.ipodate,'cusip':cusip.values})

    funda=pd.concat([funda,a])
    funda=funda.sort_values(by=['gvkey','fyear','datadate','seq'], na_position='first').drop_duplicates(subset=['fyear','gvkey'],keep='last')
    ##original code does not contain na_position, to see why it matters, see gvkey==12849
    funda.index=pd.to_datetime(funda.fyear, format='%Y')
    funda=funda.groupby('gvkey',as_index=False).resample("Y").ffill() 
    funda['fyear']=funda.index.get_level_values('fyear')  
    funda['fyear']=funda.fyear.dt.year
    funda=funda.reset_index(drop=True)
    funda=funda.sort_values(by=['gvkey','fyear'])
    funda['indexl']=funda.index.values

    funda=pd.merge(funda,step1g_xrd,left_on='ageipo',right_index=True,how='left')
    funda=pd.merge(funda,step1g_xsga,left_on='ageipo',right_index=True,how='left')

    funda['logxsga']=np.where(funda.xsga>0,np.log(funda.xsga),funda.xsga)
    funda['logxrd']=np.where(funda.xrd>0,np.log(funda.xrd),funda.xrd)

    c=[]
    for i in funda.gvkey.unique():
        #for negative xrd values, they are not transformed to log, but still use grate to add or subtract as grate is not that large.
        g=funda[funda.gvkey==i]
        a=g[np.isnan(g.xrd)].indexl
        if a.shape[0]!=0:
            for idx in sorted(a,reverse=True):#reverse makes idx start form the newest missing value
                idx2=g[g.indexl>idx].xrd.first_valid_index()
                b=g[g.indexl==idx2].logxrd.values-g[(g.indexl<=idx2) & (g.indexl>idx)].grate_x.sum()
                g['logxrd']=np.where(g.indexl==idx,b,g.logxrd)
        else:
            g=g
        c.extend(g.logxrd)
    funda['logxrd']=c

    c=[]
    for i in funda.gvkey.unique():
        #for negative xrd values, they are not transformed to log, but still use grate to add or subtract as grate is not that latge.
        g=funda[funda.gvkey==i]
        a=g[np.isnan(g.xsga)].indexl
        if a.shape[0]!=0:
            for idx in sorted(a,reverse=True):#reverse makes idx start form the newest missing value
                idx2=g[g.indexl>idx].xsga.first_valid_index()
                b=g[g.indexl==idx2].logxsga.values-g[(g.indexl<=idx2) & (g.indexl>idx)].grate_y.sum()
                g['logxsga']=np.where(g.indexl==idx,b,g.logxsga)
        else:
            g=g
        c.extend(g.logxsga)
    funda['logxsga']=c

    # combine founding information
    ftable = pd.read_excel('scopeProject/data/raw/ritterIPO/IPO-age.xlsx',usecols=['CUSIP', 'offer date','Founding'],dtype={'offer date':str,'CUSIP':str})
    ftable['Founding']=np.where(ftable.Founding==-99, np.nan, ftable.Founding)
    ftable['Founding']=np.where(ftable.Founding==-9, np.nan, ftable.Founding)
    ftable['Founding']=np.where(ftable.Founding==201, 2013, ftable.Founding)
    ftable['Founding']=np.where(ftable.Founding==0, np.nan, ftable.Founding) # Anshu added this.

    ftable.dropna(inplace=True)
    ftable['Founding']=ftable.Founding.astype('int32')
    funda['CUSIP']=funda.cusip.astype(str)
    funda=pd.merge(funda,ftable,on='CUSIP',how='left')

    funda.loc[funda['ipodate'].notnull(),'foundingf']=funda.firstcomp-(funda.ipodate.dt.year-8)
    funda['foundingf']=np.where(funda.foundingf<=0,funda.firstcomp, (funda.ipodate.dt.year-8))
    ##merged by CUSIP, gvkey==19538, Founding=2016, while firstcomp=1987.
    funda['Founding']=np.where(funda.Founding>funda.firstcomp,np.nan,funda.Founding)
    funda['Founding']=np.where(np.isnan(funda.Founding),funda.foundingf,funda.Founding)
    funda['Founding']=np.where(np.isnan(funda.Founding),funda.firstcomp,funda.Founding)
    funda=funda.sort_values(by=['gvkey','fyear'])

    gv=funda.gvkey.unique()         
    fy=funda.groupby('gvkey',as_index=False)['Founding'].first()
    cusip=funda.groupby('gvkey').cusip.unique().apply(lambda x: list(set(x))[0])
    a=pd.DataFrame({'gvkey':gv,'fyear':fy.Founding,'cusip':cusip.values})
    funda=pd.concat([funda, a])

    funda=funda.sort_values(by=['gvkey','fyear','count'],ascending=True,na_position='first').drop_duplicates(subset=['fyear','gvkey'],keep='last')
    funda.index=pd.to_datetime(funda.fyear, format='%Y')
    funda=funda.groupby('gvkey',as_index=False).resample("Y").ffill()    

    funda['fyear']=funda.index.get_level_values('fyear')  
    funda['fyear']=funda.fyear.dt.year
    funda=funda.reset_index(drop=True)
    funda=funda.sort_values(by=['gvkey','fyear'],ascending=True,na_position='last').drop_duplicates(subset=['gvkey','fyear'],keep='first')
    funda=funda.reset_index(drop=True)
    funda['indexl']=funda.index.values

    funda['step2g_xrd']=step2g_xrd[0]
    funda['step2g_xsga']=step2g_xsga[0]
    funda['firstcomp']=funda.groupby('gvkey',as_index=False).firstcomp.bfill()
    funda['ipodate']=funda.groupby('gvkey',as_index=False).ipodate.bfill()

    c=[]
    for i in funda.gvkey.unique():
        g=funda[funda.gvkey==i]
        a=g[np.isnan(g.logxrd)].indexl
        if a.shape[0]!=0:
            for idx in sorted(a,reverse=True):#reverse makes idx start form the newest missing value
                idx2=g[g.indexl>idx].logxrd.first_valid_index()
                if idx2 is not None:
                    b=g[g.indexl==idx2].logxrd.values-g[(g.indexl<=idx2) & (g.indexl>idx)].step2g_xrd.sum()
                    g['logxrd']=np.where(g.indexl==idx,b,g.logxrd)
        else:
            g=g
        c.extend(g.logxrd)
    funda['logxrd']=c

    c=[]
    for i in funda.gvkey.unique():
        #for negative xrd values, they are not transformed to log, but still use grate to add or subtract as grate is not that latge.
        g=funda[funda.gvkey==i]
        a=g[np.isnan(g.logxsga)].indexl
        if a.shape[0]!=0:
            for idx in sorted(a,reverse=True):#reverse makes idx start form the newest missing value
                idx2=g[g.indexl>idx].logxsga.first_valid_index()
                if idx2 is not None:
                    b=g[g.indexl==idx2].logxsga.values-g[(g.indexl<=idx2) & (g.indexl>idx)].step2g_xsga.sum()
                    g['logxsga']=np.where(g.indexl==idx,b,g.logxsga)
        else:
            g=g
        c.extend(g.logxsga)
    funda['logxsga']=c

    funda['logxsga']=funda.logxsga.astype('float')
    funda['xsga']=np.where(~(funda.xsga<0),np.exp(funda.logxsga),funda.xsga)
    funda['logxrd']=funda.logxrd.astype('float')
    funda['xrd']=np.where(~(funda.xrd<0),np.exp(funda.logxrd),funda.xrd)
    funda=funda.sort_values(by=['gvkey','fyear','count'],ascending=True,na_position='first').drop_duplicates(subset=['fyear','gvkey'],keep='last')

    ##parameters for d_{XRD} from Ewens   
    sicg1=[3714,3716,3750,3751,3792,4813,4812,4841,4833,4832]+list(range(100,1000))+list(range(2000,2400))+list(range(2700,2750))+list(range(2770,2800))+list(range(3100,3200))+list(range(3940,3990))+list(range(2500,2520))+list(range(2590,2600))+list(range(3630,3660))+list(range(3710,3712))+list(range(3900,3940))+list(range(3990,4000))+list(range(5000,6000))+list(range(7200,7300))+list(range(7600,7700))+list(range(8000,8100))
    sicg2=list(range(2520,2590))+list(range(2600,2700))+list(range(2750,2770))+list(range(2800,2830))+list(range(2840,2900))+list(range(3000,3100))+list(range(3200,3570))+list(range(3580,3622))+list(range(3623,3630))+list(range(3700,3710))+list(range(3712,3714))+list(range(3715,3716))+list(range(3717,3750))+list(range(3752,3792))+list(range(3793,3800))+list(range(3860,3900))+list(range(1200,1400))+list(range(2900,3000))+list(range(4900,4950))
    sicg3=[3622,7391]+list(range(3570,3580))+list(range(3660,3693))+list(range(3694,3700))+list(range(3810,3840))+list(range(7370,7380))+list(range(8730,8735))+list(range(4800,4900))
    sicg4=list(range(2830,2840))+list(range(3693,3694))+list(range(3840,3860))

    funda["theta_g2"]=np.where(funda['sich'].isin(sicg1), 0.33,np.where(
            funda['sich'].isin(sicg2), 0.42,np.where(
                    funda['sich'].isin(sicg3), 0.46,np.where(
                            funda.sich.isin(sicg4),0.34,0.3))))

    funda['gamma_o2']=np.where(funda['sich'].isin(sicg1), 0.19,np.where(
            funda['sich'].isin(sicg2), 0.22,np.where(
                    funda['sich'].isin(sicg3), 0.44,np.where(
                            funda.sich.isin(sicg4),0.49,0.34))))

    funda['kcap_v2']=0
    funda['ocap_v2']=0

    def genkcap_single(group):
        """Calculate knowledge capital for a single company group"""
        result = group.copy()
        n = len(result)
        for i in range(1, n):
            result.iloc[i, 0] = result.iloc[i-1, 0] * (1 - result.iloc[i, 1]) + result.iloc[i, 2]
        return result

    def genocap_single(group):
        """Calculate organizational capital for a single company group"""
        result = group.copy()
        n = len(result)
        for i in range(1, n):
            result.iloc[i, 0] = result.iloc[i-1, 0] * 0.8 + result.iloc[i, 2] * result.iloc[i, 1]
        return result

    def process_batch(gvkeys_batch, funda):
        """Process a batch of gvkeys to calculate kcap and ocap"""
        result_df = pd.DataFrame()
        
        for gvkey in gvkeys_batch:
            # Extract company data
            company_data = funda[funda['gvkey'] == gvkey].copy()
            
            if len(company_data) > 0:
                # Knowledge capital calculation
                k_data = company_data[['kcap_v2', 'theta_g2', 'xrd']].copy()
                k_result = genkcap_single(k_data)
                company_data['kcap_v2'] = k_result['kcap_v2']
                
                # Organizational capital calculation
                o_data = company_data[['ocap_v2', 'gamma_o2', 'xsga']].copy()
                o_result = genocap_single(o_data)
                company_data['ocap_v2'] = o_result['ocap_v2']
                
                # Keep only relevant columns for the final result
                result_df = pd.concat([result_df, company_data[['gvkey', 'fyear', 'kcap_v2', 'ocap_v2']]])
        
        return result_df

    def calculate_intangible_capital_batched(funda, batch_size=500, max_workers=4):
        """
        Calculate intangible capital using batched processing
        
        Parameters:
        - funda: DataFrame with the required columns
        - batch_size: Number of companies to process in each batch
        - max_workers: Number of parallel workers (careful with memory usage)
        
        Returns:
        - Updated DataFrame with calculated kcap_v2 and ocap_v2
        """
        # Ensure required columns exist
        if not all(col in funda.columns for col in ['gvkey', 'kcap_v2', 'ocap_v2', 'theta_g2', 'gamma_o2', 'xrd', 'xsga']):
            raise ValueError("Missing required columns in the input DataFrame")
        
        # Get unique companies
        unique_gvkeys = funda['gvkey'].unique()
        total_companies = len(unique_gvkeys)
        print(f"Processing {total_companies} companies in batches of {batch_size}")
        
        # Split into batches
        batches = [unique_gvkeys[i:i + batch_size] for i in range(0, total_companies, batch_size)]
        
        results = []

        for batch_idx, gvkeys_batch in enumerate(tqdm(batches, desc="Processing company batches")):
            batch_result = process_batch(gvkeys_batch, funda)
            results.append(batch_result)
        
        # Combine results
        result_df = pd.concat(results, ignore_index=True)
        
        # Merge results back to original dataframe
        funda_result = funda.drop(['kcap_v2', 'ocap_v2'], axis=1)
        funda_result = pd.merge(
            funda_result, 
            result_df[['gvkey', 'fyear', 'kcap_v2', 'ocap_v2']], 
            on=['gvkey', 'fyear'],
            how='left'
        )
        
        return funda_result
    
    with warnings.catch_warnings():
        warnings.filterwarnings('ignore', category=FutureWarning)
        funda = calculate_intangible_capital_batched(funda, batch_size=200)
    
    funda=funda[funda['count']>=0]
    tokeep=funda[['gvkey','fyear','kcap_v2','ocap_v2']]

    tokeep.to_csv('scopeProject/data/raw/epwIntans/intan_updated_2024.csv', index=False)