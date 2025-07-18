%let year_start = 1950;
%let year_end = 2020;

*%let gap = 6;

* this file searches for contemporaneous links. it does not account for;
* fiscal-year reporting lag as in Fama-French 1993;
* each link is valid for the period between Dec 31 (fyear-1) and Dec 31 (fyear);

* this creates 'scopeProject/data/raw/compustat/compustat_crsp_link.csv'

* filter CCM links;

proc sql;
  create table lnk as select * from crsp.ccmxpf_lnkhist where
  linktype in ("LU", "LC") and
  /* "LU" and "LC" are the two highest data quality categories for links. */

  /* Extend the period to deal with fiscal year issues */
  /* Note that the ".B" and ".E" missing value codes represent the   */
  /* earliest possible beginning date and latest possible end date   */
  /* of the Link Date range, respectively.                           */
   (&year_end.+1 >=year(linkdt) or linkdt=.B) and
   (&year_start.-1 <=year(linkenddt) or linkenddt=.E)
    /* primary link assigned by Compustat or CRSP */
  and linkprim in ("P", "C")
    order by gvkey, linkdt;
quit;

* merge onto Compustat;

proc sql;
  create table mydata as select * from lnk, 
    comp.funda (keep=gvkey fyear tic cik datadate indfmt datafmt popsrc consol) as cst 
  where
    indfmt='INDL' /* FS - Financial Services ('INDL' for industrial ) */
    and datafmt='STD' /* STD - Standardized */
    and popsrc='D' /* D - Domestic (USA, Canada and ADRs)*/
    and consol='C' /* C - Consolidated. Parent and Subsidiary accounts combined */
    and lnk.gvkey=cst.gvkey
    and (&year_start. <=fyear <=&year_end.) 
	and (linkdt < mdy(12, 31, fyear - 1) or linkdt = .B)
    and (linkenddt > mdy(12, 31, fyear) or linkenddt = .E);
quit;

/* Verify that we have unique gvkey-permco and gvkey-permno links */
proc sort data=mydata nodupkey; by lpermco gvkey datadate; run;

data gvkey_permco_permno; set mydata;
   rename lpermno=permno;
   rename lpermco=permco;
run;

* download this link

* download Compustat Annual

* download CRSP monthly

* finish the merge in 0_clean_cs_crsp.py

proc download data=gvkey_permco_permno out=compustat_crsp_link; run;

*endrsubmit;
*signoff;