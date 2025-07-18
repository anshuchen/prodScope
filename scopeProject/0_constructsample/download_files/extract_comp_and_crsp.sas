* extract Compustat Annual and CRSP Monthly;

libname comp '/wrds/comp/sasdata/d_na';

proc sql;
create table compa_for_crspmerge as select f.gvkey, year(f.datadate) as year, fyear, f.datadate, cusip, naicsh, revt, at, sale, cogs, 
xopr, ib, emp, ipodate, prcc_f, dvpsx_f as div, csho, xrd, capx, ppegt, ppent
from comp.funda(where=(indfmt='INDL' & datafmt='STD' & popsrc='D' & consol='C' & CURCD='USD' & missing(gvkey)=0 &
FIC='USA')) as f, compd.company as c
where f.gvkey = c.gvkey;quit;

proc export data=compa_for_crspmerge
    outfile="/home/compa_for_crspmerge.csv"
    dbms=csvs
    replace;
run;

proc sql;
create table crsp_monthly as 
	select msf.permco, msf.permno, msf.mthcaldt AS date, 
         msf.mthret AS ret, msf.shrout,
         msf.mthprc AS altprc,
         ssih.primaryexch, ssih.siccd
	FROM crsp.msf_v2 AS msf, crsp.stksecurityinfohist AS ssih
	where msf.permno = ssih.permno
	and ssih.secinfostartdt <= msf.mthcaldt
	and msf.mthcaldt <= ssih.secinfoenddt
	and ssih.sharetype = 'NS'
	and ssih.securitytype = 'EQTY'
	and ssih.securitysubtype = 'COM'
	and ssih.usincflg = 'Y'
	and ssih.issuertype in ('ACOR', 'CORP')
	and ssih.primaryexch in ('N', 'A', 'Q')
	and ssih.conditionaltype in ('RW', 'NW')
	and ssih.tradingstatusflg = 'A';quit;

proc export data=crsp_monthly
    outfile="/home/crsp_monthly.csv"
    dbms=csvs
    replace;
run;