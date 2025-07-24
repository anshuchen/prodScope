libname common '/wrds/sec/sasdata/common/';
options encoding='utf-8';

proc sql;
create table cikgvkey as select * from common.wciklink_gvkey;
quit;

proc export data=cikgvkey
    outfile="/home/WCIKLINK_GVKEY.csv"
    dbms=csvs
    replace;
run;