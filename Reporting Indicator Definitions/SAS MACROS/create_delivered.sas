


PROC OPTIONS OPTION = MACRO; RUN;

%MACRO create_delivered();	

libname eva 'C:\DATA\CSV DATASETS';



data del;
format del_date obs_date  ddmmyy10.;
set eva.delivered ;
if value_datetime ne '';
dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
dd1=substr(obs_datetime,9,2);
mm1=substr(obs_datetime,6,2);
yy1=substr(obs_datetime,1,4);
obs_date=mdy(mm1,dd1,yy1);
del_date=mdy(mm,dd,yy);
*if mdy(10,31,2008) >=del_date>=mdy(03,01,2008);
delivered='Y';
drop dd mm yy value_datetime dd1 mm1 yy1 obs_datetime;
run;



data del1;
Format delivered $1. obs_date ddmmyy10.;
set eva.delivered ;
if value_datetime='';
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
obs_date=mdy(mm,dd,yy);
*if obs_date>mdy(03,01,2008);
delivered='Y';
keep person_id delivered obs_date ;
run;


proc sort data=del; by person_id obs_date; run;

proc sort data=del1; by person_id obs_date; run;



data del_date;
merge del(in=a) del1;
by person_id;
if a ;
run;


data del_nodate;
merge del(in=a) del1(in=b);
by person_id;
if b and not a ;
run;



proc sort data=del_date nodupkey; by person_id ; run;

proc sort data=del_nodate nodupkey; by person_id ; run;


data delivery_final;
set del_date del_nodate;
by person_id;
keep person_id delivered obs_date del_date;
run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE del del1 del_nodate del_date;
		RUN;
   		QUIT;



%MEND create_delivered;

%create_delivered;
