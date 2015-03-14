


Libname p 'C:\DATA\CSV DATASETS';

PROC IMPORT OUT= WORK.dead_combined 
            DATAFILE= "C:\DATA\CSV DATASETS\death_combined2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data dead_p2;
format death_rprt ddmmyy10.; format death_date1 ddmmyy10.;
set dead_combined;
dd=substr(date_created,9,2);
mm=substr(date_created,6,2);
yy=substr(date_created,1,4);
dd1=substr(death_date,9,2);
mm1=substr(death_date,6,2);
yy1=substr(death_date,1,4);
death_rprt=mdy(mm,dd,yy);
death_date1=mdy(mm1,dd1,yy1);
keep person_id death_rprt  death_date1;
*rename death_rprt=obs_datetime;
*if value_datetime eq '';
run;

proc sort data=dead_p2 nodupkey; by person_id death_rprt; run;

data dublicates;
set dead_p2;
by person_id;
retain count ;
if first.person_id then do;
count=0;
end;
count=count+1;
run;

data first_report(rename=death_rprt=first_rptdate);
set dublicates;
by person_id;
if first.person_id;
keep person_id death_rprt;
run;

data last_report(rename=(death_rprt=last_rptdate count=last_count));
set dublicates;
by person_id;
if last.person_id;
keep person_id death_rprt count;
run;

data rpt_deaths;
merge first_report(in=a) last_report(in=b) dublicates(drop=count);
by person_id;
if a and b;
if first_rptdate=last_rptdate then reported='TRUE ' ;ELSE reported='FALSE';
run;

data rpt_deaths1;
set rpt_deaths;
if reported='FALSE' and ((first_rptdate-30)<=death_date1) and (death_date1<=(first_rptdate+30)) then reported1='MAYB_TRUE';
else if reported='TRUE ' then reported1='TRUE';
else reported1='FALSE';
run;

proc freq data=rpt_deaths1; table reported1;run;
proc freq data=rpt_deaths; table reported;run;











data dublicates;
set dead_p2;
by person_id;
retain count ;
if first.person_id then do;
count=0;
end;
count=count+1;
run;

proc sort data=dead_persons  nodupkey ;by person_id obs_datetime;run;




proc sort data=dead_d nodupkey; by person_id; run;
proc sort data=dead_p nodupkey; by person_id; run;

data dead_dp;
merge dead_d(in=a) dead_p(in=b);
by person_id;
run;


