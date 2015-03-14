


Libname p 'C:\DATA\CSV DATASETS';
/*
data dead_persons;
set p.death_combine2;
run;
*/
PROC IMPORT OUT= WORK.dead_persons 
            DATAFILE= "C:\DATA\CSV DATASETS\death_combined.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


proc sort data=dead_persons  nodupkey ;by person_id obs_datetime;run;

data dead_d;
format death_date1 ddmmyy10.;
set dead_persons;
dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
death_date1=mdy(mm,dd,yy);
keep person_id death_date1 concept_id;
*rename death_date1=value_datetime;
if death_date1 ne '';
run;



data dead_p;
format death_rprt ddmmyy10.;
set dead_persons;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
death_rprt=mdy(mm,dd,yy);
keep person_id death_rprt concept_id;
*rename death_rprt=obs_datetime;
*if value_datetime eq '';
run;


proc sort data=dead_d nodupkey; by person_id; run;
proc sort data=dead_p nodupkey; by person_id; run;

data dead_dp;
merge dead_d(in=a) dead_p(in=b);
by person_id;
run;
