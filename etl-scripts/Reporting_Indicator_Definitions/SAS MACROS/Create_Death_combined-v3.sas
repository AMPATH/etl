


Libname p 'C:\DATA\CSV DATASETS';

PROC IMPORT OUT= WORK.dead_obs 
            DATAFILE= "C:\DATA\CSV DATASETS\death_obs.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


PROC IMPORT OUT= WORK.dead_persons 
            DATAFILE= "C:\DATA\CSV DATASETS\death_persons.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data death_obs1;
format death_rprt ddmmyy10.; format death_rprt1 ddmmyy10.;
set dead_obs;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
dd1=substr(value_datetime,9,2);
mm1=substr(value_datetime,6,2);
yy1=substr(value_datetime,1,4);
death_rprt=mdy(mm,dd,yy);
death_rprt1=mdy(mm1,dd1,yy1);
keep person_id death_rprt death_rprt1 concept_id ;
run;

proc sort data=death_obs1 nodupkey out=death_obs; by person_id death_rprt; run;


data death_persons;
format death_rprt ddmmyy10.; format death_date1 ddmmyy10.;
set dead_persons;
dd=substr(date_created,9,2);
mm=substr(date_created,6,2);
yy=substr(date_created,1,4);
dd1=substr(death_date,9,2);
mm1=substr(death_date,6,2);
yy1=substr(death_date,1,4);
death_rprt=mdy(mm,dd,yy);
death_date1=mdy(mm1,dd1,yy1);
keep person_id death_rprt  death_date1;
run;

proc sort data=death_persons nodupkey; by person_id death_rprt; run;



data death_combined(rename=death_rprt=persons_rptdate);
merge death_persons(in=a) death_obs(in=b drop=concept_id);
by person_id;
if b or a;
run;

proc sort data=death_obs1; by person_id ; run;


data death_notobs;
merge death_combined(in=a where=(death_date1 eq .)) death_obs1(in=b where=(concept_id=1570));
by person_id;
if a ;
if death_rprt1 ne . then do;death_date1=death_rprt1;end;else
if death_date1 eq . then do;death_date1=persons_rptdate;end;
run;


data death_notobs(rename=death_rprt=persons_rptdate);
merge death_persons(in=a) death_obs(in=b);
by person_id;
if b and not a;
run;

data death_combined;
set death_obs death_notobs;
by person_id;
run;









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

/* Mine */
data rpt_deaths1;
set rpt_deaths;
if reported='FALSE' and death_date1 ne . and (abs(first_rptdate-death_date1)<=91) then reported1='TRUE';
else if reported='TRUE ' then reported1='TRUE';
else reported1='FALSE';
run;

proc freq data=rpt_deaths1; table reported1;run;

proc sort data=conf_dead;by patient_id;run;
proc sort data=rpt_deaths1;by person_id;run;

data rpt_deaths2 www1 www2;
merge rpt_deaths1(in=b) conf_dead (in=a rename=patient_id=person_id);
by person_id;
if a and b then output rpt_deaths2;
if b and not a then output www1;
if a and not b then output www2;
run;











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



