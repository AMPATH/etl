


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_deaths();	

PROC IMPORT OUT= WORK.dead_persons 
            DATAFILE= "C:\DATA\DATA SETS\deaths1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.deaths1 
            DATAFILE= "C:\DATA\DATA SETS\deaths.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data dead_d;
format death_date1 ddmmyy10.;
set dead_persons;
/*
dd=substr(death_date,9,2);
mm=substr(death_date,6,2);
yy=substr(death_date,1,4);
death_date1=mdy(mm,dd,yy);
*/
YY=scan(death_date,1, '-');
MM=scan(death_date,2, '-');
DD=scan(scan(death_date,1, ' '),-1, '-');
death_date1=mdy(mm,dd,yy);

keep person_id death_date1;
rename death_date1=death_date;
if death_date ne '';
run;


data dead_y;
format death_date1 ddmmyy10.;
set dead_persons;
/*
dd=substr(death_date,9,2);
mm=substr(death_date,6,2);
yy=substr(death_date,1,4);
death_date1=mdy(mm,dd,yy);
*/

YY=scan(death_date,1, '-');
MM=scan(death_date,2, '-');
DD=scan(scan(death_date,1, ' '),-1, '-');
death_date1=mdy(mm,dd,yyyy);
keep person_id death_date1;
rename death_date1=death_date;
if death_date eq '';
run;


/*To get deaths that date of death is known*/
data death_knownd;
Format death_date ddmmyy10.;
set deaths1;
if concept_id=1570;
/*
dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
death_date=mdy(mm,dd,yy);
*/
yy=scan(value_datetime,1, '/');
mm=scan(value_datetime,2, '/');
dd=scan(scan(value_datetime,1, ' '),-1, '/');
death_date=mdy(mm,dd,yyyy);
keep person_id death_date;
run;



proc sort data=dead_d; by person_id;run;
proc sort data=death_knownd; by person_id;run;


/*To merge deaths in the person table and the obs table*/

data death_n;
merge dead_d(in=a) death_knownd(in=b);
by person_id;
if b and not a;
run;


data death_a;
set dead_d death_n;
by person_id;
run;


/*To get deaths that date of death is unknown*/
data death_unknownd;
format death_date ddmmyy10.;
set deaths1;
if concept_id ne 1570;
/*
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
death_date=mdy(mm,dd,yy);

mm=scan(obs_datetime,1, '/');
dd=scan(obs_datetime,2, '/');
yyyy=scan(scan(obs_datetime,1, ' '),-1, '/');
death_date=mdy(mm,dd,yyyy);
*/
death_date=obs_datetime;
keep person_id death_date;
run;

proc sort data=death_unknownd; by person_id death_date; run;

data death_first;
set death_unknownd;
by person_id death_date;
if first.person_id;
run;

data deaths_o;
merge death_a(in=a) death_first(in=b);
by person_id;
if b and not a;
run;

data death_aa;
set deaths_o death_a;
by person_id;
run;


proc sort data=death_aa; by person_id; run;
proc sort data=Dead_y; by person_id; run;

data death_t;
merge death_aa(in=a) dead_y(in=b);
by person_id;
if b and not a;
run;


data deaths_all;
set death_aa death_t;
by person_id;
run;


proc sort data=deaths_all out=deaths_final nodupkey ; by person_id  ; run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE dead_d dead_persons dead_y deaths1 deaths_o death_a death_aa death_first
		death_knownd death_n death_t death_unknownd deaths_all;
		RUN;
   		QUIT;


%MEND create_deaths;
%create_deaths;
