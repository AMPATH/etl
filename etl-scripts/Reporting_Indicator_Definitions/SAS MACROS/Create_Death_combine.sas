


Libname p 'C:\DATA\CSV DATASETS';

data dead_persons;
set p.death_combine2;
run;

/*
PROC IMPORT OUT= WORK.dead_persons 
            DATAFILE= "C:\DATA\CSV DATASETS\death_combine2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/
data dead_d;
format death_date1 ddmmyy10.;
set dead_persons;
dd=substr(death_date,9,2);
mm=substr(death_date,6,2);
yy=substr(death_date,1,4);
death_date1=mdy(mm,dd,yy);
keep person_id death_date1;
rename death_date1=death_date;
if death_date ne '';
run;








data dead_y;
format death_date1 dob ddmmyy10.;
set dead_persons;
dd=substr(death_date,9,2);
mm=substr(death_date,6,2);
yy=substr(death_date,1,4);
dd1=substr(birthdate,9,2);
mm1=substr(birthdate,6,2);
yy1=substr(birthdate,1,4);
death_date1=mdy(mm,dd,yy);
dob=mdy(mm1,dd1,yy1);
keep person_id death_date1 dob;
rename death_date1=death_date;
if death_date eq '';
run;

proc sort data=dead_persons nodupkey; by person_id; run;
