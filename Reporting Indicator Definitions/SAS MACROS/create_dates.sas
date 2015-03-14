


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_dates();	


PROC IMPORT OUT= WORK.dates1 
            DATAFILE= "C:\DATA\CSV DATASETS\dates1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.dates2 
            DATAFILE= "C:\DATA\CSV DATASETS\dates2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data dates;
set dates1 dates2;
run;

data dates3;
format appdate conceptdate  datecreated ddmmyy10.;
set dates;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
dd1=substr(value_datetime,9,2);
mm1=substr(value_datetime,6,2);
yy1=substr(value_datetime,1,4);
dd2=substr(date_created,9,2);
mm2=substr(date_created,6,2);
yy2=substr(date_created,1,4);
appdate=mdy(mm,dd,yy);
conceptdate=mdy(mm1,dd1,yy1);
datecreated=mdy(mm2,dd2,yy2);
drop mm1 dd1 yy1 mm dd yy obs_datetime value_datetime obs_id dd2 mm2 yy2 date_created ;
run;

/* app_date and return_visit date*/

data date5096;
set dates3;
if concept_id=5096;
rename conceptdate=return_visit_date;
drop concept_id;
run; 

proc sort data=date5096 nodupkey out=appdate_returndate_final; by person_id appdate return_visit_date datecreated; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE dates1 dates2 dates dates3 date5096;
		RUN;
   		QUIT;


%MEND create_dates;
%create_dates;
