


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_arvstart_dates();	


PROC IMPORT OUT= WORK.relationship 
            DATAFILE= "C:\DATA\CSV DATASETS\relationship.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data relationship;
format encounter_date  date_started ddmmyy10.;
set arvs_starts;
dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
dd1=substr(obs_datetime,9,2);
mm1=substr(obs_datetime,6,2);
yy1=substr(obs_datetime,1,4);
date_started=mdy(mm,dd,yy);
encounter_date=mdy(mm1,dd1,yy1);

drop mm1 dd1 yy1 mm dd yy obs_datetime value_datetime;
run;




proc sort data=arvstart_dates nodupkey out=arvstart_dates_final; by person_id encounter_date; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE arvstart_dates arvs_starts arvs_starts ;
		RUN;
   		QUIT;


%MEND create_arvstart_dates;
%create_arvstart_dates;
