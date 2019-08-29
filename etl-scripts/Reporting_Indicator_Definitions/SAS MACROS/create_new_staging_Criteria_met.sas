


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_New_staging_Criteria_met();	


PROC IMPORT OUT= WORK.new_WHO_criteria 
            DATAFILE= "C:\DATA\CSV DATASETS\new_staging_criteria_met.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data new_WHO_criteria1;
Format apptdate ddmmyy10. new_WHO_criteria;
set new_WHO_criteria;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
apptdate=mdy(mm,dd,yy);
if value_numeric=1 then new_staging_criteria=1;
else new_staging_criteria=0;
if concept_id=1942 then staging='CDC';
else if concept_id=1943 then staging='WHO';
keep person_id apptdate staging new_staging_criteria;
run;

proc sort data=new_WHO_criteria1 nodupkey out=new_WHO_criteria_final; by person_id apptdate staging new_staging_criteria; run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE new_WHO_criteria  new_WHO_criteria1;
		RUN;
   		QUIT;


%MEND create_New_staging_Criteria_met;
%create_New_staging_Criteria_met;

