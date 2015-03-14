


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_who_paeds_staging();	


PROC IMPORT OUT= WORK.paeds_staging 
            DATAFILE= "C:\DATA\CSV DATASETS\AIDS_DEFINING_ILLNESSES.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data paeds_staging1;
Format staging_date ddmmyy10. staging;
set paeds_staging;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
staging_date=mdy(mm,dd,yy);
if value_coded=1220 then staging='WHO STAGE 1 PEDS';
else if value_coded=1221 then staging='WHO STAGE 2 PEDS';
else if value_coded=1222 then staging='WHO STAGE 3 PEDS';
else if value_coded=1223 then staging='WHO STAGE 4 PEDS';
if staging  ne '';
keep person_id staging_date staging;
run;

proc sort data=paeds_staging1 nodupkey  out=paeds_staging_final dupout=staging_dup1; by person_id staging_date  ; run;
proc sort data=paeds_staging1 nodupkey  out=paeds_staging_final dupout=staging_dup2; by person_id staging_date staging ; run;

data paeds_staging_dup;
merge staging_dup1(in=a) staging_dup2(in=b);
by person_id staging_date;
if a and not b;
run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE paeds_staging  paeds_staging1 staging_dup1 staging_dup2 ;
		RUN;
   		QUIT;


%MEND create_who_paeds_staging;
%create_who_paeds_staging;;

