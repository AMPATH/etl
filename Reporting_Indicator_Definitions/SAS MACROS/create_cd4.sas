


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_CD4();	

PROC IMPORT OUT= WORK.CD41 
            DATAFILE= "C:\DATA\CSV DATASETS\CD4.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data CD42;
Format CD4_count 7. CD4_date ddmmyy10.;
set CD41;
cd4_count=value_numeric;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
cd4_date=mdy(mm,dd,yy);
*if cd4_date le mdy(12,31,2008);
keep person_id CD4_count CD4_date;
run;


proc sort data=CD42 nodupkey out=cd4_final dupout=dupcd4 ; by person_id cd4_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE cd41 cd4 cd42;
		RUN;
   		QUIT;


%MEND create_CD4;
%create_CD4;
