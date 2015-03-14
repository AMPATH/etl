


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_CD4_percent();	

PROC IMPORT OUT= WORK.cd4_percent
            DATAFILE= "C:\DATA\CSV DATASETS\cd4_percent.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data cd4_percent1;
Format CD4_percent 7. CD4_date ddmmyy10.;
set cd4_percent;
cd4_percent=value_numeric;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
cd4_date=mdy(mm,dd,yy);
*if cd4_date le mdy(12,31,2008);
keep person_id CD4_percent CD4_date;
run;


proc sort data=CD4_percent1 nodupkey out=cd4_percent_final dupout=dupcd4 ; by person_id cd4_date; run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE cd4_percent cd4_percent1;
		RUN;
   		QUIT;


%MEND create_CD4_percent;
%create_CD4_percent;
