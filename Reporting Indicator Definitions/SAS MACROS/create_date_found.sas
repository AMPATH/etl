


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_date_found();	


PROC IMPORT OUT= WORK.date_found 
            DATAFILE= "C:\DATA\CSV DATASETS\date_found.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data dates3;
format appdate date_found   ddmmyy10.;
set date_found ;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
dd1=substr(value_datetime,9,2);
mm1=substr(value_datetime,6,2);
yy1=substr(value_datetime,1,4);
appdate=mdy(mm,dd,yy);
date_found=mdy(mm1,dd1,yy1);
drop mm1 dd1 yy1 mm dd yy obs_datetime value_datetime obs_id  ;
run;

/* app_date and return_visit date*/


proc sort data=dates3 nodupkey out=date_found_final; by person_id appdate date_found; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE date_found   dates3 ;
		RUN;
   		QUIT;


%MEND create_date_found;
%create_date_found;
