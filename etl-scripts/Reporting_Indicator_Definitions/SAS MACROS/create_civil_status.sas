


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_civil_status();	

PROC IMPORT OUT= WORK.civil_out 
            DATAFILE= "C:\DATA\DATA SETS\Civil_status.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data civil;
format app_date ddmmyy10. ;
set civil_out;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);

keep person_id app_date location_id  value_coded;
run;

proc sort data=civil out=civilstatus_final nodupkey; by person_id app_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE civil civil_out ;
		RUN;
   		QUIT;


%MEND create_civil_status;
%create_civil_status;
