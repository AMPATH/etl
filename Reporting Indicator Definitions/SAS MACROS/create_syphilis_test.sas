


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_syphilis_test();

PROC IMPORT OUT=syphilis_test
            DATAFILE= "C:\DATA\CSV DATASETS\syphilis_test_ordered.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data syphilis_test1;
Format syphilistest_date ddmmyy10.;
set syphilis_test;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
syphilistest_date=mdy(mm,dd,yy);
syphilistest='ordered';
drop value_coded dd mm yy obs_datetime concept_id ;
run;


proc sort data=syphilis_test1 nodupkey dupout=dupsyphilis_test1 out=syphilis_test_final; 
by person_id syphilistest_date; run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE syphilis_test syphilis_test1 ;
		RUN;
   		QUIT;


%MEND create_syphilis_test;

%create_syphilis_test;
