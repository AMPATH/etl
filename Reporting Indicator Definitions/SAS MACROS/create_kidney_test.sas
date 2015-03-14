


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_kidney_test();

PROC IMPORT OUT=kidney_test
            DATAFILE= "C:\DATA\CSV DATASETS\kidney_test_ordered.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data kidney_test1;
Format kidneytest_date ddmmyy10.;
set kidney_test;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
kidneytest_date=mdy(mm,dd,yy);
kidneytest=1;
drop value_coded dd mm yy obs_datetime concept_id ;
run;


proc sort data=kidney_test1 nodupkey dupout=dupkidney_test1 out=kidney_test_final; 
by person_id kidneytest_date; run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE kidney_test kidney_test1 ;
		RUN;
   		QUIT;


%MEND create_kidney_test;

%create_kidney_test;
