


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_CD4_tests();	

PROC IMPORT OUT= WORK.CD4_tests 
            DATAFILE= "C:\DATA\CSV DATASETS\CD4 tests.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data CD4_tests1;
Format CD4_date ddmmyy10.;
set CD4_tests;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
cd4_date=mdy(mm,dd,yy);
cd4_test=1;
drop concept_id value_coded dd mm yy obs_datetime ;
run;


proc sort data=CD4_tests1 nodupkey out=CD4_tests_final dupout=dupcd4_tests ; by person_id cd4_date; run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE CD4_tests  CD4_tests1 ;
		RUN;
   		QUIT;


%MEND create_CD4_tests;
%create_CD4_tests;
