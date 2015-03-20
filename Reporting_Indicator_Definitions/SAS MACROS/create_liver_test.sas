


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_liver_test();

PROC IMPORT OUT=liver_test
            DATAFILE= "C:\DATA\CSV DATASETS\liver_test_ordered.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data liver_test1;
Format livertest_date ddmmyy10.;
set liver_test;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
livertest_date=mdy(mm,dd,yy);
livertest=1;
drop value_coded dd mm yy obs_datetime concept_id ;
run;


proc sort data=liver_test1 nodupkey dupout=dupliver_test1 out=liver_test_final; 
by person_id livertest_date; run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE liver_test liver_test1 dupliver_test1 ;
		RUN;
   		QUIT;


%MEND create_liver_test;

%create_liver_test;
