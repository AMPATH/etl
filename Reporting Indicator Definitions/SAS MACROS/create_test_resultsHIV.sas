


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_test_resultsHIV();

PROC IMPORT OUT= WORK.test_results 
            DATAFILE= "C:\DATA\CSV DATASETS\test_resultsHIV.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

	

data hiv_tests;
Format hiv_tests $15. testdate ddmmyy10.;
set test_results;
where concept_id in(1030,1040,1042,1357,1361);
if value_coded =1138 then do; hiv_tests='Indeterminate';end;
if value_coded =1304 then do; hiv_tests='Poor sample';end;
if value_coded =664 then do; hiv_tests='Negative';end;
if value_coded =703 then do; hiv_tests='Positive';end;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
testdate=mdy(mm,dd,yy);
*if pcrdate>mdy(03,01,2008);
drop value_coded dd mm yy obs_datetime concept_id obs_id;
run;


proc sort data=hiv_tests nodupkey dupout=duphiv_tests1 out=hiv_tests_final; by person_id testdate  ; run;


proc sort data=duphiv_tests1 ; by person_id testdate hiv_tests ; run;





PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE duphiv_tests1 duphiv_tests hiv_tests test_results ;
		RUN;
   		QUIT;


%MEND create_test_resultsHIV;

%create_test_resultsHIV;
