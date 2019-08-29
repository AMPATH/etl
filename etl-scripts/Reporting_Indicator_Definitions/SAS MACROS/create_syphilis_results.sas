


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_syphilis_results();

PROC IMPORT OUT=syphilis_results
            DATAFILE= "C:\DATA\CSV DATASETS\syphilis_test_results.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data syphilis_results1;
Format syphilisresult_date ddmmyy10.;
set syphilis_results;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
syphilisresult_date=mdy(mm,dd,yy);
if concept_id in(1032) and value_coded=1138 then do; syphilis_result='Indeterminate  '; end;
if concept_id in(1032) and value_coded=1304 then do; syphilis_result='Poor Sample'; end;
if concept_id in(1032) and value_coded=664 then do; syphilis_result='Negative'; end;
if concept_id in(1032) and value_coded=703 then do; syphilis_result='Positive'; end;
if concept_id in(299) and value_coded=1138 then do; syphilis_result='Indeterminate  '; end;
if concept_id in(299) and value_coded=1304 then do; syphilis_result='Poor Sample'; end;
if concept_id in(299) and value_coded=664 then do; syphilis_result='Negative'; end;
if concept_id in(299) and value_coded=703 then do; syphilis_result='Positive'; end;
if concept_id in(299) and value_coded=1228 then do; syphilis_result='Reactive'; end;
if concept_id in(299) and value_coded=1229 then do; syphilis_result='Non reactive'; end;
if syphilis_result ne '';
drop value_coded dd mm yy obs_datetime concept_id ;
run;


proc sort data=syphilis_results1 nodupkey dupout=dupsyphilis_results1 out=syphilis_results_final; 
by person_id syphilisresult_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE syphilis_results syphilis_results1 ;
		RUN;
   		QUIT;


%MEND create_syphilis_results;

%create_syphilis_results;
