


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_HIV_serology_results();	


PROC IMPORT OUT= WORK.serology 
            DATAFILE= "C:\DATA\CSV DATASETS\HIV_Serology_results.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data serology1;
Format HIV_result $15. HIVdate ddmmyy10.;
set serology;
if value_coded =1138 then do; HIV_result='Indeterminate';end;
if value_coded =1304 then do; HIV_result='Poor sample';end;
if value_coded =664 then do; HIV_result='Negative';end;
if value_coded =703 then do; HIV_result='Positive';end;
if concept_id=1040 then  test='Rapid test    ';
else if concept_id=1042 then  test='HIV Elisa';
else if concept_id=1047 then test='Western blot';
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
HIVdate=mdy(mm,dd,yy);
drop value_coded dd mm yy obs_datetime concept_id;
run;



proc sort data=serology1  out=HIV_serology_final; by person_id HIVdate  HIV_result test ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE serology serology1;
		RUN;
   		QUIT;


%MEND create_HIV_serology_results;
%create_HIV_serology_results;
