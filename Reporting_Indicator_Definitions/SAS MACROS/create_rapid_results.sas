


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pcr();

PROC IMPORT OUT= WORK.pcr_elisa 
            DATAFILE= "C:\DATA\CSV DATASETS\pcr_elisa_rapid.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

	
data rapid;
Format rapid_result $15. rapid_rdate ddmmyy10.;
set pcr_elisa;
where concept_id in(1040);
if value_coded =1138 then do; rapid_result='Indeterminate';end;
if value_coded =1304 then do; rapid_result='Poor sample';end;
if value_coded =664 then do; rapid_result='Negative';end;
if value_coded =703 then do; rapid_result='Positive';end;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
rapid_rdate=mdy(mm,dd,yy);
*if pcrdate>mdy(03,01,2008);
drop value_coded dd mm yy obs_datetime concept_id obs_id;
run;


proc sort data=rapid nodupkey  out=rapid_final; by person_id rapid_rdate rapid_result ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE rapid  pcr_elisa ;
		RUN;
   		QUIT;


%MEND create_pcr;

%create_pcr;
