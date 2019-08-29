


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_elisa();	


PROC IMPORT OUT= WORK.pcr_elisa 
            DATAFILE= "C:\DATA\CSV DATASETS\pcr_elisa_rapid.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data elisa;
Format elisa_result $15. elisadate ddmmyy10.;
set pcr_elisa;
where concept_id in(1042);
if value_coded =1138 then do; elisa_result='Indeterminate';end;
if value_coded =1304 then do; elisa_result='Poor sample';end;
if value_coded =664 then do; elisa_result='Negative';end;
if value_coded =703 then do; elisa_result='Positive';end;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
elisadate=mdy(mm,dd,yy);
drop value_coded dd mm yy obs_datetime concept_id;
run;



proc sort data=elisa nodupkey  out=elisa_final; by person_id elisadate elisa_result ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE elisa pcr_elisa;
		RUN;
   		QUIT;


%MEND create_elisa;
%create_elisa;
