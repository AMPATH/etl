

PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_arv_stop_reasons();


PROC IMPORT OUT= ARV_stop_reasons 
            DATAFILE= "C:\DATA\CSV DATASETS\ARV_stop_reasons.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data ARV_stop_reasons1(drop=concept_id value_coded) ;
format app_date ddmmyy10.;
set ARV_stop_reasons(where=(concept_id=1252));
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);
drop obs_datetime dd mm yy;
if value_coded in(102) then ARV_stop_reasons='drug toxicity      ';
else if value_coded  in(1253) then ARV_stop_reasons='completed PMTCT';
else if value_coded  in(1434) then ARV_stop_reasons='poor adherence';
else if value_coded  in(5622) then ARV_stop_reasons='other non coded';
else if value_coded  in(843) then ARV_stop_reasons='regimen failure';
else if value_coded  in(983) then ARV_stop_reasons='weight change';
label ARV_stop_reasons='Reason for stopping/changing a ARV drug';
run;



data ARV_stop(drop=concept_id value_coded);
format app_date ddmmyy10.;
set ARV_stop_reasons(where=(concept_id=1255));
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);
drop obs_datetime dd mm yy;
if value_coded=1260 then ARV_stop=1;
run;



proc sort data=ARV_stop_reasons1 nodupkey; by person_id app_date ARV_stop_reasons; run;
proc sort data=ARV_stop nodupkey; by person_id app_date ; run;


data ARV_stop_reasons2;
merge ARV_stop ARV_stop_reasons1;
by person_id  app_date;
run;

proc sort data=ARV_stop_reasons2 dupout=dup_arv_reasons out=ARV_stopreason_final nodupkey; by person_id app_date ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE ARV_stop_reasons1 ARV_stop_reasons2 ARV_stop_reasons ARV_stop;
		RUN;
   		QUIT;



%Mend create_arv_stop_reasons;
%create_arv_stop_reasons;
