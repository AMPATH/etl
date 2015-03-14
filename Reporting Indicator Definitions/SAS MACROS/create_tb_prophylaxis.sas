


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_tb_prophylaxis();	

PROC IMPORT OUT= WORK.TB_prophylaxis 
            DATAFILE= "C:\DATA\CSV DATASETS\TB_prophylaxis_plan.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data TB_prophylaxis1;
Format tbprophy_date ddmmyy10.;
set TB_prophylaxis;
if value_coded=1256 then do; TB_prophylaxis_plan='start drugs           ';end;
else if value_coded=1257 then do;TB_prophylaxis_plan='continue regimen';end;
else if value_coded=1260 then do; TB_prophylaxis_plan='stop all';end;
else if value_coded=1406 then do; TB_prophylaxis_plan='refilled';end;
else if value_coded=1407 then do; TB_prophylaxis_plan='not refilled';end;
else if value_coded=981 then do; TB_prophylaxis_plan='dosing change';end;
else if value_coded=656 then do; TB_prophylaxis_drug='INH          ';end;
else if value_coded=745 then do; TB_prophylaxis_drug='ethambutol';end;
else if value_coded=767 then do; TB_prophylaxis_drug='rifampicin';end;

dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
tbprophy_date=mdy(mm,dd,yy);
keep  person_id tbprophy_date TB_prophylaxis_plan TB_prophylaxis_drug;
run;


proc sort data=TB_prophylaxis1 nodupkey  out=TB_prophylaxis_final; by person_id tbprophy_date 
TB_prophylaxis_plan TB_prophylaxis_drug ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE TB_prophylaxis1 TB_prophylaxis;
		RUN;
   		QUIT;


%MEND create_tb_prophylaxis;
%create_tb_prophylaxis;

