


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_tb();	


PROC IMPORT OUT= WORK.TBa 
            DATAFILE= "C:\DATA\CSV DATASETS\tb1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.TBb 
            DATAFILE= "C:\DATA\CSV DATASETS\tb2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN; 

proc sort data=tba; by person_id; run;
proc sort data=tbb; by person_id; run;
data tb;
set tba tbb;
run;


data tb1;
Format tb 1. tb_date ddmmyy10.;
set tb;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
tb_date=mdy(mm,dd,yy);
if concept_id in(1268) and value_coded=1256 then do; tb_plan='start drugs           ' ; tb=1;end;
if concept_id in(1268) and value_coded=1107 then do;tb_plan='none' ; tb=0;end;
if concept_id in(1268) and value_coded=1259 then do; tb_plan='change regimen' ; tb=1;end;
if concept_id in(1268) and value_coded=1257 then do; tb_plan='continue regimen' ; tb=1;end;
if concept_id in(1268) and value_coded=1260 then do; tb_plan='stop all' ; tb=0;end;
if concept_id in(1268) and value_coded=1850 then do; tb_plan='drug restart' ; tb=1;end;
if concept_id in(1268) and value_coded=2160 then do; tb_plan='TB defaulter' ; tb=1;end;
if concept_id in(1268) and value_coded=2161 then do; tb_plan='MDRTR' ; tb=1;end;
if concept_id in(1268) and value_coded=981 then do; tb_plan='dosing change' ; tb=1;end;
if concept_id in(1269) and value_coded=1267 then do; tb_plan='completed treatment' ; tb=0;end;
if concept_id in(1266) and value_coded=58 then do; TBpropystop_reason='TB' ; tb=1;end;
if concept_id in(1270) then do;  tb=1;end;
if concept_id in(1111) and value_coded=1107 then do;  tb=0;end;
if concept_id in(1111) and value_coded not in(1107) then do;  tb=1;end;
if concept_id in(2022,1506)  then do;  tb=1;end;
if value_coded=58 then do; tb=1;end;
keep  tb person_id tb_date tb_plan TBpropystop_reason value_coded concept_id;
run;



proc sort data=tb1 nodupkey  out=tb_final; by person_id tb_date tb_plan tb  ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE tb tb1 tba tbb;
		RUN;
   		QUIT;


%MEND create_tb;
%create_tb;

