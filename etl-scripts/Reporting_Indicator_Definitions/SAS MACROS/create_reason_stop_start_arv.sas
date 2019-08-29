


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_stopstartarv();	

PROC IMPORT OUT= WORK.stopstartarv
            DATAFILE= "C:\DATA\CSV DATASETS\Reason_stop_or_start_arv.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data stopstartarv1;
format start_stop_date ddmmyy10.;
set stopstartarv;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
start_stop_date=mdy(mm,dd,yy);

if  concept_id=1251  and value_coded in(1067) then reason_start='UNKNOWN                 ' ;
if  concept_id=1251  and value_coded in(1148) then reason_start='Tpmtct';
if  concept_id=1251  and value_coded in(1185) then reason_start='Treatment';
if  concept_id=1251  and value_coded in(1206) then reason_start='WhoStage3';
if  concept_id=1251  and value_coded in(1207) then reason_start='WhoStage4';
if  concept_id=1251  and value_coded in(1776) then reason_start='PMTCT';
if  concept_id=1251  and value_coded in(2006) then reason_start='Clinical Disease';
if  concept_id=1251  and value_coded in(2044) then reason_start='WhoStage3 CD4 less 350';
if  concept_id=1251  and value_coded in(2090) then reason_start='PEP';


if  concept_id=1252  and value_coded=102 then reason_stop=' 	 TOXICITY, DRUG';
if  concept_id=1252  and  value_coded=1434 then reason_stop='POOR ADHERENCE, NOS';
if  concept_id=1252  and  value_coded=843 then reason_stop=' 	REGIMEN FAILURE';
if  concept_id=1252  and  value_coded=983 then reason_stop='WEIGHT CHANGE ';
if  concept_id=1252  and  value_coded=5622 then reason_stop=' 	OTHER NON-CODED';
if  concept_id=1252  and  value_coded=1253 then reason_stop=' COMPLETED Tpmtct';
drop concept_id dd mm yy obs_datetime ;
run;




proc sort data=stopstartarv1 out=stopstartarv_final nodupkey; by person_id start_stop_date ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE stopstartarv stopstartarv1 ;
		RUN;
   		QUIT;


%MEND create_stopstartarv;
%create_stopstartarv;
