


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_Fluconazole();	

PROC IMPORT OUT= WORK.fluco
            DATAFILE= "C:\DATA\CSV DATASETS\Fluconazole.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data onfluconazole(keep=person_id fluco_date onfluconazole) 
fluconazole_plan(keep=person_id fluco_date fluconazole_plan) ;
set fluco;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
fluco_date=mdy(mm,dd,yy);

if (concept_id=1277 and value_coded in(1256,1257,1406)) or (concept_id=1112 and value_coded=747) or
(concept_id=1278 and value_coded=747) or (concept_id=1193 and value_coded=747) then   onfluconazole=1;
if onfluconazole ne . then output onfluconazole;
format fluco_date ddmmyy10.;


if concept_id=1277 and value_coded =1256 then fluconazole_plan='Start     ';
if concept_id=1277 and value_coded in(1257,1406) then fluconazole_plan='Continue     ';
if concept_id=1277 and value_coded =1260 then fluconazole_plan='Stop     ';
if concept_id=1277 and value_coded =1407 then fluconazole_plan='Not refilled     ';
if fluconazole_plan ne '' then output fluconazole_plan;

format fluco_date ddmmyy10.;

run;

proc sort data=onfluconazole; by person_id fluco_date;
proc sort data=fluconazole_plan; by person_id fluco_date;

data Fluco1;
merge onfluconazole fluconazole_plan;
by person_id fluco_date;
run;

proc sort data=fluco1 nodupkey out=fluco_final; by person_id fluco_date fluconazole_plan; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE  fluco  fluco1 onfluconazole fluconazole_plan ;
		RUN;
   		QUIT;


%MEND create_Fluconazole;
%create_Fluconazole;
