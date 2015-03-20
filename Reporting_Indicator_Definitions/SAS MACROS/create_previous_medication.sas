


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_previous_medication();	

PROC IMPORT OUT= WORK.previous_medication
            DATAFILE= "C:\DATA\CSV DATASETS\previous_medication.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data previous_medication1;
Format app_date ddmmyy10.;
set previous_medication;
if value_coded=1159 then medication='tb drugs               ';
if value_coded=1195 then medication='antibiotics';
if value_coded=1635 then medication='hypertension drugs';
if value_coded=1636 then medication='diabetes drugs';
if value_coded=5841 then medication='herbal drugs';
if value_coded=656 then medication='isoniazid drugs';
if value_coded=780 then medication='oral FP drugs';
if value_coded=916 then medication='co-trimoxazole';
if value_coded=5622 then medication='other non_coded';
if value_coded=1624 then medication='dont know';
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);
drop  obs_datetime dd mm yy value_coded concept_id;
run;



proc sort data=previous_medication1 nodupkey  out=previous_medication_final ; by person_id app_date medication; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE previous_medication previous_medication1 ;
		RUN;
   		QUIT;


%MEND create_previous_medication;
%create_previous_medication;

