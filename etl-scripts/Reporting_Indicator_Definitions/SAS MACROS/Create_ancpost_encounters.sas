
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_ancpost_enc();


PROC IMPORT OUT= WORK.ancpost_enc 
            DATAFILE= "C:\DATA\CSV DATASETS\ancpost_enc.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data ancpost1 (rename=patient_id=person_id);
format encounter_date ddmmyy10.;
set ancpost_enc;
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
encounter_date=mdy(mm,dd,yy);
drop dd mm yy encounter_datetime;
run;

proc sort data=ancpost1; by person_id encounter_date; run;

data ancpost_final;
set ancpost1;  
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE ancpost1 ancpost ancpost_enc;
		RUN;
   		QUIT;



%Mend create_ancpost_enc;
%Create_ancpost_enc;
