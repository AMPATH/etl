
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_encounters_tb();


PROC IMPORT OUT= WORK.encounters 
            DATAFILE= "C:\DATA\CSV DATASETS\tb_encounters.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;




data encounters1 (rename=patient_id=person_id);
format encounter_date ddmmyy10.;
set encounters;
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
encounter_date=mdy(mm,dd,yy);
drop dd mm yy encounter_datetime;
if encounter_type in(1,2,14) then ptype='adult ';
else if encounter_type in(3,4,15) then ptype='paeds ';
run;

proc sort data=encounters1; by person_id encounter_date; run;

data encounterstb_final;
set encounters1;  
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE encounters1   encounters;
		RUN;
   		QUIT;



%Mend create_encounters_tb;
%Create_encounters_tb;
