
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_encounters();


PROC IMPORT OUT= WORK.encounters1 
            DATAFILE= "C:\DATA\CSV DATASETS\encounters1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


PROC IMPORT OUT= WORK.encounters2 
            DATAFILE= "C:\DATA\CSV DATASETS\encounters2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data encounters;
set encounters1 encounters2 ;
run;


data encounters1a (rename=patient_id=person_id);
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

proc sort data=encounters1a; by person_id encounter_date; run;

data encounters_final;
set encounters1a;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE encounters1  encounters encounters1a;
		RUN;
   		QUIT;



%Mend create_encounters;
%Create_encounters;
