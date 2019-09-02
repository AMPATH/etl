


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_encounters();	

PROC IMPORT OUT= WORK.encounters1 
            DATAFILE= "C:\SAS_CODE\REPORT  Monthly LTFU\New\Encounter1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


PROC IMPORT OUT= WORK.encounters2 
            DATAFILE= "C:\SAS_CODE\REPORT  Monthly LTFU\New\Encounter2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

/*
proc sort data=encounters1; by patient_id; run;
proc sort data=encounters2; by patient_id; run;
*/

data encounters;
set encounters1 encounters2;
*by patient_id;
run;


data enc;
format app_date ddmmyy10.;
set encounters;
dd=substr(encounter_datetime,9,2);
mm=substr(encounter_datetime,6,2);
yy=substr(encounter_datetime,1,4);
app_date=mdy(mm,dd,yy);
*mm=scan(encounter_datetime,1, '-');
*dd=scan(encounter_datetime,2, '-');
*yyyy=scan(scan(encounter_datetime,1, ' '),-1, '-');
*app_date=mdy(mm,dd,yyyy);
drop encounter_datetime dd mm yy ;
if encounter_type=1 then ptype='adult  ';
else if encounter_type=2 then ptype='adult';
else if encounter_type=14 then ptype='adult';
else if encounter_type=17 then ptype='adult';
else if encounter_type=19 then ptype='adult';

else if encounter_type=3 then ptype='paeds';
else if encounter_type=4 then ptype='paeds';
else if encounter_type=15 then ptype='paeds';
else if encounter_type=26 then ptype='paeds';

rename patient_id=person_id;
run;

proc sort data=enc out=encounter_final nodupkey; by person_id app_date; run;

/*
proc sort data=enc ; by person_id app_date; run;
data dupenc ; set enc ; by person_id app_date ; if first.app_date=0 or last.app_date=0 ; run ;
proc print data=dupenc(obs=100) ; title 'duplicate encounters' ; run ;
*/

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE encounters1 encounters2 enc;
		RUN;
   		QUIT;


%MEND create_encounters;
%create_encounters;
