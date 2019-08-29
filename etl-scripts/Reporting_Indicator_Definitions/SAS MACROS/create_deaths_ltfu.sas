


PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_deaths_ltfu;


%GLOBAL start_date final_date;
%let start_date=mdy(03,01,2008);
%LET final_date=mdy(05,25,2009);

%include 'C:\DATA\SAS MACROS\create_deaths.sas';


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


proc sort data=encounters1 ; by patient_id; run;
proc sort data=encounters2 ; by patient_id; run;

data encounters;
set encounters1 encounters2;
by patient_id;
rename patient_id=person_id;
run;


data encounters1a;
format encounter_date ddmmyy10.;
set encounters;
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
encounter_date=mdy(mm,dd,yy);
drop dd mm yy encounter_datetime;
if encounter_type in(1,2,14) then ptype='adult ';
else if encounter_type in(3,4,15) then ptype='paed ';
if ptype ne '';
run;

proc sort data=encounters1a; by person_id encounter_date; run;

data encountersc;
set encounters1a;
run;



data lag_encounter;
set encountersc;
lagdate=lag(encounter_date);
lagid=lag(person_id);
if lagid ne person_id then lagdate= .;
if lagdate ne . then lag_period=int((encounter_date-lagdate)/30.25);
format lagdate ddmmyy10.;
*drop return_visit_date;
run; 


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE Encounters Encounters1 Encounters2 Encounters1a;
		RUN;
   		QUIT;




proc sort data=lag_encounter; by person_id ; run;
proc sort data=deaths_final nodupkey; by person_id ; run;

data encountera;
merge lag_encounter(in=a)  deaths_final(in=b);
by person_id;
if a or b;
run;

proc sort data=encountera; by person_id encounter_date; run;

data persons_ltfu1;
set encountera;
if lag_period=>3 and  dead ne 1 then tracking= 'ltfu  ';
else if dead=1  then tracking= 'dead';
else if (lag_period ne . and lag_period<3) and  dead ne 1 then tracking= 'Active  ';
else if (lag_period=. and  dead ne 1)  and (int((&final_date-encounter_date)/30.25) lt 3) then tracking= 'Active  ';
if tracking ne '';
run;

data persons_ltfu2;
set encountera;
if lag_period eq . and encounter_type not in(1,3) and dead ne 1;
drop encounter_type encounter_id;
if int((&final_date-encounter_date)/30.25) lt 3 then tracking= 'Active  ';
else if int((&final_date-encounter_date)/30.25) ge 3 then tracking= 'ltfu  ';
run;

data persons_ltfu;
set persons_ltfu1 persons_ltfu2;
by person_id;
run;


%Mend create_deaths_ltfu;
%create_deaths_ltfu;
