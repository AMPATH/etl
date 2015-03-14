


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_deaths();	

PROC IMPORT OUT= WORK.dead_obs 
            DATAFILE= "C:\DATA\CSV DATASETS\death_obs.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


PROC IMPORT OUT= WORK.dead_persons 
            DATAFILE= "C:\DATA\CSV DATASETS\death_persons.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data death_obs1;
format death_rprt ddmmyy10.; format death_dateo ddmmyy10. ;
set dead_obs;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
dd1=substr(value_datetime,9,2);
mm1=substr(value_datetime,6,2);
yy1=substr(value_datetime,1,4);
death_rprt=mdy(mm,dd,yy);
death_dateo=mdy(mm1,dd1,yy1);
keep person_id death_rprt death_dateo encounter_id ;
run;

proc sort data=death_obs1 nodupkey out=death_obs; by person_id death_rprt; run;

proc sort data=death_obs; by person_id encounter_id; run;
proc sort data=Encounters_final out=encounters(keep= encounter_date encounter_id person_id); by person_id encounter_id; run;

data death_enc1;
merge death_obs(in=a) encounters(in=b);
by person_id encounter_id;
if a;
run;



data death_persons;
format death_creatp ddmmyy10.; format death_rptp ddmmyy10.; format death_datep ddmmyy10.;
set dead_persons;
dd=substr(date_created,9,2);
mm=substr(date_created,6,2);
yy=substr(date_created,1,4);
ddp=substr(death_date,9,2);
mmp=substr(death_date,6,2);
yyp=substr(death_date,1,4);
dd1=substr(date_changed,9,2);
mm1=substr(date_changed,6,2);
yy1=substr(date_changed,1,4);
death_creatp=mdy(mm,dd,yy);
death_rptp=mdy(mm1,dd1,yy1);
death_datep=mdy(mmp,ddp,yyp);
keep person_id death_creatp death_datep death_rptp ;
run;

proc sort data=death_persons nodupkey; by person_id death_creatp; run;



data death_combined;
merge death_persons(in=a) death_enc1(in=b);
by person_id;
if b or a;
run;

data death_combined1;
format reported_date ddmmyy10.;
format dod ddmmyy10.;
set death_combined;
by person_id;
if  death_datep ne . then do; dod=death_datep;end;
  else if  death_dateo ne . then do; dod=death_dateo;end;
   else if dod eq . and death_creatp ne . then do;dod=death_creatp;end;
   
if death_rptp ne . then do; reported_date=death_rptp;end;
else if encounter_date ne . then do; reported_date=encounter_date;end;
else if death_rprt ne . then do; reported_date=death_rprt;end;
else if death_creatp ne . then do; reported_date=death_creatp;end;
  run;



data death_combined_all(keep=person_id reported_date dod dead);
set death_combined1 ;
by person_id;
if last.person_id;
dead=1;
run;

/* Data cleaning for death patients*/

libname eva 'C:\DATA\CSV DATASETS';

data Encounter_visits;
set encounters;
by person_id;
retain no_visits ;
if first.person_id then do;
no_visits=0;
end;
no_visits=no_visits+1;
run;


data dead_enc(where=(reported_date<=encounter_date));
merge death_combined_all(in=a) eva.dead_dqa(in=b) Encounter_visits(in=c);
by person_id;
if a and b and c;
*if reported_date<=encounter_date;
run;

data dead_enc1(rename=no_visits=last_no_visits);
set dead_enc;
by person_id;
if last.person_id;
keep person_id no_visits;
run;

data dead_enc2(drop=encounter_id);
merge dead_enc(in=a) dead_enc1(in=b);
by person_id;
if a;
diff=last_no_visits-no_visits;
run;

proc sort data=dead_enc2;by person_id diff;run;


data dead_encdqa(keep=person_id) dead_live;
set dead_enc2;
by person_id diff;
if last.person_id and diff>2 then output dead_encdqa;
if last.person_id and diff<=2 then output dead_live;
run;

proc sort data=dead_encdqa;by person_id;run;

data death_combined_all1;
merge death_combined_all(in=a) dead_encdqa(in=b);
by person_id;
if a and not b;
run;


proc sort data=death_combined_all1 out=deaths_final nodupkey ; by person_id  ; run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE death_combined_all death_combined_all1 death_combined1 death_combined   death_obs1 death_obs death_persons 
               dead_persons dead_obs death_enc1 encounters dead_encdqa dead_enc2 dead_enc1  dead_enc Encounter_visits; 
		RUN;
   		QUIT;

%MEND create_deaths;
%create_deaths;
