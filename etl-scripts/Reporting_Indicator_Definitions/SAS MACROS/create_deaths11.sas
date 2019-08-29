


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




proc sort data=death_combined_all out=deaths_final nodupkey ; by person_id  ; run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE death_combined_all death_combined1 death_combined   death_obs1 death_obs death_persons 
               dead_persons dead_obs death_enc1 encounters; 
		RUN;
   		QUIT;

%MEND create_deaths;
%create_deaths;
