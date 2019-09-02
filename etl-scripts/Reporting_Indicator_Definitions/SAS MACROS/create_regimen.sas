


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_regimen();	


PROC IMPORT OUT= WORK.regimen1
            DATAFILE= "C:\DATA\CSV DATASETS\regimen1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


PROC IMPORT OUT= WORK.regimen2
            DATAFILE= "C:\DATA\CSV DATASETS\regimen2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

proc sort data=regimen1 ; by person_id;
proc sort data=regimen2 ; by person_id;run;



data regimen;
set regimen1 regimen2;
by person_id;
run;


proc sort data=regimen nodupkey; by person_id obs_datetime value_coded;run;

proc transpose data=regimen out=wide3 prefix=value_coded;
   by person_id obs_datetime;
   id value_coded;
   var value_coded;
run;

data reg;
set wide3;
if value_coded628 ne . then do;lamivudine=1;end;
if value_coded633 ne . then do; efavirenz=1; end;
if value_coded797 ne . then do; azt=1;end;
 if value_coded630 ne . then do; azt=1; lamivudine=1; end;
 if value_coded625 ne . then do; stavudine=1;end;
if value_coded631 ne . then do; nevirapine=1;end;
 if value_coded792 ne . then do;  stavudine=1;  lamivudine=1 ; nevirapine=1;end;
 if value_coded794 ne . then do; lopinavir=1; ritonavir=1;end;
 if value_coded796 ne . then do; didanosine=1;end;
 if value_coded814 ne . then do; abacavir=1;end;
 if value_coded802 ne . then do; tenofovir=1;end;
 if value_coded749 ne . then do; indinavir=1;end;
 if value_coded1400 ne . then do;lamivudine=1; tenofovir=1;end;
 if value_coded5424 ne . then do;other_arvs=1; end;
 if value_coded635 ne . then do; nelfinavir=1;end;
  if value_coded5811 ne . then do;Unknown_arvs=1; end;

drop value_coded628 value_coded633 value_coded797 value_coded630 value_coded625
value_coded631 value_coded792 value_coded794 value_coded796 value_coded814 _NAME_
value_coded802 value_coded749 value_coded1400 value_coded5424 value_coded635 
value_coded5811; 

reg_count=sum(lamivudine,efavirenz,azt,stavudine,nevirapine,lopinavir,ritonavir,didanosine,abacavir,
  tenofovir,indinavir,other_arvs,nelfinavir,unknown_arvs);
length arv_combinations $80;
array arv_a[14] lamivudine efavirenz azt stavudine nevirapine lopinavir ritonavir didanosine abacavir
  tenofovir indinavir other_arvs nelfinavir unknown_arvs;
array names_a[14] $15 _temporary_ ('lamivudine' 'efavirenz' 'azt' 'stavudine' 'nevirapine '
'lopinavir' 'ritonavir' 'didanosine' 'abacavir' 'tenofovir' 'indinavir' 'other_arvs' 'nelfinavir' 'unknown_arvs' ) ;
do i=1 to 14;
if arv_a[i] EQ 1 then arv_combinations = catx('-',arv_combinations,
names_a[i]);
drop i;
end;
run;


data regimen_final;
format regimen_date ddmmyy10.;
set reg;
if reg_count not in(5,6,7,10);
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
regimen_date=mdy(mm,dd,yy);
drop obs_datetime dd mm yy;
keep reg_count person_id regimen_date arv_combinations;
run;

proc sort data=regimen_final; by reg_count arv_combinations; run;


%MEND create_regimen;
%Create_regimen;



/* to get when patient started new regimen

proc summary data=Regimen_final nway;
class person_id arv_combinations;
output out=regimen_start min(regimen_date)=start;
run;

libname try 'F:\';

data try.trial;
set regimen_start;
if person_id in(348,380);
drop _TYPE_ _FREQ_;
run;


ods pdf;
proc freq data=regimen_final; table arv_combinations; run;
ods pdf close;


