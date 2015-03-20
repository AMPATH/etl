


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_arvs();


Libname tmp 'C:\DATA\CSV DATASETS';

data arvs(rename=value_coded1=value_coded);
set tmp.arvs;
dd=substr(date_obs_datetime_,9,2);
mm=substr(date_obs_datetime_,6,2);
yy=substr(date_obs_datetime_,1,4);
arv_date=mdy(mm,dd,yy);
value_coded1=value_coded*1;
format arv_date ddmmyy10.;
drop dd mm yy date_obs_datetime_ value_coded ;
run;



data onarv(keep=person_id arv_date onarv) 
arvchangeformulation(keep=person_id arv_date arvchangeformulation) 
arvchangeregimen(keep=person_id arv_date arvchangeregimen) 
arvstopall(keep=person_id arv_date arvstopall) 
arvstart(keep=person_id arv_date arvstart)   ;
		set arvs;
  ** 1088 CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT ;
  ** 1192 ANTIRETROVIRAL USE ;
  ** 1250 ANTIRETROVIRALS STARTED ;
  ** 1255 ANTIRETROVIRAL PLAN ;
  ** 981=dosing change 1256=START DRUGS 1257=CONTINUE REGIMEN 1258=CHANGE FORMULATION 1259=CHANGE REGIMEN ** ;

 if (concept_id=1192 and value_numeric=1) or (concept_id=1255 and value_coded in (981,1256,1257,1258,1259,1406,1849,1850)) 
	or (concept_id=1088 and value_coded ne .) or (concept_id=966 and value_coded ne .) or (concept_id=1250 and 
value_coded ne .) or (concept_id=2154 and value_coded not in( 1066,.)) then   onarv=1;
if onarv ne . then output onarv;

if concept_id=1255 and value_coded in (981,1258) then do; arvchangeformulation=1 ;output arvchangeformulation; end;


if concept_id=1255 and value_coded=1259 then do; arvchangeregimen=1 ; output arvchangeregimen; end;
** 1107=none 1260=stop all ** ;
  

if concept_id=1255 and value_coded=1260 then do; arvstopall=1 ; output arvstopall; end;
	 
if (concept_id=1250 and value_coded ne .) or (concept_id=1255 and value_coded=1256)
then arvstart=1 ; output arvstart; 
	
  label arvchangeregimen='ARV change regimen'
  	arvchangeformulation='ARV change formulation'
	arvstopall='ARV stop all' ;
run ;

proc sort data=onarv nodupkey; by person_id arv_date onarv; run;
proc sort data=arvchangeformulation nodupkey; by person_id arv_date arvchangeformulation; run;
proc sort data=arvchangeregimen nodupkey; by person_id arv_date arvchangeregimen; run;
proc sort data=arvstopall nodupkey; by person_id arv_date arvstopall; run;
proc sort data=arvstart nodupkey; by person_id arv_date arvstart; run;



data arvuse;
merge  onarv(in=a) arvchangeformulation arvchangeregimen arvstopall arvstart;
by person_id arv_date;
run;




proc freq data=arvuse ;
  title 'arvuse' ;
  tables onarv arvstopall arvchangeregimen arvchangeformulation arvstart;
run ;

** code specific arvs ** ;



data stav(keep=person_id  arv_date stavudine) lam(keep=person_id  arv_date lamivudine) 
	nev(keep=person_id  arv_date nevirapine) efav(keep=person_id  arv_date efavirenz)
	nelf(keep=person_id  arv_date nelfinavir) lop(keep=person_id  arv_date lopinavir)
	rit(keep=person_id  arv_date ritonavir) did(keep=person_id  arv_date didanosine)
	azt(keep=person_id  arv_date azt) ten(keep=person_id  arv_date tenofovir)
	abac(keep=person_id  arv_date abacavir) ind(keep=person_id  arv_date indinavir)
	emtri(keep=person_id  arv_date emtricitabine) ; 
	set arvs(where=(concept_id not in(2157,1087,1086,1088)));
  if value_coded=625 then do ;
	STAVUDINE=1 ;
	output stav ;
  end ;
  else if value_coded=628 then do ;
	LAMIVUDINE=1 ;
	output lam ;
  end ;
  else if value_coded=630 then do ;
	LAMIVUDINE=1 ;
	output lam ;
	AZT=1 ;
	output azt ;
  end ;
  else if value_coded=631 then do ;
	NEVIRAPINE=1 ;
	output nev ;
  end ;
  else if value_coded=633 then do ;
	EFAVIRENZ=1 ;
	output efav ;
  end ;
  else if value_coded=635 then do ;
	NELFINAVIR=1 ;
	output nelf ;
  end ;
  else if value_coded in (749,811) then do ;
	INDINAVIR=1 ;
	output ind ;
  end ;
  else if value_coded in(791,6180) then do ;
	emtricitabine=1 ;
	output emtri ;
  end ;
  else if value_coded=792 then do ;
	STAVUDINE=1 ;
	output stav ;
	LAMIVUDINE=1 ;
	output lam ;
	NEVIRAPINE=1 ;
	output nev ;
  end ;
  else if value_coded=794 then do ;
	LOPINAVIR=1 ;
	output lop ;
	RITONAVIR=1 ;
	output rit ;
  end ;
  else if value_coded=796 then do ;
	DIDANOSINE=1 ;
	output did ;
  end ;
  else if value_coded=797 then do ;
	AZT=1 ;
	output azt ;
  end ;
  else if value_coded in(802,6180) then do ;
	TENOFOVIR=1 ;
	output ten ;
  end ; 
  else if value_coded=814 then do ;
	ABACAVIR=1 ;
	output abac ;
  end ;
  else if value_coded=1400 then do ;
	LAMIVUDINE=1 ;
	output lam ;
	TENOFOVIR=1 ;
	output ten ;
  end ;
run ;


proc sort data=stav ; by person_id arv_date ;
data stav1 ; set stav ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=lam ; by person_id arv_date ;
data lam1 ; set lam ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=nev ; by person_id arv_date ;
data nev1 ; set nev ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=efav ; by person_id arv_date ;
data efav1 ; set efav ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=nelf ; by person_id arv_date ;
data nelf1 ; set nelf ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=lop ; by person_id arv_date ;
data lop1 ; set lop ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=rit ; by person_id arv_date ;
data rit1 ; set rit ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=did ; by person_id arv_date ;
data did1 ; set did ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=azt ; by person_id arv_date ;
data azt1 ; set azt ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=ten ; by person_id arv_date ;
data ten1 ; set ten ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=abac ; by person_id arv_date ;
data abac1 ; set abac ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=ind ; by person_id arv_date ;
data ind1 ; set ind ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=emtri ; by person_id arv_date ;
data emtri1 ; set emtri ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

data arvmeds ; merge stav1 lam1 nev1 efav1 nelf1 lop1 rit1 did1 azt1 ten1 abac1 ind1 emtri1 ;
  by person_id arv_date ;
 if sum(of STAVUDINE LAMIVUDINE--emtricitabine)>1 then onarv=1;
run ;
/* bring in patients on express care to be on ARVs*/

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


data xpress(keep=patient_id arv_date xp  rename=patient_id=person_id) ;
format arv_date ddmmyy10.;
set encounters1 encounters2;
if encounter_type in(17,19);
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
arv_date=mdy(mm,dd,yy);
drop dd mm yy encounter_datetime;
xp=1;
run;

proc sort data=xpress nodupkey; by person_id arv_date; run;


/* whether ARVS for treatment or prophylaxis*/

data arv_final;
length person_id 8.;
merge arvuse xpress arvmeds;
by person_id arv_date ;
if xp=1 then onarv=1;
run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE arvs onarv arvchangeformulation arvchangeregimen arvstopall arvstart
		stav1 lam1 nev1 efav1 nelf1 lop1 rit1 did1 azt1 ten1 abac1 ind1 emtri1
		stav lam nev efav nelf lop rit did azt ten abac ind emtri arvuse arvmeds mednotuse usestmed usenotmed;
		RUN;
   		QUIT;

	

%MEND create_arvs;
%create_arvs;
