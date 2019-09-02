


Libname tmp 'C:\DATA\CSV DATASETS';

data arvs;
set tmp.arvs;
dd=substr(date_obs_datetime_,9,2);
mm=substr(date_obs_datetime_,6,2);
yy=substr(date_obs_datetime_,1,4);
arv_date=mdy(mm,dd,yy);
format arv_date ddmmyy10.;
drop dd mm yy date_obs_datetime_ ;
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
 if (concept_id=1192 and value_numeric=1) or (concept_id=1255 and value_coded in(981,1256,1257,1258,1259,1406,
1849,1850,1260)) or (concept_id=1088 and value_coded ne .) or (concept_id=966 and value_coded ne .) or 
(concept_id in(1250,1251) and value_coded ne .) or (concept_id=2154 and value_coded not in( 1066,.)) or 
(concept_id=1499 and value_datetime ne .)  then   onarv=1;
if onarv ne . then output onarv;

if concept_id=1255 and value_coded in (981,1258) then arvchangeformulation=1 ;
if arvchangeformulation ne . then  output arvchangeformulation; 


if concept_id=1255 and value_coded=1259 then  arvchangeregimen=1 ; 
if arvchangeregimen ne . then output arvchangeregimen; 
** 1107=none 1260=stop all ** ;
  

if concept_id=1255 and value_coded=1260 then arvstopall=1 ; 
if arvstopall ne . then output arvstopall; 
	 
if (concept_id=1250 and value_coded ne .) or (concept_id=1255 and value_coded=1256)
then arvstart=1 ; 
if arvstart ne . then output arvstart; 
	
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
	ralt(keep=person_id  arv_date raltegravir) emtri(keep=person_id  arv_date emtricitabine)
    other(keep=person_id  arv_date other_arvs) unk(keep=person_id  arv_date unknown); 
set arvs(where=(concept_id not in(2157,1087,1086)));
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
  else if value_coded=6156 then do ;
	RALTEGRAVIR=1 ;
	output ralt ;
  end ;
   else if value_coded=5424 then do ;
	OTHER_ARVS=1 ;
	output other ;
  end ;
   else if value_coded=5811 then do ;
	UNKNOWN=1 ;
	output unk ;
  end ;
run ;

**sorting the specific arv datasets** ;

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
proc sort data=ralt ; by person_id arv_date ;
data ralt1 ; set ralt ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=other ; by person_id arv_date ;
data other1 ; set other ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=unk ; by person_id arv_date ;
data unk1 ; set unk ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

data arvmeds ; merge stav1 lam1 nev1 efav1 nelf1 lop1 rit1 did1 azt1 ten1 abac1 ind1 ralt1 emtri1 other1 unk1;
  by person_id arv_date ;
 if sum(of STAVUDINE LAMIVUDINE--UNKNOWN)>1 then onarv=1;
run ;


/* bring in patients on express care to be on ARVs*/

PROC IMPORT OUT= WORK.encounters1 
            DATAFILE= "C:\DATA\CSV DATASETS\encounters_all.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

/*

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
*/

data xpress(keep=patient_id arv_date xp  rename=patient_id=person_id) ;
format arv_date ddmmyy10.;
set encounters1 ;
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
IF arvstart=1 THEN onarv=1;
run;

data  extra_arvs;
set arv_final;
by person_id arv_date ;
IF arvstart ne 1;
if sum(of STAVUDINE LAMIVUDINE--UNKNOWN)>3 then output extra_arvs;
run;



PROC DATASETS LIBRARY=WORK NOLIST;
	DELETE arvs onarv arvchangeformulation arvchangeregimen arvstopall arvstart
		   stav1 lam1 nev1 efav1 nelf1 lop1 rit1 did1 azt1 ten1 abac1 ind1 emtri1 ralt1 other1 unk1 ink1
		   stav lam nev efav nelf lop rit did azt ten abac ind emtri ralt other unk arvuse arvmeds mednotuse usestmed usenotmed;
		RUN;
   		QUIT;
