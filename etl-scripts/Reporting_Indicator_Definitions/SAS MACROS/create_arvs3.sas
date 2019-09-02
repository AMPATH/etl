


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_arvs();


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

 if (concept_id=1192 and value_numeric=1) or (concept_id=1255 and value_coded in (981,1256,1257,1258,1259)) 
	or (concept_id=1088 and value_coded ne .) or (concept_id=966 and value_coded ne .) or (concept_id=1250 and 
value_coded ne .) or (concept_id=2154 and value_coded not in( 1066,.)) then   onarv=1;


	else if ((concept_id=1192 and value_numeric=0) or (concept_id=1255 and value_coded in (1107,1260))) then  
	onarv=0 ; 
if onarv ne . then
output onarv;

if concept_id=1255 and value_coded in (981,1258) then do; arvchangeformulation=1 ;output arvchangeformulation; end;


if concept_id=1255 and value_coded=1259 then do; arvchangeregimen=1 ; output arvchangeregimen; end;
** 1107=none 1260=stop all ** ;
  

if concept_id=1255 and value_coded=1260 then do; arvstopall=1 ; output arvstopall; end;
	 
if concept_id=1250 and value_coded ne . then do ;arvstart=1 ; output arvstart; end ;
	
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
merge  onarv arvchangeformulation arvchangeregimen arvstopall arvstart;
by person_id arv_date;
run;




proc freq data=arvuse ;
  title 'arvuse' ;
  tables onarv arvstopall arvchangeregimen arvchangeformulation arvstart;
run ;

** code specific arvs ** ;

/* current ARVs*/

data stava(keep=person_id  arv_date stavudine) lama(keep=person_id  arv_date lamivudine) 
	neva(keep=person_id  arv_date nevirapine) efava(keep=person_id  arv_date efavirenz)
	nelfa(keep=person_id  arv_date nelfinavir) lopa(keep=person_id  arv_date lopinavir)
	rita(keep=person_id  arv_date ritonavir) dida(keep=person_id  arv_date didanosine)
	azta(keep=person_id  arv_date azt) tena(keep=person_id  arv_date tenofovir)
	abaca(keep=person_id  arv_date abacavir) inda(keep=person_id  arv_date indinavir)
	emtria(keep=person_id  arv_date emtricitabine) ; 
	set arvs(where=(concept_id in(1088,2154)));
  if value_coded=625 then do ;
	STAVUDINE=1 ;
	output stava ;
  end ;
  else if value_coded=628 then do ;
	LAMIVUDINE=1 ;
	output lama ;
  end ;
  else if value_coded=630 then do ;
	LAMIVUDINE=1 ;
	output lama ;
	AZT=1 ;
	output azta ;
  end ;
  else if value_coded=631 then do ;
	NEVIRAPINE=1 ;
	output neva ;
  end ;
  else if value_coded=633 then do ;
	EFAVIRENZ=1 ;
	output efava ;
  end ;
  else if value_coded=635 then do ;
	NELFINAVIR=1 ;
	output nelfa ;
  end ;
  else if value_coded in (749,811) then do ;
	INDINAVIR=1 ;
	output inda ;
  end ;
  else if value_coded in(791,6180) then do ;
	emtricitabine=1 ;
	output emtria ;
  end ;
  else if value_coded=792 then do ;
	STAVUDINE=1 ;
	output stava ;
	LAMIVUDINE=1 ;
	output lama ;
	NEVIRAPINE=1 ;
	output neva ;
  end ;
  else if value_coded=794 then do ;
	LOPINAVIR=1 ;
	output lopa ;
	RITONAVIR=1 ;
	output rita ;
  end ;
  else if value_coded=796 then do ;
	DIDANOSINE=1 ;
	output dida ;
  end ;
  else if value_coded=797 then do ;
	AZT=1 ;
	output azta ;
  end ;
  else if value_coded in(802,6180) then do ;
	TENOFOVIR=1 ;
	output tena ;
  end ; 
  else if value_coded=814 then do ;
	ABACAVIR=1 ;
	output abaca ;
  end ;
  else if value_coded=1400 then do ;
	LAMIVUDINE=1 ;
	output lama ;
	TENOFOVIR=1 ;
	output tena ;
  end ;
run ;


proc sort data=stava ; by person_id arv_date ;
data stav1 ; set stava ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=lama ; by person_id arv_date ;
data lam1 ; set lama ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=neva ; by person_id arv_date ;
data nev1 ; set neva ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=efava ; by person_id arv_date ;
data efav1 ; set efava ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=nelfa ; by person_id arv_date ;
data nelf1 ; set nelfa ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=lopa ; by person_id arv_date ;
data lop1 ; set lopa ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=rita ; by person_id arv_date ;
data rit1 ; set rita ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=dida ; by person_id arv_date ;
data did1 ; set dida ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=azta ; by person_id arv_date ;
data azt1 ; set azta ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=tena ; by person_id arv_date ;
data ten1 ; set tena ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=abaca ; by person_id arv_date ;
data abac1 ; set abaca ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=inda ; by person_id arv_date ;
data ind1 ; set inda ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=emtria ; by person_id arv_date ;
data emtri1 ; set emtria ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

data arvmeds ; merge stav1 lam1 nev1 efav1 nelf1 lop1 rit1 did1 azt1 ten1 abac1 ind1 emtri1 ;
  by person_id arv_date ;
run ;


/* whether ARVS for treatment or prophylaxis*/

data arv_final;
merge arvuse arvmeds;
by person_id arv_date ;
run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE arvs onarv arvchangeformulation arvchangeregimen arvstopall arvstart
		stav1 lam1 nev1 efav1 nelf1 lop1 rit1 did1 azt1 ten1 abac1 ind1 emtri1
		stav lam nev efav nelf lop rit did azt ten abac ind emtri arvuse arvmeds mednotuse usestmed usenotmed;
		RUN;
   		QUIT;

	

%MEND create_arvs;
%create_arvs;
