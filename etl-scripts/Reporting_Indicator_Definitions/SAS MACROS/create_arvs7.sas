


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_arvs();


Libname tmp 'C:\DATA\CSV DATASETS';

data arvs;
set tmp.arvs;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
arv_date=mdy(mm,dd,yy);
format arv_date ddmmyy10.;
drop dd mm yy obs_datetime ;
run;



data arvsall(keep=person_id arv_date arvsall concept_id value_coded)
onarv(keep=person_id arv_date onarv)
previousonarv (keep=person_id arv_date previousonarv)
arvchangeformulation(keep=person_id arv_date arvchangeformulation) 
arvchangeregimen(keep=person_id arv_date arvchangeregimen) 
arvstopall(keep=person_id arv_date arvstopall) 
arvstart(keep=person_id arv_date arvstart)
arvrestart(keep=person_id arv_date arvrestart)
arvsubstitution(keep=person_id arv_date arvsubstitution)
arvdosechange(keep=person_id arv_date arvdosechange)
arvreasonstop(keep=person_id arv_date arvreasonstop)
arveverchanged(keep=person_id arv_date arveverchanged) ;
		set arvs;
  ** 1088 CURRENT ANTIRETROVIRAL DRUGS USED FOR TREATMENT ;
  ** 1192 ANTIRETROVIRAL USE ;
  ** 1250 ANTIRETROVIRALS STARTED ;
  ** 1255 ANTIRETROVIRAL PLAN ;
  ** 1999 ARVS CHANGE**;
  ** 981=dosing change 1256=START DRUGS 1257=CONTINUE REGIMEN 1258=CHANGE FORMULATION 1259=CHANGE REGIMEN ** ;

  if arv_date ne . then arvsall=1;
  if arvsall=1 then output arvsall;

 if (concept_id=1192 and value_coded ='1065') or (concept_id=1255 and value_coded in('981','1256','1257','1258','1259','1406','1849','1850','1260'))
or (concept_id=1088 and value_coded ne '1107') or (concept_id=966 and value_coded ne '') or (concept_id=1252 and value_coded ne '') 
or (concept_id in (1250,1251) and value_coded ne '') or (concept_id=2154 and value_coded not in ( '1066','')) or (concept_id=1999 and value_coded='1065') 
 or (concept_id=1895 and value_coded ne '') or (concept_id=1992 and value_coded in ('1065','1776')) then onarv=1;
if onarv ne . then output onarv;

if (concept_id in (1086, 1087,1187,1176,1147,1387,2157) and value_coded not in ('1067','1624','1066')) then previousonarv=1;
if  previousonarv ne . then output previousonarv;

if concept_id=1255 and value_coded in ('1258') then arvchangeformulation=1 ;
if arvchangeformulation ne . then  output arvchangeformulation; 

if concept_id=1255 and value_coded in ('981') then arvdosechange=1 ;
if arvdosechange ne . then  output arvdosechange; 

if concept_id=1255 and value_coded='1259' then  arvchangeregimen=1 ; 
if arvchangeregimen ne . then output arvchangeregimen; 
** 1107=none 1260=stop all ** ;
  
if concept_id=1255 and value_coded='1260' then arvstopall=1 ; 
if arvstopall ne . then output arvstopall; 

if concept_id=1252 and value_coded eq '843' then arvreasonstop='Regimen Failure  ';
if concept_id=1252 and value_coded eq '983' then arvreasonstop='Weight Change';
if concept_id=1252 and value_coded eq '102' then arvreasonstop='Toxicity Drug';
if concept_id=1252 and value_coded eq '5622' then arvreasonstop='Other Non Coded';
if concept_id=1252 and value_coded eq '1253' then arvreasonstop='Completed TpMTCT';
if concept_id=1252 and value_coded eq '1434' then arvreasonstop='Poor Adherence';

if arvreasonstop ne '.' then output arvreasonstop; 

if concept_id=1255 and value_coded='1850' then arvrestart=1 ; 
if arvrestart ne . then output arvrestart; 

if concept_id=1255 and value_coded='1849' then arvsubstitution=1 ; 
if arvsubstitution ne . then output arvsubstitution; 
 
if (concept_id=1250 and value_coded ne '.') or (concept_id=1255 and value_coded='1256')or (concept_id=1251 and value_coded ne '.')
or (concept_id=2155 and value_coded in ('1776','1185')) then arvstart=1 ; 
if arvstart ne . then output arvstart; 
	
if concept_id=1999 and value_coded='1065' then  arveverchanged=1 ; 
if arveverchanged ne . then output arveverchanged; 


  label arvchangeregimen='ARV change regimen'
  	arvchangeformulation='ARV change formulation'
	arvstopall='ARV stop all' ;
run ;


proc sort data=arvsall nodupkey; by person_id arv_date arvsall; run;
proc sort data=onarv nodupkey; by person_id arv_date onarv; run;
proc sort data=previousonarv nodupkey; by person_id arv_date previousonarv; run;
proc sort data=arvchangeformulation nodupkey; by person_id arv_date arvchangeformulation; run;
proc sort data=arvchangeregimen nodupkey; by person_id arv_date arvchangeregimen; run;
proc sort data=arvstopall nodupkey; by person_id arv_date arvstopall; run;
proc sort data=arvreasonstop nodupkey; by person_id arv_date arvreasonstop; run;
proc sort data=arvstart nodupkey; by person_id arv_date arvstart; run;
proc sort data=arvdosechange nodupkey; by person_id arv_date arvdosechange; run;
proc sort data=arvrestart nodupkey; by person_id arv_date arvrestart; run;
proc sort data=arvsubstitution nodupkey; by person_id arv_date arvsubstitution; run;
proc sort data=arveverchanged nodupkey; by person_id arv_date arveverchanged; run;


data arvuse;
merge arvsall onarv(in=a) previousonarv arvchangeformulation arvchangeregimen arvstopall arvreasonstop arvstart  arvdosechange arvrestart arvsubstitution arveverchanged ;
by person_id arv_date;
run;



/*
proc freq data=arvuse ;
  title 'arvuse' ;
  tables onarv arvstopall arvchangeregimen arvchangeformulation arvstart;
run ;*/

** code specific arvs ** ;




data stav(keep=person_id  arv_date stavudine)	lam(keep=person_id  arv_date lamivudine) 
	nev(keep=person_id  arv_date nevirapine)		efav(keep=person_id  arv_date efavirenz)
	nelf(keep=person_id  arv_date nelfinavir)		lop(keep=person_id  arv_date lopinavir)
	rit(keep=person_id  arv_date ritonavir)			did(keep=person_id  arv_date didanosine)
	azt(keep=person_id  arv_date azt)				ten(keep=person_id  arv_date tenofovir)
	abac(keep=person_id  arv_date abacavir)			ind(keep=person_id  arv_date indinavir)
	emtri(keep=person_id  arv_date emtricitabine)	ralt(keep=person_id  arv_date RALTEGRAVIR)
	Dar(keep=person_id  arv_date DARUNAVIR)			etra(keep=person_id  arv_date ETRAVIRINE)
	Ataza(keep=person_id  arv_date ATAZANAVIR)		unk(keep=person_id  arv_date unk) 
	other(keep=person_id  arv_date other);

	set arvs(where=(concept_id not in(2157,1087,1086,1176) AND concept_id IN (1255,1250,1895)));
*set arvs(where=(concept_id not in(2157,1087,1086,1176) AND concept_id IN (1255,1250,1895)));

  if value_coded in('625','792','6965') then do ;
	STAVUDINE=1 ;
	output stav ;
  end ;

  if value_coded in('628','792','630','1400','6467','6679','6964','6965') then do ;
	LAMIVUDINE=1 ;
	output lam ;
  end ;

if value_coded in('797','6467','630') then do ;
	AZT=1 ;
	output azt ;
  end ;

if value_coded in('631','6467','792') then do ;
	NEVIRAPINE=1 ;
	output nev ;
  end ;

if value_coded in ('633','6964') then do ;
	EFAVIRENZ=1 ;
	output efav ;
  end ;

if value_coded='635' then do ;
	NELFINAVIR=1 ;
	output nelf ;
  end ;

if value_coded in ('749') then do ;
	INDINAVIR=1 ;
	output ind ;
  end ;

 if value_coded in('791','6180') then do ;
	emtricitabine=1 ;
	output emtri ;
  end ;


 if value_coded='794' then do ;
	LOPINAVIR=1 ;
	output lop ;
   end ;

   
 if value_coded in('794','6160','795') then do ;
	RITONAVIR=1 ;
	output rit ;
  end ;

if value_coded='796' then do ;
	DIDANOSINE=1 ;
	output did ;
  end ;

if value_coded in('802','6180','1400','6964') then do ;
	TENOFOVIR=1 ;
	output ten ;
  end ; 

if value_coded in ('814','6679') then do ;
	ABACAVIR=1 ;
	output abac ;
  end ;

if value_coded='6156' then do ;
	RALTEGRAVIR=1 ;
	output ralt ;
  end ;

if value_coded='6157' then do ;
	DARUNAVIR=1 ;
	output Dar ;
  end ;

if value_coded='6158' then do ;
	ETRAVIRINE=1 ;
	output etra ;
  end ;

if value_coded='6159' then do ;
	ATAZANAVIR=1 ;
	output ataza ;
  end ;

if value_coded='5811' then do ;
	UNK=1 ;
	output unk ;
  end ;

 if value_coded='5424' then do ;
	OTHER=1 ;
	output other ;
  end ;
run ;


data Cstav(keep=person_id  arv_date Cstavudine)	Clam(keep=person_id  arv_date Clamivudine) 
	Cnev(keep=person_id  arv_date Cnevirapine)		Cefav(keep=person_id  arv_date Cefavirenz)
	Cnelf(keep=person_id  arv_date Cnelfinavir)		Clop(keep=person_id  arv_date Clopinavir)
	Crit(keep=person_id  arv_date Critonavir)			Cdid(keep=person_id  arv_date Cdidanosine)
	Cazt(keep=person_id  arv_date Cazt)				Cten(keep=person_id  arv_date Ctenofovir)
	Cabac(keep=person_id  arv_date Cabacavir)			Cind(keep=person_id  arv_date Cindinavir)
	Cemtri(keep=person_id  arv_date Cemtricitabine)	Cralt(keep=person_id  arv_date CRALTEGRAVIR)
	CDar(keep=person_id  arv_date CDARUNAVIR)			Cetra(keep=person_id  arv_date CETRAVIRINE)
	CAtaza(keep=person_id  arv_date CATAZANAVIR)		Cunk(keep=person_id  arv_date Cunk) 
	Cother(keep=person_id  arv_date Cother);
	set arvs(where=(concept_id not in(2157,1087,1086,1176) and concept_id in(1192,1088,966,2154,1895)));

  if value_coded in('625','792','6965') then do ;
	CSTAVUDINE=1 ;
	output Cstav ;
  end ;

  if value_coded in('628','792','630','1400','6467','6679','6964','6965') then do ;
	CLAMIVUDINE=1 ;
	output Clam ;
  end ;

if value_coded in('797','6467','630') then do ;
	CAZT=1 ;
	output Cazt ;
  end ;

if value_coded in('631','6467','792') then do ;
	CNEVIRAPINE=1 ;
	output Cnev ;
  end ;

if value_coded in ('633','6964') then do ;
	CEFAVIRENZ=1 ;
	output Cefav ;
  end ;

if value_coded='635' then do ;
	CNELFINAVIR=1 ;
	output Cnelf ;
  end ;

if value_coded in ('749') then do ;
	CINDINAVIR=1 ;
	output Cind ;
  end ;

 if value_coded in('791','6180') then do ;
	Cemtricitabine=1 ;
	output Cemtri ;
  end ;


 if value_coded='794' then do ;
	CLOPINAVIR=1 ;
	output Clop ;
   end ;

   
 if value_coded in('794','6160','795') then do ;
	CRITONAVIR=1 ;
	output Crit ;
  end ;

if value_coded='796' then do ;
	CDIDANOSINE=1 ;
	output Cdid ;
  end ;

if value_coded in('802','6180','1400','6964') then do ;
	CTENOFOVIR=1 ;
	output Cten ;
  end ; 

if value_coded in ('814','6679') then do ;
	CABACAVIR=1 ;
	output Cabac ;
  end ;

if value_coded='6156' then do ;
	CRALTEGRAVIR=1 ;
	output Cralt ;
  end ;

if value_coded='6157' then do ;
	CDARUNAVIR=1 ;
	output CDar ;
  end ;

if value_coded='6158' then do ;
	CETRAVIRINE=1 ;
	output Cetra ;
  end ;

if value_coded='6159' then do ;
	CATAZANAVIR=1 ;
	output Cataza ;
  end ;

if value_coded='5811' then do ;
	CUNK=1 ;
	output Cunk ;
  end ;

 if value_coded='5424' then do ;
	COTHER=1 ;
	output Cother ;
  end ;
run ;




/*........................*/


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

proc sort data=Dar ; by person_id arv_date ;
data dar1 ; set dar ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

proc sort data=etra ; by person_id arv_date ;
data etra1 ; set etra ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

proc sort data=ataza ; by person_id arv_date ;
data ataza1 ; set ataza ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

proc sort data=UNK ; by person_id arv_date ;
data UNK1 ; set UNK ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=OTHER ; by person_id arv_date ;
data OTHER1 ; set OTHER ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

data arvmeds ; merge stav1 lam1 nev1 efav1 nelf1 lop1 rit1 did1 azt1 ten1 abac1 ind1 emtri1 ralt1 dar1 etra1 ataza1 UNK1 OTHER1;
  by person_id arv_date ;
 *if sum(of STAVUDINE LAMIVUDINE--OTHER)>1 then onarv=1;

run ;

/*...................................*/

proc sort data=Cstav ; by person_id arv_date ;
data Cstav1 ; set Cstav ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Clam ; by person_id arv_date ;
data Clam1 ; set Clam ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cnev ; by person_id arv_date ;
data Cnev1 ; set Cnev ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cefav ; by person_id arv_date ;
data Cefav1 ; set Cefav ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cnelf ; by person_id arv_date ;
data Cnelf1 ; set Cnelf ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Clop ; by person_id arv_date ;
data Clop1 ; set Clop ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Crit ; by person_id arv_date ;
data Crit1 ; set Crit ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cdid ; by person_id arv_date ;
data Cdid1 ; set Cdid ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cazt ; by person_id arv_date ;
data Cazt1 ; set Cazt ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cten ; by person_id arv_date ;
data Cten1 ; set Cten ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cabac ; by person_id arv_date ;
data Cabac1 ; set Cabac ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cind ; by person_id arv_date ;
data Cind1 ; set Cind ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=Cemtri ; by person_id arv_date ;
data Cemtri1 ; set Cemtri ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

proc sort data=Cralt ; by person_id arv_date ;
data Cralt1 ; set Cralt ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

proc sort data=CDar ; by person_id arv_date ;
data Cdar1 ; set Cdar ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

proc sort data=Cetra ; by person_id arv_date ;
data Cetra1 ; set Cetra ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

proc sort data=Cataza ; by person_id arv_date ;
data Cataza1 ; set Cataza ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

proc sort data=CUNK ; by person_id arv_date ;
data CUNK1 ; set CUNK ;
  by person_id arv_date ;
  if last.arv_date ;
run ;
proc sort data=COTHER ; by person_id arv_date ;
data COTHER1 ; set COTHER ;
  by person_id arv_date ;
  if last.arv_date ;
run ;

data Carvmeds ; merge Cstav1 Clam1 Cnev1 Cefav1 Cnelf1 Clop1 Crit1 Cdid1 Cazt1 Cten1 Cabac1 Cind1 Cemtri1 Cralt1 Cdar1 Cetra1 Cataza1 CUNK1 COTHER1;
  by person_id arv_date ;
 *if sum(of CSTAVUDINE CLAMIVUDINE--COTHER)>1 then onarv=1;

run ;
/* bring in patients on express care to be on ARVs

PROC IMPORT OUT= WORK.encounters1 
            DATAFILE= "C:\DATA\DATA SETS\encountersall.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/


PROC IMPORT OUT= WORK.encounters1a 
            DATAFILE= "C:\DATA\CSV DATASETS\encounters1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


PROC IMPORT OUT= WORK.encounters1b 
            DATAFILE= "C:\DATA\CSV DATASETS\encounters2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data encounters1;
set encounters1a encounters1b;
run;



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


data arv_final1;
length person_id 8.;
merge arvuse xpress Carvmeds arvmeds;
by person_id arv_date ;
if xp=1 then onarv=1;
IF arvstart=1 THEN onarv=1;
run;
/*
data arv_firstdrugs;
set arv_final1;
by person_id arv_date ;
if first.person_id;
firstdrug=1;
run;
*/ 
data arv_final;
set arv_final1 ;
by person_id arv_date ;
if onarv ne 1 and sum(CSTAVUDINE--OTHER)>=1 then onarv=1;
else if (onarv ne 1 and arvstart=1) or (onarv ne 1 and arvsubstitution=1) or (onarv ne 1 and arveverchanged=1) or (onarv ne 1 and arvdosechange=1)then onarv=1;
*drop firstdrug;
run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE arvs arvsall onarv arvchangeformulation arvreasonstop previousonarv arvchangeregimen arvstopall arvstart 
		arvdosechange arvrestart arvsubstitution arveverchanged stav1 lam1 nev1 efav1 nelf1 lop1 rit1 did1 azt1
		ten1 abac1 ind1 emtri1 ralt1 dar1 etra1 ataza1  arv_final1 UNK1 OTHER1 stav lam nev efav nelf lop rit 
		did azt ten abac ind emtri ralt dar etra ataza UNK OTHER arvuse arvmeds mednotuse usestmed usenotmed 
		arv_firstdrugs encounters1a encounters1b xpress arv_final1
        Cstav1 Clam1 Cnev1 Cefav1 Cnelf1 Clop1 Crit1 Cdid1 Cazt1
		Cten1 Cabac1 Cind1 Cemtri1 Cralt1 Cdar1 Cetra1 Cataza1  arv_final1 Carvmeds CUNK1 COTHER1 Cstav Clam Cnev Cefav Cnelf Clop Crit 
		Cdid Cazt Cten Cabac Cind Cemtri Cralt Cdar Cetra Cataza CUNK COTHER
;
		RUN;
   		QUIT;

	

%MEND create_arvs;
%create_arvs;
