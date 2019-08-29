


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_tb();	

Libname tmp 'C:\DATA\CSV DATASETS';

data tb;
set tmp.tb1 tmp.tb2;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
tb_date=mdy(mm,dd,yy);
format tb_date ddmmyy10.;
drop dd mm yy obs_datetime ;
run;



data tbtx(keep=person_id tb_date tbtx)  
tbchangeregimen(keep=person_id tb_date tbchangeregimen) 
tbstopall(keep=person_id tb_date tbstopall) 
tbstart(keep=person_id tb_date tbstart)   ;
; set tb ;
  *1256=START DRUGS, 1257=CONTINUE REGIMEN, 1258=CHANGE FORMULATION, 1259=CHANGE REGIMEN, 1260=STOP ALL ;
  if (concept_id=1268 and value_coded in (1256,1257,1259)) or (concept_id=5965 and value_numeric=1065) then do; tbtx=1 ; 
  if tbtx=. then delete ; output tbtx;end;

  
if (concept_id=1268 and value_coded in (1259))  then do; tbchangeregimen=1 ; 
  if tbchangeregimen=. then delete ; output tbchangeregimen;end;
  
if (concept_id=1268 and value_coded in (1260))  then do; tbstopall=1 ;
  if tbstopall=. then delete ; output tbstopall; end;

if (concept_id=1268 and value_coded in (1256))  then do; tbstart=1 ;
  if tbstart=. then delete ; output tbstart; end;

run ;

proc sort data=tbtx nodupkey ; by person_id tb_date ;run;
proc sort data=tbchangeregimen nodupkey ; by person_id tb_date ;run;
proc sort data=tbstopall nodupkey ; by person_id tb_date ;run;
proc sort data=tbstart nodupkey ; by person_id tb_date ;run;

data tbtrt;
merge tbtx tbchangeregimen tbstopall tbstart;
by person_id tb_date ;
run;



**  code TB treatment meds 

** 1111 PATIENT REPORTED CURRENT TUBERCULOSIS TREATMENT 
** 1270	TUBERCULOSIS TREATMENT STARTED	
** 1193 CURRENT MEDICATIONS	 


'Ethambutol=745, Ethambutol & INH=1108, Ethambutol Current=5833'
'767=RIFAMPICIN, 1194=RIFAMPICIN AND ISONIAZID (aka Rifinah)' 
768=RIFAMPICIN ISONIAZID AND PYRAZINAMIDE (aka Rifater)' 
1131=RIFAMPICIN ISONIAZID PYRAZINAMIDE AND ETHAMBUTOL (aka Rifafour), 5831=RIFAMPIN CURRENT' 
438=STREPTOMYCIN, 5835=STREPTOMYCIN CURRENT, 437=STREPTOMYCIN INJECTION, 439=STREPTOMYCIN TABLETS
'5829 PYRAZINAMIDE' 
'656=ISONIAZID, 5827=ISONIAZID CURRENT' ;
** ;

proc freq data=tb ;
  title '1111=PATIENT REPORTED CURRENT TUBERCULOSIS TREATMENT, 1270=TUBERCULOSIS TREATMENT STARTED' ;
  where concept_id in (1111,1270,1193) ;
  tables concept_id * value_coded /missing ;
run   ;


** ethambutol ethambutolinh rifampicin rifater rifafour rifinah streptomycin pyrazinamide ;

data eth(keep=person_id tb_date ethambutol) ethinh(keep=person_id tb_date ethambutolinh)
	rifam(keep=person_id tb_date rifampicin) rifat(keep=person_id tb_date rifater)
	rifaf(keep=person_id tb_date rifafour) rifin(keep=person_id tb_date rifinah)
	strep(keep=person_id tb_date streptomycin) pyraz(keep=person_id tb_date pyrazinamide)
	inhtx(keep=person_id tb_date inhtbtx) ; 
	set tb (where=(concept_id in(1111,1193,1270))) ; 
  if value_coded=745 then do ;
	ethambutol=1 ;
	output eth ;
  end ;
  else if value_coded=1108 then do ;
	ethambutolinh=1 ;
	output ethinh ;
  end ;
  else if value_coded=767 then do ;
	rifampicin=1 ;
	output rifam ;
  end ;
  else if value_coded=768 then do ;
	rifater=1 ;
	output rifat ;
  end ;
  else if value_coded=1131 then do ;
	rifafour=1 ;
	output rifaf ;
  end ;
  else if value_coded=1194 then do ;
	rifinah=1 ;
	output rifin ;
  end ;
  else if value_coded=438 then do ;
	streptomycin=1 ;
	output strep ;
  end ;
  else if value_coded=5829 then do ;
	pyrazinamide=1 ;
	output pyraz ;
  end ;
  else if value_coded in (656,5827) then do ;
	inhtbtx=1 ;
	output inhtx ;
  end ;
run ;




proc sort data=eth nodupkey ; by person_id tb_date ;run;
proc sort data=ethinh nodupkey ; by person_id tb_date ;run;
proc sort data=rifam nodupkey ; by person_id tb_date ;run;
proc sort data=rifat nodupkey ; by person_id tb_date ;run;
proc sort data=rifaf nodupkey ; by person_id tb_date ;run;
proc sort data=rifin nodupkey ; by person_id tb_date ;run;
proc sort data=strep nodupkey ; by person_id tb_date ;run;
proc sort data=pyraz nodupkey ; by person_id tb_date ;run;
proc sort data=inhtx nodupkey ; by person_id tb_date ;run;


data tbmeds inheth inhonly;
	merge eth ethinh rifam rifat rifaf rifin strep pyraz inhtx ;
  by  person_id tb_date ;
  if inhtbtx=1 and ethambutol=1 then do ;
	ethambutolinh=1 ; ethambutol=. ; 
  end ;
  if inhtbtx=1 and (ethambutolinh=1 or ethambutol=1) then output inheth ;
  else if inhtbtx=1 and ethambutolinh=. and ethambutol=. then output inhonly ;
  else output tbmeds ;
run ;

/* Mike added below dataset so as to capture those patients who are currently on TB */
data tb_current;
set tb;
if concept_id=1111;
run;
proc sort data=tb_current nodupkey  out=tbcurrent_final; by person_id tb_date; run;


data tb1;
set tb;

if concept_id in(1268) and value_coded=1256 then do; tb_plan='start drugs           ' ; tb=1;end;
if concept_id in(1268) and value_coded=1107 then do;tb_plan='none' ; tb=0;end;
if concept_id in(1268) and value_coded=1259 then do; tb_plan='change regimen' ; tb=1;end;
if concept_id in(1268) and value_coded=1257 then do; tb_plan='continue regimen' ; tb=1;end;
if concept_id in(1268) and value_coded=1260 then do; tb_plan='stop all' ; tb=0;end;
if concept_id in(1268) and value_coded=1850 then do; tb_plan='drug restart' ; tb=1;end;
if concept_id in(1268) and value_coded=2160 then do; tb_plan='TB defaulter' ; tb=1;end;
if concept_id in(1268) and value_coded=2161 then do; tb_plan='MDRTR' ; tb=1;end;
if concept_id in(1268) and value_coded=981 then do; tb_plan='dosing change' ; tb=1;end;
if concept_id in(1269) and value_coded=1267 then do; tb_plan='completed treatment' ; tb=0;end;
if concept_id in(1266) and value_coded=58 then do; TBpropystop_reason='TB' ; tb=1;end;
if concept_id in(1270) then do;  tb=1;end;
if concept_id in(1111) and value_coded=1107 then do;  tb=0;end;
if concept_id in(1111) and value_coded not in(1107) then do;  tb=1;end;
if concept_id in(2022,1506)  then do;  tb=1;end;
if value_coded=58 then do; tb=1;end;
keep  tb person_id tb_date tb_plan TBpropystop_reason value_coded concept_id;
run;



proc sort data=tb1 nodupkey  out=tb_final; by person_id tb_date tb_plan tb  ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE tb tb1 tba tbb tb_current eth ethinh rifam rifat rifaf rifin strep pyraz inhtx
				tbchangeregimen tbmeds tbstart tbstopall tbtrt tbtx ; /* Mike added this line to reduce the outputted datasets in work */
		RUN;
   		QUIT;


%MEND create_tb;
%create_tb;

