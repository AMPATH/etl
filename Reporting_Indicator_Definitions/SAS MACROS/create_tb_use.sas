


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_tb();	

PROC IMPORT OUT= WORK.TBa 
            DATAFILE= "C:\DATA\CSV DATASETS\tb1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.TBb 
            DATAFILE= "C:\DATA\CSV DATASETS\tb2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

proc sort data=tba; by person_id; run;
proc sort data=tbb; by person_id; run;

data tb;
set tba tbb;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
tb_date=mdy(mm,dd,yy);
Format tb_date ddmmyy10.;
drop dd mm yy obs_datetime;
run;


/*active TB*/



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





/*extra pulmonary*


data tbpulmonary; 
set tb(where=(concept_id in (1208,1225,6048) 
	and value_coded in (42,58,831,2104,2105,5338)))  ;
  tbpulmonary=1 ;
  if value_coded=2104 then tbpulmonaryPC='P' ;
  else if value_coded=2105 then tbpulmonaryPC='C' ;
  Label tbpulmonary='Staging: Pulmonary Tuberculosis (current)'
	tbpulmonaryPC='Pulmonary Tuberculosis Presumed/Confirmed' ;
  keep person_id  tb_date tbpulmonary tbpulmonaryPC ;
run ; 
proc sort data=tbpulmonary; by person_id tb_date tbpulmonaryPC ;
data tbpulmonary1 ; set tbpulmonary;
  by person_id tb_date tbpulmonaryPC ;
  if last.tb_date ;
run ;

*/



data tbjoin;
merge tbtrt tbmeds;
by person_id tb_date;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE tb tb1 tba tbb;
		RUN;
   		QUIT;


%MEND create_tb;
%create_tb;

