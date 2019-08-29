


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
Format tb_date  ddmmyy10.;
drop dd mm yy obs_datetime ;
run;


/*active TB*/

data tbtreat(keep=person_id  tb_date tbtx)  
tbstart(keep=person_id tbstart tb_date)
tbstop(keep=person_id tbstop tb_date)
tbstop_reason(keep=person_id tbstop_reason tb_date)
tbcontinue(keep=person_id tbcontinue tb_date);

set tb;
  *1256=START DRUGS, 1257=CONTINUE REGIMEN, 1258=CHANGE FORMULATION, 1259=CHANGE REGIMEN, 1260=STOP ALL ;
  if (concept_id=1268 and value_coded in (1256,1257,1259)) or (concept_id=5965 and value_numeric=1) then do; 
	tbtx=1 ;	
	output tbtreat;
	end;

  if concept_id=1268 and value_coded in (1256) then do;
	tbstart=1;
	output tbstart;
	end;

	if concept_id=1268 and value_coded in (1260) then do;
	tbstop=1;
	output tbstop;
	end;

	
	if concept_id=1269 and value_coded in (1267) then do;
	tbstop_reason='completed trtment';
	output tbstop_reason;
	end;
	
	if concept_id=1268 and value_coded in (1257) then do;
	tbcontinue=1;
	output tbcontinue;
	end;
run ;


proc sort data=tbtreat ; by person_id tb_date ;
data tbtreat_use ; set tbtreat ;
  by person_id tb_date ;
  if last.tb_date ;
run ;


proc sort data=tbstart ; by person_id tb_date ;
data tbstart_use ; set tbstart ;
  by person_id tb_date ;
  if last.tb_date ;
run ;



proc sort data=tbstop ; by person_id tb_date ;
data tbstop_use ; set tbstop ;
  by person_id tb_date ;
  if last.tb_date ;
run ;



proc sort data=tbstop_reason ; by person_id tb_date ;
data tbstop_reason_use ; set tbstop_reason ;
  by person_id tb_date ;
  if last.tb_date ;
run ;


proc sort data=tbcontinue ; by person_id tb_date ;
data tbcontinue_use ; set tbcontinue ;
  by person_id tb_date ;
  if last.tb_date ;
run ;

**  code TB treatment meds ** ;


** 1111 PATIENT REPORTED CURRENT TUBERCULOSIS TREATMENT ;
** 1270	TUBERCULOSIS TREATMENT STARTED	;
** 1193 CURRENT MEDICATIONS	 ;
** ethambutol ethambutolinh rifampicin rifater rifafour rifinah streptomycin pyrazinamide ;
data eth(keep=person_id  tb_date ethambutol) ethinh(keep=person_id tb_date ethambutolinh)
	rifam(keep=person_id  tb_date rifampicin) rifat(keep=person_id  tb_date rifater)
	rifaf(keep=person_id  tb_date rifafour) rifin(keep=person_id  tb_date rifinah)
	strep(keep=person_id  tb_date streptomycin) pyraz(keep=person_id  tb_date pyrazinamide)
	inhtx(keep=person_id  tb_date inhtbtx) ; 
	set tb(where=(concept_id in(1111,1193,1270)))  ; 
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


proc sort data=eth ; by person_id tb_date ;
data eth1 ; set eth ;
  by person_id tb_date ;
  if last.tb_date ;
run ;
proc sort data=ethinh ; by person_id tb_date ;
data ethinh1 ; set ethinh ;
  by person_id tb_date ;
  if last.tb_date ;
run ;
proc sort data=rifam ; by person_id tb_date ;
data rifam1 ; set rifam ;
  by person_id tb_date ;
  if last.tb_date ;
run ;
proc sort data=rifat ; by person_id tb_date ;
data rifat1 ; set rifat ;
  by person_id tb_date ;
  if last.tb_date ;
run ;
proc sort data=rifaf ; by person_id tb_date ;
data rifaf1 ; set rifaf ;
  by person_id tb_date ;
  if last.tb_date ;
run ;
proc sort data=rifin ; by person_id tb_date ;
data rifin1 ; set rifin ;
  by person_id tb_date ;
  if last.tb_date ;
run ;
proc sort data=strep ; by person_id tb_date ;
data strep1 ; set strep ;
  by person_id tb_date ;
  if last.tb_date ;
run ;
proc sort data=pyraz ; by person_id tb_date ;
data pyraz1 ; set pyraz ;
  by person_id tb_date ;
  if last.tb_date ;
run ;
proc sort data=inhtx ; by person_id tb_date ;
data inhtx1 ; set inhtx ;
  by person_id tb_date ;
  if last.tb_date ;
run ;

data tbmeds(keep=person_id tb_date  ethambutol ethambutolinh rifampicin rifater rifafour rifinah streptomycin pyrazinamide) 
	inhonly inheth ; 
	merge eth1 ethinh1 rifam1 rifat1 rifaf1 rifin1 strep1 pyraz1 inhtx1 ;
  by person_id tb_date ;
  if inhtbtx=1 and ethambutol=1 then do ;
	ethambutolinh=1 ; ethambutol=. ; 
  end ;
  if inhtbtx=1 and (ethambutolinh=1 or ethambutol=1) then output inheth ;
  else if inhtbtx=1 and ethambutolinh=. and ethambutol=. then output inhonly ;
  else output tbmeds ;
run ;

data tb_final;
merge Tbtreat_use(in=a) tbstart_use tbcontinue_use tbstop_use tbstop_reason_use Tbmeds ;
by person_id tb_date;
;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE tb tb1 tba tbb tbstart_use Tbtreat_use tbstop_use Tbmeds eth1 ethinh1 rifam1 rifat1 rifaf1 
		rifin1 strep1 pyraz1 inhtx1 tbstop_reason_use tbcontinue_use ;
		RUN;
   		QUIT;


%MEND create_tb;
%create_tb;

