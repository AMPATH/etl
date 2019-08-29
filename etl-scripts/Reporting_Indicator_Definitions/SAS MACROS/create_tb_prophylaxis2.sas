


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_tb_prophylaxis();	

PROC IMPORT OUT= WORK.TB_prophylaxis 
            DATAFILE= "C:\DATA\CSV DATASETS\TB_prophylaxis_plan.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data TB_prophylaxis1;
Format tbprophy_date ddmmyy10.;
set TB_prophylaxis;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
tbprophy_date=mdy(mm,dd,yy);
drop obs_datetime dd mm yy;
run;



data tbproph ;
set TB_prophylaxis1(where=(concept_id in (1110,1264,1265)) ) ;
  ** 981=DOSING CHANGE, 1107=NONE, 1256=START DRUGS, 1257=CONTINUE REGIMEN, 1259=CHANGE REGIMEN, 1260=STOP ALL ;
  ** ISONIAZID=656 1107=none ;
  if (concept_id=1265 and value_coded in (981,1256,1257,1259)) or (concept_id=1264 and value_coded ne .)
	or (concept_id=1110 and value_coded = 656) then inh=1 ;
  if inh =. then delete ;
  keep person_id  tbprophy_date inh ;
run ;
proc sort data=tbproph ; by person_id tbprophy_date ;

data Tb_prophylaxis_final ; set tbproph ;
  by person_id tbprophy_date ;
  if last.tbprophy_date ;
run ;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE TB_prophylaxis1 TB_prophylaxis;
		RUN;
   		QUIT;


%MEND create_tb_prophylaxis;
%create_tb_prophylaxis;

