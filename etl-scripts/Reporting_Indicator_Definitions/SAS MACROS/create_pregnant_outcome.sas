


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pregnant_outcome();	


PROC IMPORT OUT= WORK.preg_outcome1 
            DATAFILE= "C:\DATA\CSV DATASETS\preg_outcome.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data preg_outcome;
Format preg_outcome $30. preg_outcomedate ddmmyy10.;
set preg_outcome1;
if value_coded =1843 then do; preg_outcome='Live birth';end;
if value_coded =1844 then do; preg_outcome='early infant death ';end;
if value_coded =1845 then do; preg_outcome='late infant death ';end;
if value_coded =1993 then do; preg_outcome='stillbirth ';end;
if value_coded =48 then do; preg_outcome='miscarriage  ';end;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
preg_outcomedate=mdy(mm,dd,yy);
*if preg_outcomedate>=mdy(03,01,2008);
keep person_id preg_outcome preg_outcomedate;
run;

proc sort data=preg_outcome out=preg_outcome_final nodupkey ; by person_id preg_outcomedate ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE preg_outcome1  preg_outcome;
		RUN;
   		QUIT;


%MEND create_pregnant_outcome;

%create_pregnant_outcome;
