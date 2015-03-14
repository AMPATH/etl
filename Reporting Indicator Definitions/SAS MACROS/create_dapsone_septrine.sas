


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_dapsone_septrine();	


PROC IMPORT OUT= WORK.Dapsone_septrine 
            DATAFILE= "C:\DATA\CSV DATASETS\dapson_septrine.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data Dapsone_septrine1;
Format appdate ddmmyy10.;
set Dapsone_septrine;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
appdate=mdy(mm,dd,yy);
if (concept_id=1261 and value_coded in (981,1256,1257,1259)) or (concept_id=1263 and value_coded ne .) 
	or (concept_id=1109 and value_coded in (92,916)) then oiproph=1 ;
  if oiproph =. then delete ;
  keep person_id  appdate oiproph ;
run ;
run;


proc sort data=Dapsone_septrine1 ; by person_id appdate ;
data Dapsone_septrine_final ; set Dapsone_septrine1 ;
  by person_id appdate ;
  if last.appdate ;
run ;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE Dapsone_septrine Dapsone_septrine1;
		RUN;
   		QUIT;


%MEND create_dapsone_septrine;
%create_dapsone_septrine;
