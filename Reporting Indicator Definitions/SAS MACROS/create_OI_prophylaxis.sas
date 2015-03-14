


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_OI_prophylaxis();	


PROC IMPORT OUT= WORK.oi_prophylaxis
            DATAFILE= "C:\DATA\CSV DATASETS\oi prophylaxis.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data oi_prophylaxis1;
Format appdate ddmmyy10.;
set oi_prophylaxis;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
appdate=mdy(mm,dd,yy);
if (concept_id=1261 and value_coded in (981,1256,1257,1259)) or (concept_id=1263 and value_coded ne .) 
	or (concept_id in (1109,8346) and value_coded in (92,916)) then oiproph=1 ;
  if oiproph =. then delete ;
  keep person_id  appdate oiproph ;
run ;
run;


proc sort data=oi_prophylaxis1 ; by person_id appdate ;
data oi_prophylaxis_final ; set oi_prophylaxis1;
  by person_id appdate ;
  if last.appdate ;
run ;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE oi_prophylaxis1 oi_prophylaxis;
		RUN;
   		QUIT;


%MEND create_OI_prophylaxis;
%create_OI_prophylaxis;
