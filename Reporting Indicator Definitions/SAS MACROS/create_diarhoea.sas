


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_diarhoea();	


PROC IMPORT OUT= WORK.diarhoea 
            DATAFILE= "C:\DATA\CSV DATASETS\diarhoea.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data diarhoea1;
Format Diarhoea $1. diar_date ddmmyy10.;
set diarhoea;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
diar_date=mdy(mm,dd,yy);
diarhoea=1;
keep person_id diar_date diarhoea;
run;


proc sort data=diarhoea1 nodupkey  out=diarhoea_final; by person_id diar_date ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE diarhoea diarhoea1;
		RUN;
   		QUIT;


%MEND create_diarhoea;
%create_diarhoea;
