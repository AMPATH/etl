


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_aes();	


PROC IMPORT OUT= WORK.aes 
            DATAFILE= "C:\DATA\CSV DATASETS\side_effects.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data aes1;
Format aes $1. aes_date ddmmyy10.;
set aes;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
aes_date=mdy(mm,dd,yy);
aes=1;
keep person_id aes_date aes;
run;


proc sort data=aes1 nodupkey  out=aes_final; by person_id aes_date ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE aes aes1;
		RUN;
   		QUIT;


%MEND create_aes;

%create_aes;
