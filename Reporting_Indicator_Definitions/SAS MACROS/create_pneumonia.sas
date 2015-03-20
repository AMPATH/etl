


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pneumonia();	


PROC IMPORT OUT= WORK.pneumonia 
            DATAFILE= "C:\DATA\CSV DATASETS\pneumonia.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data pneumonia1;
Format pneumonia $1. pneum_date ddmmyy10.;
set pneumonia;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
pneum_date=mdy(mm,dd,yy);
pneumonia=1;
keep person_id pneum_date pneumonia;
run;


proc sort data=pneumonia1 nodupkey  out=pneumonia_final; by person_id pneum_date ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pneumonia pneumonia1;
		RUN;
   		QUIT;


%MEND create_pneumonia;


%create_pneumonia;
