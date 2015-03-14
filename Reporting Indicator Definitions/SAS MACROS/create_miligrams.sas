


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_miligrams();	


PROC IMPORT OUT= WORK.Miligrams 
            DATAFILE= "C:\DATA\DATA SETS\miligrams.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data mili2;
Format Miligrams 3.1 mg_date ddmmyy10.;
set Miligrams;

dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
mg_date=mdy(mm,dd,yy);
Miligrams=value_numeric;
keep person_id mg_date Miligrams;
run;


proc sort data=mili2   out=miligram_final /*dupout=dupmili*/; by person_id mg_date Miligrams  ; run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE Miligrams mili2 dupmili ;
		RUN;
   		QUIT;


%MEND create_miligrams;
%create_miligrams;
