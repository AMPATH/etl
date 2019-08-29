


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pmtct_prophylaxis();	

PROC IMPORT OUT= WORK.pmtct_arv 
            DATAFILE= "C:\DATA\CSV DATASETS\pmtct_arv.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data pmtct;
format pmtct_date ddmmyy10.;
set pmtct_arv ;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
pmtct_date=mdy(mm,dd,yy);
pmtct=1;
drop dd mm yy obs_datetime;
run;

proc sort data=pmtct out=pmtct_final nodupkey; by person_id pmtct_date;run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pmtct pmtct_arv   ;
		RUN;
   		QUIT;


%MEND create_pmtct_prophylaxis;

%create_pmtct_prophylaxis;
