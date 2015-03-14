


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_hiv_infected();	


PROC IMPORT OUT= WORK.hiv_infected
            DATAFILE= "C:\DATA\CSV DATASETS\hiv_infected.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data hiv_infected1;
Format app_date ddmmyy10.;
set hiv_infected;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);
hiv_infected=1;
drop value_coded dd mm yy obs_datetime concept_id;
run;


proc sort data=hiv_infected1 nodupkey  out=hiv_infected_final ; by person_id app_date  ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE hiv_infected1 hiv_infected;
		RUN;
   		QUIT;


%MEND create_hiv_infected;
%create_hiv_infected;
