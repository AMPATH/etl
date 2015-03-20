


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_found_dead();	


PROC IMPORT OUT= WORK.found_dead 
            DATAFILE= "C:\DATA\CSV DATASETS\found_dead.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data found_dead1;
format found_date ddmmyy10.;
set found_dead;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
found_date=mdy(mm,dd,yy);
found_dead=1;
drop dd mm yy obs_datetime value_coded concept_id;
run;


proc sort data=found_dead1 nodupkey out=found_dead_final; by person_id found_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE found_dead1 found_dead;
		RUN;
   		QUIT;


%MEND  create_found_dead;
%create_found_dead;
