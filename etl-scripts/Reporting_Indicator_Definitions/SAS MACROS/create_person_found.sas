


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_person_found();	


PROC IMPORT OUT= WORK.found 
            DATAFILE= "C:\DATA\CSV DATASETS\found_outreach.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data found1;
format found_date ddmmyy10.;
set found;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
found_date=mdy(mm,dd,yy);
found=1;
drop dd mm yy obs_datetime value_numeric concept_id;
run;


proc sort data=found1 nodupkey out=found_final; by person_id found_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE found1 found;
		RUN;
   		QUIT;


%MEND  create_person_found;
%create_person_found;
