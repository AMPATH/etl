


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_malaria();	


PROC IMPORT OUT= WORK.malaria 
            DATAFILE= "C:\DATA\CSV DATASETS\malaria.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data malaria1;
format malaria_date ddmmyy10.;
set malaria;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
malaria_date=mdy(mm,dd,yy);
if value_coded in(703,123) then malaria=1;
drop dd mm yy obs_datetime value_coded concept_id;
run;


proc sort data=malaria1 nodupkey out=malaria_final; by person_id malaria_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE malaria malaria1;
		RUN;
   		QUIT;


%MEND create_malaria;
%create_malaria;
