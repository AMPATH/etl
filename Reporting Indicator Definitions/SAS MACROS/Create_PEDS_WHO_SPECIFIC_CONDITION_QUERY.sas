


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO Create_PEDS_WHO_CONDITION();	


PROC IMPORT OUT= WORK.AIDS_DEFINING_ILLNESSES 
            DATAFILE= "C:\DATA\CSV DATASETS\AIDS_DEFINING_ILLNESSES.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data PEDS_WHO;
format appdate ddmmyy10.;
set AIDS_DEFINING_ILLNESSES (rename=name=PEDS_WHO_CONDITION);
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
appdate=mdy(mm,dd,yy);
if concept_id in(1225);
drop dd mm yy obs_datetime value_coded concept_id ;
run;


proc sort data=PEDS_WHO nodupkey out=PEDS_WHO_CONDITION_final dupout=duppeds_who; by person_id appdate ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE AIDS_DEFINING_ILLNESSES  PEDS_WHO;
		RUN;
   		QUIT;


%MEND Create_PEDS_WHO_CONDITION;
%Create_PEDS_WHO_CONDITION;
