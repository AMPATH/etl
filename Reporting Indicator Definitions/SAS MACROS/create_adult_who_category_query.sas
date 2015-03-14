


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_adult_who_category_query();	


PROC IMPORT OUT= WORK.AIDS_DEFINING_ILLNESSES 
            DATAFILE= "C:\DATA\CSV DATASETS\AIDS_DEFINING_ILLNESSES.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data ADULT_WHO;
format appdate ddmmyy10.;
set AIDS_DEFINING_ILLNESSES(RENAME=NAME=adult_who_category);
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
appdate=mdy(mm,dd,yy);
if concept_id in(5356);
drop dd mm yy obs_datetime value_coded concept_id ;
run;


proc sort data=ADULT_WHO nodupkey out=ADULT_WHO_CATEGORY_QUERY_final dupout=duppeds_who; by person_id appdate ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE AIDS_DEFINING_ILLNESSES  ADULT_WHO;
		RUN;
   		QUIT;


%MEND create_adult_who_category_query;
%create_adult_who_category_query;
