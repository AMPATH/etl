


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_current_visit_types();	

PROC IMPORT OUT= WORK.visit_types
            DATAFILE= "C:\DATA\CSV DATASETS\current_visit_types.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;




data visit_types1;
format visit_date ddmmyy10.;
set visit_types;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
visit_date=mdy(mm,dd,yy);

drop  dd mm yy obs_datetime ;
run;

data visit_types2;
set visit_types1;
if concept_id = 1839 and value_coded=1246 then visit_type ='Scheduled    ';
  else if concept_id = 1246 and value_coded=1065 then visit_type ='Scheduled    ';

	 else if  concept_id = 1839 and value_coded in (1837,1838) then visit_type ='Unscheduled ';
	    else if  concept_id = 1246 and value_coded = 1066 then visit_type ='Unscheduled ';




drop concept_id  value_coded;
RUN;


proc sort data=visit_types2 out=visit_types_final nodupkey; by person_id visit_date;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE  visit_types2  visit_types1 visit_types;
		RUN;
   		QUIT;


%MEND create_current_visit_types;
%create_current_visit_types;
