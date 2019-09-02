


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_Problem_added();	


PROC IMPORT OUT= WORK.problem 
            DATAFILE= "C:\DATA\CSV DATASETS\problem_added.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


/*PROC IMPORT OUT= WORK.concept_names 
            DATAFILE= "C:\DATA\CSV DATASETS\concept_names.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;*/

Libname tmp 'C:\DATA\CSV DATASETS';

data problem1(rename=value_coded=concept_id);
format apptdate ddmmyy10.;
set problem;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
apptdate=mdy(mm,dd,yy);
drop dd mm yy obs_datetime concept_id; 
run;

data concept_names ;
set tmp.concept_names;
concept_id1=concept_id*1;
drop concept_id;
rename concept_id1=concept_id;
run;

proc sort data=problem1; by concept_id;
proc sort data=concept_names out=concepts dupout=dupconcepts nodupkey; by concept_id; run;

data problem2(rename=name=problem_added );
merge problem1(in=a) concepts ;
by concept_id;
if a;
run;

proc sort data=problem2 nodupkey out=problem_added_final; by person_id apptdate problem_added; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE problem concept_names problem1 dupconcepts concepts problem2 ;
		RUN;
   		QUIT;


%MEND create_Problem_added;
%create_Problem_added;
