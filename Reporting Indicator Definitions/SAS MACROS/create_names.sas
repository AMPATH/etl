


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_names();

libname t 'C:\DATA\CSV DATASETS';

data names;
format person_id1 8.;
set t.names;
person_id1=person_id*1;
drop person_id;
rename person_id1=person_id;
run;


/*
PROC IMPORT OUT= WORK.names 
            DATAFILE= "C:\Documents and Settings\Administrator\My Documents\ESIKUKU\MACROS\Datasets\names.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/

proc sort nodupkey data=names out=names_final; by person_id;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE names ;
		RUN;
   		QUIT;

	

%MEND create_names;

%create_names;
