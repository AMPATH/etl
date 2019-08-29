
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_employed();


PROC IMPORT OUT= WORK.employed 
            DATAFILE= "C:\DATA\CSV DATASETS\employed_electricity.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data emp;
set employed;
if concept_id in(1678,5608);
employed=1;
keep person_id employed;
run;

proc sort data=emp out=employed_final nodupkey; by person_id;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE employed emp;
		RUN;
   		QUIT;


%Mend create_employed;
%Create_employed;
