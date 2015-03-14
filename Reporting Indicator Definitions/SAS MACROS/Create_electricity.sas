
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_electricity();


PROC IMPORT OUT= WORK.electricity 
            DATAFILE= "C:\DATA\CSV DATASETS\employed_electricity.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data elec;
set electricity;
if concept_id=5609;
electricity=1;
run;

proc sort data=elec nodupkey out=electricity_final; by person_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE electricity elec;
		RUN;
   		QUIT;



%Mend create_electricity;
%Create_electricity;
