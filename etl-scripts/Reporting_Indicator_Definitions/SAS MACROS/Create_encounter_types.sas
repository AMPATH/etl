
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_encounter_type ();


PROC IMPORT OUT= WORK.encounter_type 
            DATAFILE= "C:\DATA\CSV DATASETS\encounter_type.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data enct;
set encounter_type ;
keep encounter_type_id name;
rename 
encounter_type_id=encounter_type
name=encounter_name;
run;

proc sort data=enct nodupkey out=encounter_type_final; by encounter_type;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE encounter_type enct;
		RUN;
   		QUIT;


%Mend create_encounter_type;
%create_encounter_type;
