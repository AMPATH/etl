


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_relationship();	


PROC IMPORT OUT= WORK.relationship 
            DATAFILE= "C:\DATA\CSV DATASETS\relationship.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

proc sort data=relationship;by person_a;run;


data relationship_parent(rename=person_a=person_id);
set relationship;
by person_a;
keep person_a person_b relationship;
run;

proc sort data=relationship;by person_b;run;

data relationship_child(rename=person_b=person_id);
set relationship;

keep person_a person_b relationship;
run;


proc sort data=relationship_parent nodupkey out=relationship_parent_final; by person_id; run;
proc sort data=relationship_child nodupkey out=relationship_child_final; by person_id; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE relationship_parent relationship_child relationship ;
		RUN;
   		QUIT;


%MEND create_relationship;
%create_relationship;
