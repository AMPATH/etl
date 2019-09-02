
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_person_voided();

PROC IMPORT OUT= WORK.person_voided 
            DATAFILE= "C:\DATA\CSV DATASETS\persons_voided.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

	



proc sort data=person_voided nodupkey out=person_voided_final(keep=person_id); by person_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE person;
		RUN;
   		QUIT;


%Mend create_person_voided;
%Create_person_voided;
