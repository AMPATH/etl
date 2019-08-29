
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_identifiers();



PROC IMPORT OUT= WORK.patient_identifier 
            DATAFILE= "C:\DATA\CSV DATASETS\identifier.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


proc format;
value idtype	1='old ampathid'
				3='New ampathid'
				4='invalid ampathid'
				8='universal id';
			
data identifier1(keep=patient_id identifier_type identifier rename=patient_id=person_id);
set patient_identifier ;
if voided=0 ;
format identifier_type idtype.;
run;




proc sort data=identifier1 nodupkey out=identifier_final; by person_id identifier_type; run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE patient_identifier identifier1 ;
		RUN;
   		QUIT;



%Mend create_identifiers;
%Create_identifiers;
