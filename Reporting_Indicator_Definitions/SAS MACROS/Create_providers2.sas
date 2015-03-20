
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_providers();

PROC IMPORT OUT= WORK.PROVIDERS 
            DATATABLE= "providers" 
            DBMS=ACCESS97 REPLACE;
     DATABASE="C:\DATA\CSV DATASETS\transfer.mdb"; 
     SCANMEMO=YES;
     USEDATE=NO;
     SCANTIME=YES;
RUN;


proc sort data=PROVIDERS nodupkey out=PROVIDERS_final; by person_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE PROVIDERS1 PROVIDERS;
		RUN;
   		QUIT;


%Mend create_providers;
%create_providers;
