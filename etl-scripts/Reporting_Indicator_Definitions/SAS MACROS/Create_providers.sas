
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_providers();

PROC IMPORT OUT= WORK.PROVIDERS 
            DATATABLE= "provider" 
            DBMS=ACCESS97 REPLACE;
     DATABASE="C:\DATA\CSV DATASETS\transfer.mdb"; 
     SCANMEMO=YES;
     USEDATE=NO;
     SCANTIME=YES;
RUN;


data PROVIDERS1;
set PROVIDERS;
id1=scan(system_id,1, '-');
drop user_id;
rename id1=user_id;
drop Field7 id system_id;
run;



proc sort data=PROVIDERS1 nodupkey out=PROVIDERS_final; by user_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE PROVIDERS1 PROVIDERS;
		RUN;
   		QUIT;


%Mend create_providers;
%create_providers;
