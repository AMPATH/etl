


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_location();	

libname tmp 'C:\DATA\DATA SETS';

/*
PROC IMPORT OUT= WORK.location_out 
            DATAFILE= "C:\SAS_CODE\DISCORDANT\location.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/

data location_out;
set tmp.location;
run;


data location;
set location_out;
run;

proc sort data=location  out=location_final nodupkey; by location_id ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE location location_out ;
		RUN;
   		QUIT;


%MEND create_location;
%create_location;
