
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_user();


PROC IMPORT OUT= WORK.user 
            DATAFILE= "C:\DATA\CSV DATASETS\users.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data user1;
set user;
*if username ne'' ;
drop retired;
run;
proc sort data=user1 nodupkey out=user_final; by user_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE user user1;
		RUN;
   		QUIT;


%Mend create_user;
%Create_user;
