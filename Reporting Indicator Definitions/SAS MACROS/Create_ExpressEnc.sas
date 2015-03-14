
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_expressenc();


PROC IMPORT OUT= WORK.expressenc1 
            DATAFILE= "C:\DATA\DATA SETS\expressenc.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


/*PROC IMPORT OUT= WORK.encounters2 
            DATAFILE= "C:\DATA\DATA SETS\encounters2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;*/


data expressenc;
set expressenc1 ;
run;


data expressenc1a(rename=(patient_id=person_id)) ;
format encounter_date ddmmyy10.;
set expressenc;
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
encounter_date=mdy(mm,dd,yy);
drop dd mm yy encounter_datetime;
run;

proc sort data=expressenc1a; by person_id encounter_date; run;

data expressenc_final;
set expressenc1a;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE expressenc1  expressenc expressenc1a;
		RUN;
   		QUIT;



%Mend create_expressenc;
%Create_expressenc;
