
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_outreach();


PROC IMPORT OUT= WORK.outreach1 
            DATAFILE= "C:\DATA\CSV DATASETS\outreach.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data outreach;
format outreach_date ddmmyy10.;
set outreach1;
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
outreach_date=mdy(mm,dd,yy);
rename patient_id=person_id;
drop dd mm yy encounter_datetime;
run;

proc sort data=outreach nodupkey out=outreach_final; by person_id outreach_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE outreach outreach1;
		RUN;
   		QUIT;



%Mend create_outreach;
%Create_outreach;
