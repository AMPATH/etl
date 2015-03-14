
PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_viral_detect();


PROC IMPORT OUT= WORK.viral_load_detect 
            DATAFILE= "C:\DATA\CSV DATASETS\Viral_load_detect.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data vl_detect(rename=(obs_datetime=detect_date));
format vl_detect $12.;
set Viral_load_detect;

if value_coded=1306 then  vl_detect='poor sample';
if value_coded=1304 then  vl_detect='undetected';
drop value_coded concept_id;
run;


proc sort data=vl_detect out=vl_detect1 dupout=vl_detect_dup nodupkey; by person_id detect_date ;run;


proc sort data=vl_detect out=viral_detect_final; by person_id detect_date;run;






PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE vl_detect vl_detect1;
		RUN;
   		QUIT;

	

%MEND create_viral_detect;
%create_viral_detect;

