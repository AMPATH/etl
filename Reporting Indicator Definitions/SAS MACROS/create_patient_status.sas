/*  Delivery dates*/

%Macro create_patient_status();


PROC IMPORT OUT= WORK.patient_status 
            DATAFILE= "C:\DATA\CSV DATASETS\patient_status.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data pt_status ;
format status_date ddmmyy10.;
set patient_status;
dd=substr(obs_datetime,9,2); 
mm=substr(obs_datetime,6,2); 
yy=substr(obs_datetime,1,4); 


status_date=mdy(mm,dd,yy);
drop dd mm yy obs_datetime concept_id;
run;



proc sort data=pt_status; by person_id status_date; run;

data patient_status_final;
set pt_status;  
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pt_status patient_status;
		RUN;
   		QUIT;



%Mend create_patient_status;
%Create_patient_status;
