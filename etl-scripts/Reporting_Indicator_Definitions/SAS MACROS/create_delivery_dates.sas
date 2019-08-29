/*  Delivery dates*/

%Macro create_delivery_dates();


PROC IMPORT OUT= WORK.delivery_dates 
            DATAFILE= "C:\DATA\CSV DATASETS\delivery_dates.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data delivery ;
format delivery_date  encounter_date ddmmyy10.;
set delivery_dates;
dd=substr(value_datetime,9,2); 
mm=substr(value_datetime,6,2); 
yy=substr(value_datetime,1,4); 

dd1=substr(obs_datetime,9,2); 
mm1=substr(obs_datetime,6,2); 
yy1=substr(obs_datetime,1,4); 

delivery_date=mdy(mm,dd,yy);
encounter_date=mdy(mm1,dd1,yy1);
drop dd mm yy dd1 mm1 yy1 obs_datetime value_datetime concept_id;
run;



proc sort data=delivery; by person_id encounter_date; run;

data delivery_dates_final;
set delivery;  
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE delivery delivery_dates;
		RUN;
   		QUIT;



%Mend create_delivery_dates;
%Create_delivery_dates;
