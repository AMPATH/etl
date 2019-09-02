


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_delivery_date();	

PROC IMPORT OUT= WORK.delivery_date_out 
            DATAFILE= "C:\DATA\DATA SETS\Delivery_date.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data Delivdate;
format  Deliv_reported_date Deliverydate date9. ;
set delivery_date_out;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
Deliv_reported_date=mdy(mm,dd,yy);

ryy=scan(value_datetime,1, '-');
rmm=scan(value_datetime,2, '-');
rdd=scan(scan(value_datetime,1, ' '),-1, '-');
Deliverydate=mdy(rmm,rdd,ryy);

keep person_id Deliv_reported_date   Deliverydate ;
run;

proc sort data=Delivdate out=Delivery_date_final nodupkey; by person_id Deliv_reported_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE Delivdate delivery_date_out ;
		RUN;
   		QUIT;


%MEND create_delivery_date;
%create_delivery_date;
