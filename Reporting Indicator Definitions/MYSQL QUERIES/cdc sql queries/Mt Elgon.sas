Libname patients 'C:\DATA\REPORTS\NASCOP AMPATH 711and713 and Monthly REPORTS\2012\february';


PROC IMPORT OUT= WORK.mtelgon 
            DATAFILE= "C:\DATA\MYSQL QUERIES\cdc sql queries\mt.elgon1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data mtelgonFeb;
Format  app_date ddmmyy10.;
set mtelgon;
dd=substr(encounter_datetime,9,2);
mm=substr(encounter_datetime,6,2);
yy=substr(encounter_datetime,1,4);
app_date=mdy(mm,dd,yy);
drop encounter_datetime mm dd yy provider_id;
run;
proc sort data=mtelgonFeb nodupkey;by patient_id app_date;run;

data mtelgonfeb1(rename=app_date=first_appdate);
set mtelgonFeb;
by patient_id app_date;
if first.patient_id then do;
no_visits=0;
end;
no_visits=no_visits+1;
run;
/*and mdy(01,16,2012)<=app_date<=mdy(02,15,2012)*/;

data elgonfeb;
set mtelgonfeb1;
if mdy(01,16,2012)<=first_appdate<=mdy(02,15,2012) and no_visits=1;
run;
