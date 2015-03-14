


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_return_date();

/*
PROC IMPORT OUT= WORK.Return_visit_date 
            DATAFILE= "C:\DATA\CSV DATASETS\Return_visit_date.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/

Libname rtc 'C:\DATA\CSV DATASETS';

/*get all RTCS as indicated on the encounter forms*/
data return_date1(where=(return_date ne .));
Format app_date return_date ddmmyy10.;
set rtc.Return_visit_date;
if value_datetime ne '';
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
dd1=substr(value_datetime,9,2);
mm1=substr(value_datetime,6,2);
yy1=substr(value_datetime,1,4);
app_date=mdy(mm,dd,yy);
return_date=mdy(mm1,dd1,yy1);
*drop dd mm yy mm1 dd1 yy1 value_datetime obs_datetime concept_id value_numeric obs_group_id obs_id ;
run;

/* Get estimated RTCs as per weeks, days or months*/

data rtcsets;
Format app_date  ddmmyy10.;
set rtc.Return_visit_date;
if concept_id=1922;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);
keep person_id app_date obs_id encounter_id;
run;


data rtcset1;
set rtc.Return_visit_date;
if concept_id in(1892,1893,1894);
if  concept_id=1892 then rtc_period='days   ';
else if concept_id =1893 then rtc_period='weeks';
else if concept_id=1894 then rtc_period='months';
keep person_id rtc_period obs_group_id value_numeric;
run;


proc sort data=rtcsets; by person_id obs_id;run;
proc sort data=rtcset1; by person_id obs_group_id;run;
data rtc(drop=obs_id);
merge rtcsets(in=a) rtcset1(rename=(obs_group_id=obs_id));
by person_id obs_id;
if a;
run;


/* merge the estimated RTC to actual return visit date*/

proc sort data=rtc; by person_id app_date encounter_id;
proc sort data=return_date1; by person_id app_date encounter_id;

data  rtc_all (drop=rtc_period value_numeric);
format estimated_rtc ddmmyy10.;
merge return_date1(in=a) rtc(in=b);
by person_id app_date;
if a or b;
if rtc_period='weeks' then estimated_rtc=app_date+(value_numeric*7);
else if rtc_period='months' then estimated_rtc=app_date+(value_numeric*30.25);
else if rtc_period='days' then estimated_rtc=app_date+value_numeric;
run;

data rtc_all1(keep=person_id app_date return_date rtc_estimated encounter_id);
set rtc_all;
if return_date  ne . then do; rtc_estimated = 'No'; end;
if return_date  eq . then do; rtc_estimated = 'Yes'; return_date=estimated_rtc; end;
run;

proc sort data=rtc_all1 out=return_date_final nodupkey; by person_id app_date encounter_id ;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE rtc_all return_date1 rtc Rtcset1 Rtcset Return_visit_date Rtcsets rtc_all1;
		RUN;
   		QUIT;


%MEND create_return_date;

%create_return_date;
