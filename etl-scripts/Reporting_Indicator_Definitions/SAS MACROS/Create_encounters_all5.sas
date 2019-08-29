


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_return_date();



Libname rtc 'C:\DATA\CSV DATASETS';


PROC IMPORT OUT= WORK.encounters1 
            DATAFILE= "C:\DATA\CSV DATASETS\encountersall.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data Return_visit_date;
set rtc.Return_visit_date;
run;
	
/*get all RTCS as indicated on the encounter forms*/
data return_date1(where=(return_date ne .));
Format app_date return_date ddmmyy10.;
set Return_visit_date;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
dd1=substr(value_datetime,9,2);
mm1=substr(value_datetime,6,2);
yy1=substr(value_datetime,1,4);
app_date=mdy(mm,dd,yy);
return_date=mdy(mm1,dd1,yy1);
drop dd mm yy mm1 dd1 yy1 value_datetime obs_datetime concept_id value_numeric obs_group_id obs_id ;
run;

/* Get estimated RTCs as per weeks, days or months*/

data rtcsets;
Format app_date ddmmyy10.;
set Return_visit_date;
where concept_id=1922;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
dd1=substr(value_datetime,9,2);
mm1=substr(value_datetime,6,2);
yy1=substr(value_datetime,1,4);
app_date=mdy(mm,dd,yy);
keep person_id app_date obs_id encounter_id;
run;


data rtcset1;
set Return_visit_date;
where concept_id in(1892,1893,1894);
if  concept_id=1892 then rtc_period='days   ';
else if concept_id =1893 then rtc_period='weeks';
else if concept_id=1894 then rtc_period='months';
obs_group_id1=obs_group_id*1;
keep person_id rtc_period obs_group_id1 value_numeric;
run;


proc sort data=rtcsets; by person_id obs_id;run;
proc sort data=rtcset1; by person_id obs_group_id1;run;
data rtc(drop=obs_id);
merge rtcsets(in=a) rtcset1(rename=(obs_group_id1=obs_id));
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

data rtc_all1(drop=estimated_rtc encounter_id);
set rtc_all;
if return_date  eq . then do; rtc_estimated = 'Yes'; end;
if return_date  ne . then do; rtc_estimated = 'No'; end;
if return_date eq . then return_date=estimated_rtc;
encounter_id1=encounter_id*1;
run;

proc sort data=rtc_all1 out=return_date_final nodupkey; by person_id app_date encounter_id1 ;
run;
proc sort data=return_date_final(rename=encounter_id1=encounter_id); by  encounter_id; run;

proc sort data=encounters1(rename=(patient_id=person_id)); by  encounter_id; run;

data enc_rtc(rename=return_date=RTCdate);
merge encounters1(in=a) return_date_final(in=b);
if encounter_type in(1,2,14) then ptype='adult ';
else if encounter_type in(3,4,15) then ptype='paeds ';
by encounter_id;
if a ;
run;

data enc_rtc1(rename=(app_date=rtcapp_date));
format encounter_date   ddmmyy10.;
set enc_rtc;
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
encounter_date=mdy(mm,dd,yy);
if rtc_estimated = '' and app_date=.  then do;
app_date=encounter_date;
RTCdate=(encounter_date+21);
rtc_estimated='Yes';
end;
drop dd yy mm encounter_datetime ;
run;
*then app_date=datepart(encounter_datetime);





proc sort data=enc_rtc1 out=encounters_final nodupkey; by person_id rtcapp_date ;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE rtc_all return_date1 rtc Rtcset1 Rtcset Return_visit_date Rtcsets rtc_all1 enc_rtc1;
		RUN;
   		QUIT;


%MEND create_return_date;

%create_return_date;
