
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_encounters();


PROC IMPORT OUT= WORK.encounters1 
            DATAFILE= "C:\DATA\DATA SETS\encountersall.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.rtc
            DATAFILE= "C:\DATA\DATA SETS\rtc_appdate.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


PROC IMPORT OUT= WORK.rtc_2
            DATAFILE= "C:\DATA\DATA SETS\rtc_appdate_2.csv" 
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
proc sort data=rtc; by  obs_group_id; run;
proc sort data=rtc_2(rename=(obs_id=obs_group_id)); by  obs_group_id ; run;

data rtcobs;
merge rtc (in=a) rtc_2 (in=b);
 by  obs_group_id;
 if a ;
 if concept_id in (1893,1892,1894) and obs_group_id=. then delete;
*drop obs_group_id;
 run;

proc sort data=encounters1(rename=(patient_id=person_id)); by  encounter_id; run;
proc sort data=rtcobs; by  encounter_id; run;

data enc_rtc2;
merge encounters1(in=a) rtcobs(in=b);
by encounter_id;
if a ;
run;


data encounters1a;
format encounter_date rtc_date  rtcapp_date ddmmyy10.;
set enc_rtc2;
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
encounter_date=mdy(mm,dd,yy);
dd2=substr(value_datetime,9,2); 
mm2=substr(value_datetime,6,2); 
yy2=substr(value_datetime,1,4); 
rtc_date=mdy(mm2,dd2,yy2);
dd1=substr(obs_datetime,9,2); 
mm1=substr(obs_datetime,6,2); 
yy1=substr(obs_datetime,1,4); 
rtcapp_date=mdy(mm1,dd1,yy1);
if value_numeric =. then tagged='dated     '; else  tagged='yes';
if encounter_type in(1,2,14) then ptype='adult ';
else if encounter_type in(3,4,15) then ptype='paeds ';
drop dd mm yy value_datetime dd1 mm1 yy1 dd2 mm2 yy2 obs_datetime  encounter_datetime ;
run;





 
data rtc1;
set encounters1a;
format RTCest    ddmmyy10.;
if concept_id =1892 then duration=int(value_numeric);
if concept_id =1893 then duration=int(value_numeric*7);
if concept_id =1894 then duration=int(value_numeric*30.45);
if tagged='yes' then RTCEst =int(rtcapp_date+duration);
if tagged='dated' then  RTCest =rtc_date;
if duration =. and rtc_date=. then RTCest=int(encounter_date+21);
if ( tagged='dated' and  rtc_date <=encounter_date)  then RTCest=int(encounter_date+21);
run;

proc sort nodupkey  data=rtc1 ; by  encounter_id rtcapp_date; run;
 
data encounters_final (rename=(rtcest=RTCdate));
set rtc1;
drop obs_group_id value_numeric duration rtc_date tagged;
run; 





PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE encounters1  encounters encounters1a rtc rtc1 enc_rtc2 rtc_2 rtcobs ;
		RUN;
   		QUIT;



%Mend create_encounters;
%Create_encounters;
