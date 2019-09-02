


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_RTC_date();	

PROC IMPORT OUT= WORK.RTC_out 
            DATAFILE= "C:\DATA\DATA SETS\rtc.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data rtcdate;
format app_date rtcdate date9. ;
set rtc_out;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);

ryy=scan(value_datetime,1, '-');
rmm=scan(value_datetime,2, '-');
rdd=scan(scan(value_datetime,1, ' '),-1, '-');
rtcdate=mdy(rmm,rdd,ryy);

keep person_id app_date location_id  rtcdate ;
run;

proc sort data=rtcdate out=RTC_final nodupkey; by person_id app_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE rtcdate rtc_out ;
		RUN;
   		QUIT;


%MEND create_RTC_date;
%create_RTC_date;
