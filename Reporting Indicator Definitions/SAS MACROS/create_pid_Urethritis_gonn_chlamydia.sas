


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pid_utr_gonrrh_chlamy();	

PROC IMPORT OUT= WORK.create_pid_utr_gonrrh_chlamy 
            DATAFILE= "C:\DATA\DATA SETS\problemadded_pid_utr_gonrrh_chlamy.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;





data create_pid_utr_gonrrh_chlamy1;
format app_date ddmmyy10. ;
set create_pid_utr_gonrrh_chlamy;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
/*sex_unprotected1='Yes';*/

if  value_coded=370 then Diagnosis='Urethritis';
else if value_coded=902 then Diagnosis='PID';
else if value_coded=893 then Diagnosis='Gonorrhea';
else if value_coded=6247 then Diagnosis='Chlamydia';

keep person_id app_date location_id Diagnosis  ;
run;

proc sort data=create_pid_utr_gonrrh_chlamy1 out=pid_utr_gonrrh_chlamy_final nodupkey; by person_id app_date; run;

 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE create_pid_utr_gonrrh_chlamy1 create_pid_utr_gonrrh_chlamy  ;
		RUN;
   		QUIT;


%MEND create_pid_utr_gonrrh_chlamy;
%create_pid_utr_gonrrh_chlamy;
