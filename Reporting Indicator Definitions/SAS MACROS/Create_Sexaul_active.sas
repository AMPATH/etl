


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_sexual_active_6month();	
Libname tmp 'C:\DATA\DATA SETS';

/*PROC IMPORT OUT= WORK.create_sexual_active_6month 
            DATAFILE= "C:\DATA\DATA SETS\sexual_active_6month.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;*/


data create_sexual_active_6month1;
format app_date ddmmyy10. ;
set tmp.sexual_active_6month;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
Sexaul_active='Yes';

/*if  value_coded=370 then Diagnosis='Urethritis';
else if value_coded=902 then Diagnosis='PID';
else if value_coded=893 then Diagnosis='Gonorrhea';
else if value_coded=6247 then Diagnosis='Chlamydia';*/

keep person_id app_date location_id Sexaul_active  ;
run;

proc sort data=create_sexual_active_6month1 out=sexual_active_6month_final nodupkey; by person_id app_date; run;

 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE create_sexual_active_6month1 create_sexual_active_6month  ;
		RUN;
   		QUIT;


%MEND create_sexual_active_6month;
%create_sexual_active_6month;
