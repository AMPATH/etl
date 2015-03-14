


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_travel_time();	

PROC IMPORT OUT= WORK.create_travel_time 
            DATAFILE= "C:\DATA\DATA SETS\Travel_time.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data travell_T;
format app_date ddmmyy10. ;
set create_travel_time;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
create_travel_time=1;
keep person_id app_date location_id create_travel_time value_coded ;
run;
data travellcode;
set travell_T;
if  value_coded=1049 then travel_time='LESS THAN 30 MINUTES';
else if value_coded=1050 then travel_time='30 TO 60 MINUTES';
else if value_coded=1051 then travel_time='ONE TO TWO HOURS';
else if value_coded=1052 then travel_time='MORE THAN TWO HOURS';
else if value_coded=6412 then travel_time='TWO TO FOUR HOURS';
else if value_coded=6413 then travel_time='FOUR TO EIGHT HOURS';
else if value_coded=6414 then travel_time='MORE THAN EIGHT HOURS';
run;
proc sort data=travellcode out=Travetine_final /*nodupkey*/; by person_id app_date; run;

 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE travellcode travell_T create_travel_time;
		RUN;
   		QUIT;


%MEND create_travel_time;
%create_travel_time;
