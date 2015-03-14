


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_missed_pills();	

PROC IMPORT OUT= WORK.create_missed_pills 
            DATAFILE= "C:\DATA\DATA SETS\missed_pills.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data missed_pills1;
format app_date ddmmyy10. ;
set create_missed_pills;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
if  value_coded in(1107 ,1160,1161,1162) then missed_pills='Yes';
keep person_id app_date location_id missed_pills value_coded ;
run;
/*data travellcode;
set travell_T;
if  value_coded=1049 then travel_time='LESS THAN 30 MINUTES';
else if value_coded=1050 then travel_time='30 TO 60 MINUTES';
else if value_coded=1051 then travel_time='ONE TO TWO HOURS';
else if value_coded=1052 then travel_time='MORE THAN TWO HOURS';
else if value_coded=6412 then travel_time='TWO TO FOUR HOURS';
else if value_coded=6413 then travel_time='FOUR TO EIGHT HOURS';
else if value_coded=6414 then travel_time='MORE THAN EIGHT HOURS';
run;/*/
proc sort data=missed_pills1 out=missed_pills1_final nodupkey; by person_id app_date; run;

 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE missed_pills1 create_missed_pills ;
		RUN;
   		QUIT;


%MEND create_missed_pills;
%create_missed_pills;
