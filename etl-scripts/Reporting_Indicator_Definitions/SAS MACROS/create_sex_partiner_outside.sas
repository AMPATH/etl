


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_sex_partiner_numbers();	
Libname tmp 'C:\DATA\DATA SETS';

/*PROC IMPORT OUT= WORK.create_sex_partiner_numbers
            DATAFILE= "C:\DATA\DATA SETS\sex_partiner_numbers.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN; */


data create_sex_partiner_numbers1;
format app_date ddmmyy10. ;
set tmp.sex_partiner_numbers;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
sex_partiner_numbers =value_numeric;

/*if  value_coded=370 then Diagnosis='Urethritis';
else if value_coded=902 then Diagnosis='PID';
else if value_coded=893 then Diagnosis='Gonorrhea';
else if value_coded=6247 then Diagnosis='Chlamydia';*/

keep person_id app_date location_id sex_partiner_numbers  ;
run;

proc sort data=create_sex_partiner_numbers1 out=sex_partiner_numbers_final nodupkey; by person_id app_date; run;

 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE create_sex_partiner_numbers1   ;
		RUN;
   		QUIT;


%MEND create_sex_partiner_numbers;
%create_sex_partiner_numbers;
