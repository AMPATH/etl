


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_sex_unprotected();	

PROC IMPORT OUT= WORK.create_sex_unprotected 
            DATAFILE= "C:\DATA\DATA SETS\sex_unprotected.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data sex_unprotected1;
format app_date ddmmyy10. ;
set create_sex_unprotected;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);


if  value_coded=1422 then Condom_use='Yes Sometime';
else if value_coded=1066 then Condom_use='No';

if Condom_use in ('Yes Sometime','No') then sex_unprotected1='Yes';
keep person_id app_date location_id sex_unprotected1 Condom_use  ;
run;

proc sort data=sex_unprotected1 out=sex_unprotected_final nodupkey; by person_id app_date; run;

 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE sex_unprotected1 create_sex_unprotected  ;
		RUN;
   		QUIT;


%MEND create_sex_unprotected;
%create_sex_unprotected;
