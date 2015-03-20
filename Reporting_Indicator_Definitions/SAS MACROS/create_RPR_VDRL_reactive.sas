


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_RPR_VDRL_reactive();	

Libname tmp 'C:\DATA\DATA SETS';
/*
PROC IMPORT OUT= WORK.create_RPR_VDRL_reactive 
            DATAFILE= "C:\DATA\DATA SETS\RPR_VDRL_reactive.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;*/


data create_RPR_VDRL_reactive1;
format app_date ddmmyy10. ;
set tmp.rpr_vdrl_reactive;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
RPR_reactive='Yes';

/*if  value_coded=370 then Diagnosis='Urethritis';
else if value_coded=902 then Diagnosis='PID';
else if value_coded=893 then Diagnosis='Gonorrhea';
else if value_coded=6247 then Diagnosis='Chlamydia';*/

keep person_id app_date location_id RPR_reactive  ;
run;

proc sort data=create_RPR_VDRL_reactive1 out=RPR_VDRL_reactive1_final nodupkey; by person_id app_date; run;

 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE create_RPR_VDRL_reactive1  create_RPR_VDRL_reactive ;
		RUN;
   		QUIT;


%MEND create_RPR_VDRL_reactive;
%create_RPR_VDRL_reactive;
