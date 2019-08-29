


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_chest_xray();

PROC IMPORT OUT= chest_xray 
            DATAFILE= "C:\DATA\CSV DATASETS\chest_xray_results.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data chest_xray1;
Format chestxray_result $15. chestxray_date ddmmyy10.;
set chest_xray;
if value_coded =1115 then do; chestxray_result='Normal';end;
if value_coded =1136 then do; chestxray_result='Pulmonary Effusion';end;
if value_coded =1137 then do; chestxray_result='Miliary changes';end;
if value_coded =5158 then do; chestxray_result='Cardiac Enlargement';end;
if value_coded =5622 then do; chestxray_result='Non coded';end;
if value_coded =6049 then do; chestxray_result='Infiltrate';end;
if value_coded =6050 then do; chestxray_result='Non Miliary changes';end;
if value_coded =6052 then do; chestxray_result='Cavitary Lesion';end;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
chestxray_date=mdy(mm,dd,yy);
drop value_coded dd mm yy obs_datetime concept_id ;
run;


proc sort data=chest_xray1 nodupkey dupout=dupchest_xray1 out=chest_xray_final; by person_id chestxray_date chestxray_result; run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE chest_xray chest_xray1 ;
		RUN;
   		QUIT;


%MEND create_chest_xray;

%create_chest_xray;
