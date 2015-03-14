


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_chestXray_results();	

PROC IMPORT OUT= WORK.chest_xray
            DATAFILE= "C:\DATA\CSV DATASETS\chest_xray_results.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data chest_xray1;
Format xray_date ddmmyy10.;
set chest_xray;
if value_coded=1115 then cxray_results='normal                ';
else if value_coded=1136 then cxray_results='pulmonary effussion';
else if value_coded=1137 then cxray_results='miliary changes';
else if value_coded=5158 then cxray_results='cardiac enlargement';
else if value_coded=5622 then cxray_results='other non coded';
else if value_coded=6049 then cxray_results='infiltrate';
else if value_coded=6050 then cxray_results='non-milliary changes';
else if value_coded=6052 then cxray_results='cavitary lession';
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
xray_date=mdy(mm,dd,yy);
drop  obs_datetime dd mm yy value_coded concept_id;
run;



proc sort data=chest_xray1 nodupkey  out=chest_xrayresults_final dupout=dupxray_results; by person_id xray_date cxray_results  ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE chest_xray chest_xray1 ;
		RUN;
   		QUIT;


%MEND create_chestXray_results;
%create_chestXray_results;

