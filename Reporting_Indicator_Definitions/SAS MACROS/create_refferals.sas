


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_refferals();	


PROC IMPORT OUT= WORK.refferals 
            DATAFILE= "C:\DATA\CSV DATASETS\refferels.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


proc freq data=refferals; table value_coded; run;


data refferals1;
Format refferals $30. app_date ddmmyy10.;
set refferals;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);
if value_coded=1107 then refferals='NONE                          ';
if value_coded=1167 then refferals='DISCLOSURE COUNSELLING';
if value_coded=1274 then refferals='MTRH';
if value_coded=1275 then refferals='HEALTH CENTRE';
if value_coded=1288 then refferals='ALCOHOL COUNSELLING';
if value_coded=1496 then refferals='CLINICIAN';
if value_coded=1580 then refferals='SOCIAL WORKER';
if value_coded=1581 then refferals='SUPPORT GROUP ';
if value_coded=1582 then refferals='COUNSELLING ';
if value_coded in(1587,5488) then refferals='ADHERENCE COUNSELING ';
if value_coded=1771 then refferals='OVC ';
if value_coded=1772 then refferals='XPRESS CARE ';
if value_coded=1851 then refferals='RH ';
if value_coded=1990 then refferals='KABSABET DH ';
if value_coded=1991 then refferals='NANDI HILLS DH ';
if value_coded=5483 then refferals='FAMILY PLANNING ';
if value_coded=5484 then refferals='NUTRITIONAL SUPPORT ';
if value_coded=5485 then refferals='INPATIENT ';
if value_coded=5486 then refferals='SOCIAL SUPPORT SERVICES ';
if value_coded=5487 then refferals='TB TREATMENT ';
if value_coded=5489 then refferals='MENTAL HEALTH ';
if value_coded=5490 then refferals='PHYCHOSOCIAL COUNSELLING ';
if value_coded=1771 then refferals='OTHER ';
keep person_id app_date refferals; 
run;


/*proc freq data=feeding1; tables feeding; run;*/

proc sort data=refferals1 nodupkey  out=refferals_final ; by person_id app_date refferals ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE refferals1 refferals;
		RUN;
   		QUIT;


%MEND create_refferals;
%create_refferals;
