

PROC OPTIONS OPTION = MACRO; RUN;

%MACRO create_pregnant_current();	

PROC IMPORT OUT= WORK.pregnant_current 
            DATAFILE= "C:\DATA\CSV DATASETS\pregnant_current.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data preg;
format del_date pregdate ddmmyy10.;
set pregnant_current;
if value_datetime ne '';
dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
dd1=substr(obs_datetime,9,2);
mm1=substr(obs_datetime,6,2);
yy1=substr(obs_datetime,1,4);
pregdate=mdy(mm1,dd1,yy1);
del_date=mdy(mm,dd,yy);
pregnant='Y';
drop dd mm yy value_datetime dd1 mm1 yy1;
run;



data pregnant;
Format pregnant $1. pregdate ddmmyy10.;
set pregnant_current;
if value_datetime='';
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
pregdate=mdy(mm,dd,yy);
pregnant='Y';
keep person_id pregnant pregdate ;
run;


proc sort data=pregnant; by person_id pregdate; run;

proc sort data=preg; by person_id pregdate; run;


data preg1;
set pregnant preg;
by person_id;
keep person_id pregnant pregdate ;
run;


proc sort data=preg1 out=pregnant__current_final nodupkey; by person_id pregdate; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pregnant pregnant_current  preg preg1;
		RUN;
   		QUIT;



%MEND create_pregnant_current;


%create_pregnant_current;
