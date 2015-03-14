


PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_disability();

libname dis 'C:\DATA\CSV DATASETS';

PROC IMPORT OUT= WORK.disability 
            DATAFILE= "C:\DATA\CSV DATASETS\disability.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data dis;
set disability;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
disab_date=mdy(mm,dd,yy);
format disab_date ddmmyy10.;
keep person_id disab_date concept_id location_id value_text;
run;


data dis1840;
set dis;
if concept_id=1840;
run;

proc sort data=dis1840 nodupkey; by person_id; run;


data dis1841;
set dis;
if concept_id=1841;
run;

proc sort data=dis1841 nodupkey; by person_id value_text; run;


data disab_1;
merge dis1840(in=a) dis1841(in=b);
by person_id;
if a and not b;
drop disab_date;
run;

data disab_final;
set  disab_1 dis1841;
by person_id;
*drop disab_date;
run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE dis dis1840 dis1841 disab_1;
		RUN;
   		QUIT;

/*proc freq data=disab_final;table value_text; run;*/

%Mend create_disability;
%create_disability;
