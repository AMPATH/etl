


PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_disability();

libname eva 'C:\DATA\CSV DATASETS';


/*
PROC IMPORT OUT= WORK.disability 
            DATAFILE= "C:\DATA\CSV DATASETS\disability.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/

data dis;
set eva.disability1;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
disab_date=mdy(mm,dd,yy);
format disab_date ddmmyy10.;
keep person_id disab_date concept_id location_id value_text;
run;


proc sort data=dis nodupkey;by person_id value_text;run;


data dis_1;
set dis;
by person_id;
if last.person_id;
disab_type=UPCASE(value_text);
drop value_text;
run;


data disab_final;
set  dis_1;
by person_id;
run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE dis dis_1;
		RUN;
   		QUIT;


%Mend create_disability;
%create_disability;
