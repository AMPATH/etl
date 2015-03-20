
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_tb_screened_pts();



PROC IMPORT OUT= tb_screened
            DATAFILE= "C:\DATA\CSV DATASETS\tb_screened.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data tb_screened1;
format screen_date ddmmyy10.;
set tb_screened;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
screen_date=mdy(mm,dd,yy);


if concept_id ne . then tb_screened='Yes  ';

drop  value_coded value_numeric  concept_id dd mm yy obs_datetime;
run;

proc sort data=tb_screened1 out=tb_screened_final nodupkey; by person_id screen_date;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE tb_screened1 tb_screened;
		RUN;
   		QUIT;


%Mend create_tb_screened_pts;
%create_tb_screened_pts;
