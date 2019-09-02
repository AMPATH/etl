


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_Non_adhrence();	

PROC IMPORT OUT= WORK.create_Non_adhrence 
            DATAFILE= "C:\DATA\DATA SETS\missed medication During last month.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data non_adhrence1;
format app_date ddmmyy10. ;
set create_Non_adhrence;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
if value_coded=1065then Non_adhrence='Yes';
keep person_id app_date location_id Non_adhrence value_coded ;
run;


proc sort data=non_adhrence1 out=non_adhrence_final /*nodupkey*/; by person_id app_date; run;

 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE non_adhrence1 create_Non_adhrence ;
		RUN;
   		QUIT;


%MEND create_Non_adhrence;
%create_Non_adhrence;
