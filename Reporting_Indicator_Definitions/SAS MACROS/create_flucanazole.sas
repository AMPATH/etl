


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_cotrimoxazole();	

PROC IMPORT OUT= WORK.fluco
            DATAFILE= "C:\DATA\CSV DATASETS\Fluconazole.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data fluco1;
format fluco_date ddmmyy10.;
set fluco;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
fluco_date=mdy(mm,dd,yy);

drop concept_id dd mm yy obs_datetime value_numeric  ;
run;




proc sort data=fluco1 out=fluco_final nodupkey; by person_id fluco_date ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE  fluco  fluco1  ;
		RUN;
   		QUIT;


%MEND create_cotrimoxazole;
%create_cotrimoxazole;
