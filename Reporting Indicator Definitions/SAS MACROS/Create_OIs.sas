
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_OIs();


PROC IMPORT OUT= WORK.OIs 
            DATAFILE= "C:\DATA\CSV DATASETS\OIs.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data oi1;
format oi_date ddmmyy10.;
set OIs;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
oi_date=mdy(mm,dd,yy);
OI=1;
keep person_id value_coded OI oi_date ;
run;

proc sort data=oi1 out=oi_final nodupkey; by person_id oi_date value_coded;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE ois oi1;
		RUN;
   		QUIT;


%Mend create_OIs;
%Create_OIs;
