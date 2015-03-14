
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_refferals_made();


PROC IMPORT OUT= WORK.refferals_made 
            DATAFILE= "C:\DATA\CSV DATASETS\refferals_made.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data refferals(drop=value_coded dd mm yy obs_datetime);
set refferals_made;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);
format app_date date9.;
refferal=value_coded;
run;

proc sort data=refferals nodupkey out=refferals_final; by person_id app_date refferal ;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE refferals_made refferals;
		RUN;
   		QUIT;


%Mend create_refferals_made;
%Create_refferals_made;
