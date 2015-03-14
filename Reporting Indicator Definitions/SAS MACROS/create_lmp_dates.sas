


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_lmp_dates();	

PROC IMPORT OUT= WORK.lmp_dates 
            DATAFILE= "C:\DATA\CSV DATASETS\lmp_dates.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;




/*---Last LMP Dates---*/
data lmp_dates1;
format pregdate lmp_pregdate lmp_pregdate1 ddmmyy10.;
set lmp_dates;
if concept_id=1836;
dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
dd1=substr(obs_datetime,9,2);
mm1=substr(obs_datetime,6,2);
yy1=substr(obs_datetime,1,4);
pregdate=mdy(mm1,dd1,yy1);
lmp_pregdate1=mdy(mm,dd,yy);
lmp_pregdate=(lmp_pregdate1+(280+7));
drop dd mm yy value_datetime dd1 mm1 yy1 obs_datetime concept_id lmp_pregdate1;
run;

proc sort data=lmp_dates1;by person_id pregdate;run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE lmp_dates ;
		RUN;
   		QUIT;


%MEND create_lmp_dates;

%create_lmp_dates;
