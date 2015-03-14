


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_child_hiv_exposure();	


PROC IMPORT OUT= WORK.child_exposure
            DATAFILE= "C:\DATA\CSV DATASETS\Hiv_exposure_children.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data exposure;
Format exposure_date ddmmyy10. child_exposure;
set child_exposure;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
exposure_date=mdy(mm,dd,yy);
if value_coded=822 then do;child_exposure='exposure to HIV';end;
if value_coded=703 then do;child_exposure='Positive';end;
if value_coded=1169 then do; child_exposure='HIV infected';end;
keep person_id exposure_date child_exposure;
run;

proc sort data=exposure nodupkey out=child_exposure_final dupout=exposure_dups1; by person_id exposure_date; run;
proc sort data=exposure nodupkey out=child_exposure_final dupout=exposure_dups2; by person_id exposure_date child_exposure; run;


data exposure_dups;
merge exposure_dups1(in=a) exposure_dups2(in=b);
by person_id exposure_date ;
if a and not b;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE child_exposure exposure;
		RUN;
   		QUIT;


%MEND create_child_hiv_exposure;
%create_child_hiv_exposure;

