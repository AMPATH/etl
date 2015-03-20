


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_no_show_pts();	

PROC IMPORT OUT= WORK.no_show
            DATAFILE= "C:\DATA\CSV DATASETS\no_show_patients.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data no_show1;
format app_date ddmmyy10.;
set no_show;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
if name ne '' then;
rename name=no_show;
drop concept_id dd mm yy obs_datetime ;
run;


proc sort data=no_show1 out=no_show_final nodupkey; by person_id app_date no_show; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE no_show no_show1 ;
		RUN;
   		QUIT;


%MEND create_no_show_pts;
%create_no_show_pts;
