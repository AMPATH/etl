


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_haemogram_ordered();

PROC IMPORT OUT= haemogram_ordered 
            DATAFILE= "C:\DATA\CSV DATASETS\haemogram_ordered.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data haemogram_ordered1;
Format haemogram_date ddmmyy10.;
set haemogram_ordered;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
haemogram_date=mdy(mm,dd,yy);
haemogram_ordered=1;
drop value_coded dd mm yy obs_datetime concept_id ;
run;


proc sort data=haemogram_ordered1 nodupkey dupout=duphaemogram_ordered1 out=haemogram_ordered_final; 
by person_id haemogram_date; run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE haemogram_ordered  haemogram_ordered1 ;
		RUN;
   		QUIT;


%MEND create_haemogram_ordered;

%create_haemogram_ordered;
