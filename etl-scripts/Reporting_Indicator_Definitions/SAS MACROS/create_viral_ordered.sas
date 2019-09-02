


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_viral_ordered();	


PROC IMPORT OUT= WORK.viral_ordered 
            DATAFILE= "C:\DATA\CSV DATASETS\Viral_load_ordered.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data viral_load1;
Format viraldate ddmmyy10.;
set viral_ordered;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
viraldate=mdy(mm,dd,yy);
viral_test='ordered';
drop value_coded dd mm yy obs_datetime concept_id;
run;



proc sort data=viral_load1 nodupkey  out=viral_ordered_final dupout=viral_order_dup; by person_id viraldate; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE viral_load viral_load1 viral_ordered;
		RUN;
   		QUIT;


%MEND create_viral_ordered;
%create_viral_ordered;
