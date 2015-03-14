


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_viral_load();	


PROC IMPORT OUT= WORK.viral_load 
            DATAFILE= "C:\DATA\CSV DATASETS\viral_load.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data viral_load1;
Format viral_count 15. viraldate ddmmyy10.;
set viral_load;
viral_count=value_numeric;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
viraldate=mdy(mm,dd,yy);
drop value_numeric dd mm yy obs_datetime concept_id;
run;



proc sort data=viral_load1 nodupkey  out=viral_count_final dupout=viral_dup; by person_id viraldate viral_count; run;
proc sort data=viral_load1 nodupkey  out=viral_count_final dupout=viral_dup1; by person_id viraldate ; run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE viral_load viral_load1 viral_dup viral_dup1;
		RUN;
   		QUIT;


%MEND create_viral_load;
%create_viral_load;
