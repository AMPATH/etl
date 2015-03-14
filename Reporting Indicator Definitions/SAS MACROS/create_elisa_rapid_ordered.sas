


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_rapid_elisa_ordered();	


PROC IMPORT OUT= WORK.rapid_elisa_test 
            DATAFILE= "C:\DATA\CSV DATASETS\rapid_elisa_ordered.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data rapid_elisa_ordered;
Format  elisadate ddmmyy10.;
set rapid_elisa_test
;
where value_coded in(1042);
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
elisadate=mdy(mm,dd,yy);
elisa_test='ordered';
drop  dd mm yy obs_datetime concept_id value_coded;
run;



proc sort data=rapid_elisa_ordered nodupkey  out=elisarap_ordered_final; by person_id elisadate ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pcr_elisa_test elisa_test;
		RUN;
   		QUIT;


%MEND create_rapid_elisa_ordered;
%create_rapid_elisa_ordered;
