


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_elisa_test();	


PROC IMPORT OUT= WORK.pcr_elisa_test 
            DATAFILE= "C:\DATA\CSV DATASETS\pcr_elisa_ordered.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data elisa_test;
Format  elisadate ddmmyy10.;
set pcr_elisa_test;
where value_coded in(1042);
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
elisadate=mdy(mm,dd,yy);
elisa_test='ordered';
drop  dd mm yy obs_datetime concept_id value_coded;
run;



proc sort data=elisa_test nodupkey  out=elisa_test_final; by person_id elisadate ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pcr_elisa_test elisa_test;
		RUN;
   		QUIT;


%MEND create_elisa_test;
%create_elisa_test;
