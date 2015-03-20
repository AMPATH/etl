


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pcr_test();	


PROC IMPORT OUT= WORK.pcr_elisa_test 
            DATAFILE= "C:\DATA\CSV DATASETS\pcr_elisa_ordered.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data pcr_test;
Format  pcrdate ddmmyy10.;
set pcr_elisa_test;
where value_coded in(1030);
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
pcrdate=mdy(mm,dd,yy);
pcr_test='ordered';
drop  dd mm yy obs_datetime concept_id value_coded;
run;



proc sort data=pcr_test nodupkey  out=pcr_test_final; by person_id pcrdate ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pcr_elisa_test pcr_test;
		RUN;
   		QUIT;


%MEND create_pcr_test;
%create_pcr_test;
