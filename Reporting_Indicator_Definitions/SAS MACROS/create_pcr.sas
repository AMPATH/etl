


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pcr();

PROC IMPORT OUT= WORK.pcr_elisa 
            DATAFILE= "C:\DATA\CSV DATASETS\pcr_elisa_rapid.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

	

data pcr;
Format pcr_result $15. pcrdate ddmmyy10.;
set pcr_elisa;
where concept_id in(1030);
if value_coded =1138 then do; pcr_result='Indeterminate';end;
if value_coded =1304 then do; pcr_result='Poor sample';end;
if value_coded =664 then do; pcr_result='Negative';end;
if value_coded =703 then do; pcr_result='Positive';end;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
pcrdate=mdy(mm,dd,yy);
*if pcrdate>mdy(03,01,2008);
drop value_coded dd mm yy obs_datetime concept_id obs_id;
run;


proc sort data=pcr nodupkey dupout=duppcr1 out=pcr_final; by person_id pcrdate pcr_result ; run;
proc sort data=pcr nodupkey dupout=duppcr out=pcr_final; by person_id pcrdate  ; run;

proc sort data=duppcr ; by person_id pcrdate pcr_result ; run;


data diff_pcr;
merge duppcr1(in=a) duppcr(in=b);
by person_id pcrdate pcr_result ;
if b and not a;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE duppcr duppcr1 pcr pcr1 pcr_elisa ;
		RUN;
   		QUIT;


%MEND create_pcr;

%create_pcr;
