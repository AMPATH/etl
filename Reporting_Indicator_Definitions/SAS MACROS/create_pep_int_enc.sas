


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pep_int_enc();

PROC IMPORT OUT= WORK.pep_int_enc
            DATAFILE= "C:\DATA\CSV DATASETS\PEP_intial_enc.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

	

data pep;
Format Exposure $15.site $30.  pep_date ddmmyy10.;
informat site $30.;
set pep_int_enc;
where concept_id in(1061);
if value_coded =165 then do; Exposure='Sexual Assult     ';end;
if value_coded =7094 then do; Exposure='Occupational Exposure';end;
if value_coded in(5622,5564,1789) then do; Exposure='All other Exposure';end;

dd=substr(pepdate,9,2);
mm=substr(pepdate,6,2);
yy=substr(pepdate,1,4);
pep_date=mdy(mm,dd,yy);
if person_id =145991 then delete;

keep person_id gender Age pep_date location_id Exposure;
run;


proc sort data=pep nodupkey out=pep_final; by person_id pep_date  ; run;





PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pep pep_int_enc ;
		RUN;
   		QUIT;


%MEND create_pep_int_enc;

%create_pep_int_enc;
