


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_PEP_encounters();

PROC IMPORT OUT= WORK.pep_encounters
            DATAFILE= "C:\DATA\CSV DATASETS\PEP_encounters.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data pep;
Format Exposure $15.  pepdate ddmmyy10.;
set pep_encounters;
if value_coded =165 then do; Exposure='SexualAssult        ';end;
if value_coded =7094 then do; Exposure='OccExposure  ';end;
if value_coded in(5622,5564,1789) then do; Exposure='AllOthers ';end;
dd=substr(pep_date,9,2);
mm=substr(pep_date,6,2);
yy=substr(pep_date,1,4);
pepdate=mdy(mm,dd,yy);

rename patient_id=person_id;
keep patient_id gender Age pepdate location_id Exposure encounter_type;
run;

proc sort data=pep nodupkey out=pep_enc_final; by person_id pepdate  ; run;





PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pep pep_encounters ;
		RUN;
   		QUIT;


%MEND create_PEP_encounters;

%create_PEP_encounters;
