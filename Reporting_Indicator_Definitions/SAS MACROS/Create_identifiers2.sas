
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_identifiers();


PROC IMPORT OUT= WORK.patient_identifier 
           DATAFILE= "C:\DATA\CSV DATASETS\identifier.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data identifier;
format date_crt ddmmyy10. ;
set patient_identifier;
dd=substr(date_created,9,2);
mm=substr(date_created,6,2);
yy=substr(date_created,1,4);
date_crt=mdy(mm,dd,yy);
identifier=compress(identifier);
rename patient_id=person_id;
run;

/* get those who have the new Ampath Id*/
data identifier3(keep=person_id location_id identifier rename=identifier=Ampath_id);
set identifier;
/***AMPATH ID***/
if identifier_type=3;
if voided=0;
run;


/* get those who have the universal Id*/
data identifier8(keep=person_id identifier location_id rename=identifier=Universal_id);
set identifier;
/***AMPATH ID***/
if identifier_type=8;
if voided=0;
run;

/* get those who have the old Ampath Id*/
data identifier1(keep=person_id location_id identifier rename=identifier=Old_ampathid);
set identifier;
/***AMPATH ID***/
if identifier_type=1;
if voided=0;
run;


/* get those who have the invalid Ampath Id*/
data identifier4(keep=person_id identifier location_id rename=identifier=Invalid_ampathid);
set identifier;
/***AMPATH ID***/
if identifier_type=4;
if voided=0;
run;

proc sort data=identifier3 ; by person_id; run;
proc sort data=identifier1  ; by person_id; run;
proc sort data=identifier8   ; by person_id; run;
proc sort data=identifier4  ; by person_id; run;


/* Bring the different edentifiers together*/
data ident;
merge identifier3 identifier1 identifier8 identifier4;
by person_id;
run;

proc sort data=ident; by person_id ; run;

proc sort data=ident  out=identifier_final; by person_id; run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE dup1 dup3 ident ident1 Identifier8 ident2 identifier identifier1 identifier3 identifier4 identifiers patient_identifier;
		RUN;
   		QUIT;



%Mend create_identifiers;
%Create_identifiers;
