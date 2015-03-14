/*
Have been revised to introduce preffered identifier
*/
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_identifiers();


/*PROC IMPORT OUT= WORK.patient_identifier */
/*           DATAFILE= "C:\DATA\CSV DATASETS\identifier.csv" */
/*            DBMS=CSV REPLACE;*/
/*     GETNAMES=YES;*/
/*     DATAROW=2; */
/*RUN;*/

libname t 'C:\DATA\CSV DATASETS';

data identifier;
format date_crt ddmmyy10. ;
set t.identifier;
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

/* get the preffered ID*/
data identifier3i(keep=person_id location_id identifier rename=identifier=Preferred_Ampath_id);
set identifier;
/***AMPATH ID***/
*if identifier_type=3;
if preferred=1;
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

/* get those who have the ACCTG Ampath Id*/
data identifier5(keep=person_id identifier location_id rename=identifier=ACTG_id);
set identifier;
/***AMPATH ID***/
if identifier_type=27;
if voided=0;
run;
/* get those who have the National Id*/
data identifier6(keep=person_id identifier location_id rename=identifier=National_id);
set identifier;
/***AMPATH ID***/
if identifier_type=5;
if voided=0;
run;

/* get those who have the CCC Number Id*/
data identifier7(keep=person_id identifier location_id rename=identifier=CCC_No);
set identifier;
/***AMPATH ID***/
if identifier_type=28;
if voided=0;
run;



data identifier3_all;
set identifier3;
drop location_id;
run;


proc sort data=identifier3_all; by person_id; run;

proc transpose data=identifier3_all
               out=identifier3_all_transposed name=Ampath_id PREFIX=Ampath_Number;
			   by person_id;
			   var Ampath_id;
run;
data identifier3_all_final;
	set identifier3_all_transposed;
	drop Ampath_id;
run;



proc sort data=identifier3 nodupkey ; by person_id; run;
proc sort data=identifier3i nodupkey  ; by person_id; run;
proc sort data=identifier3_all_final nodupkey  ; by person_id; run;
proc sort data=identifier1 nodupkey  ; by person_id; run;
proc sort data=identifier8 nodupkey  ; by person_id; run;
proc sort data=identifier4 nodupkey  ; by person_id; run;
proc sort data=identifier5 nodupkey  ; by person_id; run;
proc sort data=identifier6 nodupkey  ; by person_id; run;
proc sort data=identifier7 nodupkey  ; by person_id; run;
/* Bring the different edentifiers together*/
data ident;
merge identifier3 identifier1 identifier8 identifier4 identifier5 identifier6  identifier3i identifier3_all_final;
by person_id;
identifier=Preferred_Ampath_id;
if identifier='' then identifier=Universal_id;
if identifier='' then identifier=Ampath_id ;
   if identifier='' then identifier=Old_ampathid;
   if identifier='' then identifier=Invalid_ampathid;
   if identifier='' then identifier=ACTG_id;
 if identifier='' then identifier=National_id;
 run;

proc sort data=ident; by person_id ; run;

proc sort data=ident nodupkey out=identifier_final1; by person_id; run;

data identifier_final;
merge identifier_final1 (in=a)identifier7;
by person_id;
if a;
run;
proc sort data=identifier_final nodupkey ; by person_id; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE dup1 dup3 ident ident1 Identifier8 ident2 identifier identifier1 identifier3 identifier4 identifier5
identifier6 identifiers patient_identifier identifier3i Identifier3_all Identifier3_all_final Identifier3_all_transposed
identifier7 identifier_final1 ;
		RUN;
   		QUIT;



%Mend create_identifiers;
%Create_identifiers;
