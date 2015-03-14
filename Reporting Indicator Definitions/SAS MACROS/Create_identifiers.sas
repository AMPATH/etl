
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
run;

/* get those who have the new Ampath Id*/
data identifier3;
set identifier;
/***AMPATH ID***/
if identifier_type=3;
if voided=0;
keep patient_id identifier date_crt identifier_type preferred location_id;
run;

proc sort data=identifier3; by patient_id preferred; run;


data identifier3;
set identifier3;
by patient_id preferred;
if last.patient_id;
run;


/* get those who have the old Ampath Id*/
data identifier1;
set identifier;
/***AMPATH ID***/
if identifier_type=1;
if voided=0;
keep patient_id identifier date_crt identifier_type preferred location_id;
run;


proc sort data=identifier1; by patient_id date_crt; run;


data identifier1;
set identifier1;
by patient_id date_crt;
if last.patient_id;
run;


/* get those who have the invalid Ampath Id*/
data identifier4;
set identifier;
/***AMPATH ID***/
if identifier_type=4;
if voided=0;
keep patient_id identifier date_created identifier_type preferred location_id;
run;


proc sort data=identifier3 nodupkey out=dup3; by patient_id; run;
proc sort data=identifier1 nodupkey out=dup1 ; by patient_id; run;


data ident;
merge identifier3(in=a) identifier1(in=b);
by patient_id;
if b and not a;
run;

proc sort data=ident; by patient_id preferred; run;


data ident;
set ident;
by patient_id preferred;
if last.patient_id;
run;


data ident1;
set identifier3 ident;
by patient_id;
run;


proc sort data=ident1 nodupkey; by patient_id; run;
proc sort data=identifier4 nodupkey; by patient_id; run;



data ident2;
merge ident1(in=a) identifier4(in=b);
by patient_id;
if b and not a;
run;


proc sort data=ident1 nodupkey; by patient_id; run;



data identifier;
set ident2 ident1;
by patient_id;
rename patient_id=person_id;
drop date_crt identifier_type preferred date_created;
run;



proc sort data=identifier nodupkey out=identifier_final; by person_id; run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE dup1 dup3 ident ident1 ident2 identifier identifier1 identifier3 identifier4 identifiers patient_identifier;
		RUN;
   		QUIT;



%Mend create_identifiers;
%Create_identifiers;
