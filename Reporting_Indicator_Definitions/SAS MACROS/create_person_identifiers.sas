

PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_person_identifiers();	

/*
libname m odbc user=esang password=eS@ng321 database=amrseldoret;

data tmp.identifier;
format date_crt ddmmyy10. ;
set m.patient_identifier;
date_crt=datepart(date_created);
run;

data tmp.person;
set m.person;
run;

data tmp.relationship;
set m.relationship;
run;
*/

LIBNAME TMP 'C:\DATA\REPORTS\PMTCT DNA ELISA_report';




data identifierM;
format date_crt ddmmyy10. ;
set TMP.identifier;
date_crt=datepart(date_created);
run;

data person;
set TMP.person;
run;

data relationship;
set TMP.relationship;
run;



data identifier3;
set identifierM;
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


data identifier1;
set identifierM;
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


data identifier4;
set identifierM;
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
run;



proc sort data=identifier nodupkey; by patient_id; run;



proc sort data=identifier;by patient_id descending date_created ;run;
proc sort nodupkey data=identifier;by patient_id;run;

data person;
set tmp.person;
keep person_id gender  birthdate;
run;
/***From AMRS Person Table***/
/*data person;
set tmp.person;
if voided=0;
keep person_id gender birthdate;
run;*/
proc sort nodupkey data=person;by person_id;run;

proc sql;
create table person_identifier as
select 
i.patient_id, i.identifier,i.location_id, p.gender, p.birthdate
from identifier i
join
person p
on
i.patient_id=p.person_id;
run;
quit;


%MEND create_person_identifiers;
%create_person_identifiers;
