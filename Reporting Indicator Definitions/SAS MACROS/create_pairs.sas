

PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pairs();	


libname ev 'C:\DATA\CSV DATASETS';

PROC IMPORT OUT=work.patient_identifier 
            DATAFILE= "C:\DATA\CSV DATASETS\identifier.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;




PROC IMPORT OUT= WORK.relationship 
            DATAFILE= "C:\DATA\CSV DATASETS\relationship.csv" 
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


data relationship;
set relationship;
run;


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
run;



proc sort data=identifier nodupkey; by patient_id; run;



proc sort data=identifier;by patient_id descending date_created ;run;
proc sort nodupkey data=identifier;by patient_id;run;

data person;
format b_date ddmmyy10.;
set ev.person;
dd=substr(birthdate,9,2);
mm=substr(birthdate,6,2);
yy=substr(birthdate,1,4);
b_date=mdy(mm,dd,yy);
if voided=0;
drop birthdate;
rename b_date=birthdate;
keep person_id gender  birthdate b_date;
run;

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

/***Parent***/
data parent;
format Parent_PersonID ;
set Relationship; 
if voided=0 and  relationship=2; 
/***Parent-Child Relationship***/
Parent_PersonID=Person_a;
keep relationship_id Parent_PersonID;
run;

proc sql;
create table Parent_identifier as
select
p.relationship_id, p.Parent_PersonID, pi.identifier as Parent_AMPATHID, pi.gender as Parent_Gender, 
pi.birthdate as Parent_birthdate, pi.location_id as location_id1
from parent p
left join
person_identifier pi
on 
p.Parent_PersonID=pi.patient_id;
run;
quit;
proc sort data=Parent_identifier;by relationship_id;run;


/***Child***/
data Child;
format Child_PersonID;
set relationship;
if voided=0 and relationship=2 ;
/***Parent-Child Relationship***/
Child_PersonID=person_b;
keep relationship_id Child_PersonID;
run;

proc sql;
create table Child_identifier as
select
c.relationship_id, c.Child_PersonID, pi.identifier as Child_AMPATHID, pi.gender as Child_Gender,
pi.birthdate as Child_Birthdate, pi.location_id as location_id
from child c
left join
person_identifier pi
on 
c.child_PersonID=pi.patient_id;
run;
quit;

proc sort data=Child_identifier;by relationship_id;run;

/***Parent-Child Relationship List***/
proc sql;
create table Parent_Child_Pair as
select 
pi.*, ci.Child_personID, ci.Child_AMPATHID, ci.Child_Gender, ci.Child_Birthdate,ci.location_id
from 
Parent_Identifier pi
join
Child_identifier ci
on
pi.relationship_id=ci.relationship_id;
run;
quit;
run;

data questionpairs;
set parent_child_pair;
if Parent_birthdate ne . and child_birthdate ne . and parent_birthdate > child_birthdate or child_ampathid='1500MT-0' 
OR Parent_birthdate>MDY(01,01,2003);
run;

/***Missing DOB
data missing_dob;
set parent_child_pair;
if (parent_birthdate=. and Parent_AMPATHID ne '') or (child_birthdate =. and Child_AMPATHID ne '');
run;

data missing_dob1;
set parent_child_pair;
if parent_birthdate=. or child_birthdate =. or Parent_AMPATHID ne '' or Child_AMPATHID ne '';
run;


/*** to get clean mother baby pairs***/

proc sort data=parent_child_pair; by Child_AMPATHID; run;

proc sort data=questionpairs; by Child_AMPATHID; run;

data Pairs_baby;
merge parent_child_pair(in=a) questionpairs(in=b); 
by Child_AMPATHID;
if a and not b;
if parent_gender='F' and child_ampathid ne '';
keep Child_AMPATHID child_PersonID  child_Gender child_birthdate 
Parent_AMPATHID Parent_PersonID Parent_Gender parent_birthdate location_id ;
run;


proc sort data=pairs_baby nodupkey dupout=duppairs out=pairs_final1; by child_PersonID Parent_PersonID; run;

proc sort data=pairs_final1 nodupkey dupout=dupchild out=pairs_final2; by child_PersonID; run;


data pair_final;
merge pairs_final2(in=a) dupchild(in=b);
if a and not b;
run; 


data ev.pair_final;
set pair_final;
run;



/*


ods rtf;
proc print data=dupchild; var Child_AMPATHID child_birthdate ; run;
ods rtf close;


ods rtf;
proc print data=duppairs; var Child_AMPATHID child_birthdate Parent_AMPATHID parent_birthdate; run;
proc print data=questionpairs; var Child_AMPATHID child_birthdate Parent_AMPATHID parent_birthdate; run;
ods rtf close;
*/



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE CHILD dup1 dup3 duppairs ident ident1 ident2 identifier identifier1 
identifier3 identifier4 missing_dob pairs_baby parent Parent_child_pair Parent_identifier
Patient_identifier Person Person_identifier Questionpairs Relationship Child_identifier dupchild  
pairs_final1 pairs_final2;
		RUN;
   		QUIT;


%MEND create_pairs;
%create_pairs;
	
