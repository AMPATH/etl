

PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pairs();	


libname ev 'C:\DATA\CSV DATASETS';


PROC IMPORT OUT= WORK.relationship 
            DATAFILE= "C:\DATA\CSV DATASETS\relationship.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

%Include 'C:\DATA\SAS MACROS\Create_identifiers2.sas';

data person(keep=person_id gender b_date rename=b_date=birthdate) ;
format b_date ddmmyy10.;
set ev.person;
dd=substr(birthdate,9,2);
mm=substr(birthdate,6,2);
yy=substr(birthdate,1,4);
b_date=mdy(mm,dd,yy);
if voided=0;
if person_id in(3998,4545,16584,17850,20473,36477,43362,59604,60644,95788,130084,130559,
170740,246371,249546,262981,29673,35968,311,48609,52516,52517,52518,53894,54493,59502,73806,76124,80775,
87398,87753,87754,87755,87756,88936,89364,48609,90486,90510,90527,90668,97484,97485,106185,107592,110064,
119894,120619,133790,133956,133980,139870,139881,154991,145992,147884,147886,148173,148176,148177,152885,
160857,164428,166137,176996,183275,202983,202984,202985,202986,202983,204495,209564,209569,216881,225497,
229567,229569,241077,241305,241780,251408,145991,266518,266520,266523,270463)then delete;	
run;



/***Parent***/
data parent;
format Parent_PersonID ;
set Relationship; 
if voided=0 and  relationship=2; 
/***Parent-Child Relationship***/
Parent_PersonID=Person_a;
if Person_a in(3998,4545,16584,17850,20473,36477,43362,59604,60644,95788,130084,130559,
170740,246371,249546,262981,29673,35968,311,48609,52516,52517,52518,53894,54493,59502,73806,76124,80775,
87398,87753,87754,87755,87756,88936,89364,48609,90486,90510,90527,90668,97484,97485,106185,107592,110064,
119894,120619,133790,133956,133980,139870,139881,154991,145992,147884,147886,148173,148176,148177,152885,
160857,164428,166137,176996,183275,202983,202984,202985,202986,202983,204495,209564,209569,216881,225497,
229567,229569,241077,241305,241780,251408,145991,266518,266520,266523,270463)then delete;	
keep relationship_id Parent_PersonID;
run;

proc sort data=parent; by Parent_PersonID;
data Parent_identifier(rename=(birthdate=parent_birthdate gender=parent_gender));
merge parent(in=a) person(in=b rename=person_id=Parent_PersonID);
by Parent_PersonID;
if a and b;
run;




proc sort data=Parent_identifier;by relationship_id;run;


/***Child***/
data Child;
format Child_PersonID;
set relationship;
if voided=0 and relationship=2 ;
/***Parent-Child Relationship***/
Child_PersonID=person_b;
if Person_b in(3998,4545,16584,17850,20473,36477,43362,59604,60644,95788,130084,130559,
170740,246371,249546,262981,29673,35968,311,48609,52516,52517,52518,53894,54493,59502,73806,76124,80775,
87398,87753,87754,87755,87756,88936,89364,48609,90486,90510,90527,90668,97484,97485,106185,107592,110064,
119894,120619,133790,133956,133980,139870,139881,154991,145992,147884,147886,148173,148176,148177,152885,
160857,164428,166137,176996,183275,202983,202984,202985,202986,202983,204495,209564,209569,216881,225497,
229567,229569,241077,241305,241780,251408,145991,266518,266520,266523,270463)then delete;	
keep relationship_id Child_PersonID;
run;



proc sort data=Child; by Child_PersonID;
data Child_identifier(rename=(birthdate=child_birthdate gender=child_gender));
merge child(in=a) person(in=b rename=person_id=Child_PersonID);
by Child_PersonID;
if a and b;
run;





proc sort data=Child_identifier;by relationship_id;run;

/***Parent-Child Relationship List***/

data Parent_Child_Pair(drop=relationship_id);
merge Parent_identifier(in=a) Child_identifier(in=b);
by relationship_id;
if a and b;
run;



/*** to get clean mother baby pairs***/



proc sort data=Parent_Child_Pair nodupkey dupout=duppairs out=pairs_final; by child_PersonID Parent_PersonID; run;

data ev.pair_final;
set pairs_final;
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
	
