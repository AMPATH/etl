
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_person();

Libname eva 'C:\DATA\CSV DATASETS';


data person;
format dob date9.;
set eva.person;
yy=input(scan(birthdate,1,'-'),4.);
mm=input(scan(birthdate,2,'-'),2.);
dd=input(scan(birthdate,3,'-'),2.);
dob=mdy(mm,dd,yy);
if dob ne . or gender ne '' or death_date ne '';
keep person_id dob gender death_date ; 
run;


proc sort data=person nodupkey out=person_final; by person_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE person;
		RUN;
   		QUIT;


%Mend create_person;
%Create_person;
