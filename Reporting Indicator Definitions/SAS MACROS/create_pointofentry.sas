


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pointofentry();


Libname eva 'C:\DATA\CSV DATASETS';
/*
PROC IMPORT OUT= WORK.point 
            DATAFILE= "C:\DATA\CSV DATASETS\point_of_entry.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/

data point;
set eva.point_of_entry;
if PointOfContactName in('ADULT INPATIENT SERVICE','PEDIATRIC INPATIENT SERVICE')then PointOfContactName='INPATIENT CARE OR HOSPITALIZATION';
if PointOfContactName in('PERPETUAL HOME-BASED COUNSELING AND TESTING')then PointOfContactName='HOME BASED TESTING PROGRAM';
run;

/* convert the person_id for those on ARV to be as though they are for the parents...
this is to help us merge this with the parent_baby pairs*/


proc sort nodupkey data=point out=pointofentry_final; by person_id;run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE point ;
		RUN;
   		QUIT;

	

%MEND create_pointofentry;

%create_pointofentry;
