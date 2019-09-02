
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_point_of_testing();


PROC IMPORT OUT= WORK.point_of_testing1 
            DATAFILE= "C:\DATA\CSV DATASETS\point_of_testing.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data testing_point;
set point_of_testing1;
if value=1776 then testing_point='PMTCT';
if value=2047 then testing_point='VCT';
if value=2048 then testing_point='MVCT';
if value=2049 then testing_point='HCT';
if value=2050 then testing_point='MCH';
if value=2177 then testing_point='PITC';
if value=2219 then testing_point='OPD_C';
if value=5485 then testing_point='IPD';
if value=5622 then testing_point='OTHER';
if value=58 then testing_point='TB';
if testing_point ne '';
drop value;
run;

proc sort data=testing_point nodupkey out=testing_point_final ; by person_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE testing_point testing_point1;
		RUN;
   		QUIT;


%Mend create_point_of_testing;
%create_point_of_testing;
