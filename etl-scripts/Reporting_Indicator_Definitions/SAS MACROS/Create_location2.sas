
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_location();

Libname tmp 'C:\DATA\CSV DATASETS';

data location;
set tmp.location;
run;

/*
PROC IMPORT OUT= WORK.location 
            DATAFILE= "C:\DATA\CSV DATASETS\location.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/

data location1;
format site $30.;
set location;
if location_id in(2,3,4,5,6,7,8,9,10,11,12,17,18,19,20,23,28,31,55,71,72,73,79,78) then do; site=name;end;
if location_id in(1,13,14,15,77,80,56,57,58,81) then do; site='MTRH                                              '; end;
if location_id in(26,27,55) then do; site='Busia'; end;
if location_id in(63) then do; site='Amukura'; end;
if location_id in(30,82) then do; site='Kitale'; end;
if location_id in(64,75,76,74) then do; site='Webuye'; end;
if location_id in(66,67,86) then do; site='Mt. Elgon'; end;
if location_id in(24,25) then do; site='Chulaimbo'; end;
if location_id in(65) then do; site='Teso'; end;
if location_id in(68) then do; site='Kabarnet'; end;
if location_id in(70) then do; site='Mosoriot'; end;
if location_id in(59,60) then do; site='Turbo'; end;
if location_id in(61,62,54) then do; site='Burnt Forest'; end;
if location_id in(69,31) then do; site='Uasin Gishu District Hospital'; end;
if location_id in(83) then do; site='Khuyangu'; end;
if location_id in(85) then do; site='Naitiri'; end;

rename name=location;
run;

proc sort data=location1 nodupkey out=location_final; by location_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE location location1;
		RUN;
   		QUIT;


%Mend create_location;
%Create_location;
