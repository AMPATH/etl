
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_location();


PROC IMPORT OUT= WORK.location 
            DATAFILE= "C:\DATA\DATA SETS\location.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data location1;
set location;

/*Ampath sites*/

if location_id in(2,3,4,5,6,7,8,9,10,11,12,17,18,19,20,23,28,31,55,71,72,73,79,78,87) then do; site=name;end;
if location_id in(1,13,14,15,77,80,56,58) then do; site='MTRH'; end;
if location_id in(26,27,81) then do; site='Busia'; end;
if location_id in(63,92,93) then do; site='Amukura'; end;
if location_id in(30,82,57,88) then do; site='Kitale'; end;
if location_id in(64,75,76,74) then do; site='Webuye'; end;
if location_id in(66,67,86,89) then do; site='Mt. Elgon'; end;
if location_id in(24,25) then do; site='Chulaimbo'; end;
if location_id in(65,90,91) then do; site='Teso'; end;
if location_id in(68) then do; site='Kabarnet'; end;
if location_id in(83) then do; site='Khuyangu'; end;
if location_id in(70) then do; site='Mosoriot'; end;
if location_id in(59,60) then do; site='Turbo'; end;
if location_id in(85) then do; site='Naitiri'; end;
if location_id in(61,62,54) then do; site='Burnt Forest'; end;
if location_id in(69) then do; site='Uasin Gishu District Hospital'; end;

/*health centres*/

if location_id in(1,13,14,15) then do; HealthCentre='MTRH          ';end;
else if location_id in(19,26,27) then do; HealthCentre='BUSIA          ';end;
else if location_id in(7,24,25) then do; HealthCentre='CHULAIMBO          ';end;
else if location_id in(2) then do; HealthCentre='MOSORIOT';end;
else if location_id in(3) then do; HealthCentre='TURBO';end;
else if location_id in(4) then do; HealthCentre='BURNT FOREST';end;
else if location_id in(5) then do; HealthCentre='AMUKURA';end;
else if location_id in(6) then do; HealthCentre='NAITIRI';end;
else if location_id in(8) then do; HealthCentre='WEBUYE';end;
else if location_id in(9) then do; HealthCentre='MT. ELGON';end;
else if location_id in(10) then do; HealthCentre='KAPENGURIA';end;
else if location_id in(11) then do; HealthCentre='KITALE';end;
else if location_id in(12) then do; HealthCentre='TESO';end;
else if location_id in(17) then do; HealthCentre='ITEN';end;
else if location_id in(18) then do; HealthCentre='KABARNET';end;
else if location_id in(20) then do; HealthCentre='PORT VICTORIA';end;
else if location_id in(23) then do; HealthCentre='KHUNYANGU';end;
else if location_id in(28) then do; HealthCentre='ZIWA';end;
else if location_id in(31) then do; HealthCentre='UASIN GISHU DH';end;
else if location_id in(54) then do; HealthCentre='PLATEAU MISSION ';end;
else if location_id in(55) then do; HealthCentre='BUMALA A';end;
else if location_id in(56) then do; HealthCentre='ELDORET PRISON';end;
else if location_id in(57) then do; HealthCentre='KITALE PRISON';end;
else if location_id in(58) then do; HealthCentre='NGERIA PRISON';end;
else if location_id in(59) then do; HealthCentre='MAUTUMA';end;
else if location_id in(60) then do; HealthCentre='CHEPSAITA';end;
else if location_id in(61) then do; HealthCentre='KAPTAGAT';end;
else if location_id in(62) then do; HealthCentre='KESSES';end;
else if location_id in(63) then do; HealthCentre='LUKOLIS';end;
else if location_id in(64) then do; HealthCentre='BOKOLI';end;
else if location_id in(65) then do; HealthCentre='ANGURAI';end;
else if location_id in(66) then do; HealthCentre='CHEPTAIS';end;
else if location_id in(67) then do; HealthCentre='CHESKAKI';end;
else if location_id in(68) then do; HealthCentre='MARIGAT';end;
else if location_id in(69) then do; HealthCentre='HURUMA SDH';end;
else if location_id in(70) then do; HealthCentre='PIONEER SDH';end;
else if location_id in(71) then do; HealthCentre='MOIS BRIDGE';end;
else if location_id in(72) then do; HealthCentre='MOI UNIVERSITY';end;
else if location_id in(73) then do; HealthCentre='SOY';end;
else if location_id in(74) then do; HealthCentre='MIHUU';end;
else if location_id in(75) then do; HealthCentre='SINOKO';end;
else if location_id in(76) then do; HealthCentre='MILO';end;
else if location_id in(77) then do; HealthCentre='MOIBEN';end;
else if location_id in(78) then do; HealthCentre='MUKHOBOLA';end;
else if location_id in(79) then do; HealthCentre='NAMBALE';end;
else if location_id in(80) then do; HealthCentre='MOI BARRACKS';end;
else if location_id in(81) then do; HealthCentre='BUSIA PRISON';end;
else if location_id in(82) then do; HealthCentre='SABOTI';end;
else if location_id in(83) then do; HealthCentre='BUMALA B';end;
else if location_id in(84) then do; HealthCentre='MTRH MAIN HOSPITAL';end;
else if location_id in(85) then do; HealthCentre='MAKUTANO';end;
else if location_id in(86) then do; HealthCentre='KAPTAMA (FRIENDS) DISP';end;
else if location_id in(87) then do; HealthCentre='SIO PORT';end;
else if location_id in(88) then do; HealthCentre='TULWET';end;
else if location_id in(89) then do; HealthCentre='KOPSIRO';end;
else if location_id in(90) then do; HealthCentre='CHANGARA';end;
else if location_id in(91) then do; HealthCentre='MALABA';end;
else if location_id in(92) then do; HealthCentre='AMASE';end;
else if location_id in(93) then do; HealthCentre='OBEKAYE';end;


else if location_id not in(31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53) then do; HealthCentre=name;end;

/* district */

if location_id in(1,13,14,56,15,4,31,54,84) then do; District='ELDORET EAST';end;
else if location_id in(77,3,60,69,71,73,28,80) then do; District='ELDORET WEST';end;
else if location_id in(58,70,62,72) then do; District='WARENG';end;
else if location_id in(2) then do; District='NANDI NORTH';end;
else if location_id in(10) then do; District='WEST POKOT';end;
else if location_id in(59) then do; District='LUGARI';end;
else if location_id in(5,63,92,93) then do; District='TESO SOUTH';end;
else if location_id in(6,85) then do; District='BUNGOMA NORTH';end;
else if location_id in(7,24,25) then do; District='KISUMU WEST';end;
else if location_id in(8,64,75,76,74) then do; District='BUNGOMA EAST';end;
else if location_id in(12,65,90,91) then do; District='TESO NORTH';end;
else if location_id in(11,82,57,88) then do; District='TRANS NZOIA WEST';end;
else if location_id in(9,66,67,86,89) then do; District='MT. ELGON';end;
else if location_id in(17) then do; District='KEIYO';end;
else if location_id in(18) then do; District='BARINGO';end;
else if location_id in(68) then do; District='MARIGAT';end;
else if location_id in(19,26,27,81,79) then do; District='BUSIA';end;
else if location_id in(20,78) then do; District='BUNYALA';end;
else if location_id in(23,83,55) then do; District='BUTULA';end;
else if location_id in(87) then do; District='SAMIA';end;
else if location_id not in(31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,16,21,22,30,80) then do; District=name;end;

/* Province */

if location_id in(1,13,77,14,56,58,15,2,70,3,60,4,62,54,11,82,57,17,18,10,68,31,61,69,71,80,72,84,73,28,88) then do; Province='NORTH RIFT';end;
else if location_id in(5,63,6,85,8,64,75,76,74,12,65,9,66,67,59,19,26,27,81,20,23,83,79,78,55,86,87,89,90,91,92,93) then do; Province='WESTERN';end;
else if location_id in(7,24,25) then do; Province='NYANZA';end;
rename name=location;
run;

proc sort data=location1 nodupkey out=location_final; by location_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE location location1;
		RUN;
   		QUIT;


%Mend create_location;
%Create_location;
