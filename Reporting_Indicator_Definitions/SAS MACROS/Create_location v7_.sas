
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_location();


PROC IMPORT OUT= WORK.location 
            DATAFILE= "C:\DATA\CSV DATASETS\location.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data location1;
set location;

/*Ampath sites*/

if location_id in(2,3,4,5,6,7,8,9,10,11,12,17,18,19,20,23,28,31,55,71,72,73,79,78,87) then do; site=name;end;
if location_id in(1,13,14,15,77,80,56,58,102) then do; site='MTRH'; end;
if location_id in(26,27,81) then do; site='Busia'; end;
if location_id in(63,92,93) then do; site='Amukura'; end;
if location_id in(30,82,57,88) then do; site='Kitale'; end;
if location_id in(64,75,76,74) then do; site='Webuye'; end;
if location_id in(66,67,86,89) then do; site='Mt. Elgon'; end;
if location_id in(24,25,103,104,105) then do; site='Chulaimbo'; end;
if location_id in(65,90,91,106) then do; site='Teso'; end;
if location_id in(68,95) then do; site='Kabarnet'; end;
if location_id in(83) then do; site='Khuyangu'; end;
if location_id in(70) then do; site='Mosoriot'; end;
if location_id in(35,59,60) then do; site='Turbo'; end;
if location_id in(85,96,97) then do; site='Naitiri'; end;
if location_id in(61,62,54) then do; site='Burnt Forest'; end;
if location_id in(69,109) then do; site='Uasin Gishu District Hospital'; end;
if location_id in(94,107) then do; site='Iten'; end;
if location_id in(98) then do; site='Mosoriot'; end;
if location_id in(99,108) then do; site='Nambale'; end;
if location_id in(100) then do; site='Port Victoria'; end;


/*health centres*/

if location_id in(1,13,14,15) then do; HealthCentre='MTRH                           '; end;
else if location_id in(19,26,27) then do; HealthCentre='BUSIA DH       ';end;
else if location_id in(7,24,25) then do; HealthCentre='CHULAIMBO  SDH        ';end;
else if location_id in(2) then do; HealthCentre='MOSORIOT HC';end;
else if location_id in(3) then do; HealthCentre='TURBO HC';end;
else if location_id in(4) then do; HealthCentre='BURNT FOREST SDH';end;
else if location_id in(5) then do; HealthCentre='AMUKURA HC';end;
else if location_id in(6) then do; HealthCentre='NAITIRI SDH';end;
else if location_id in(8) then do; HealthCentre='WEBUYE DH';end;
else if location_id in(9) then do; HealthCentre='MT. ELGON DH';end;
else if location_id in(10) then do; HealthCentre='KAPENGURIA DH';end;
else if location_id in(11) then do; HealthCentre='KITALE DH';end;
else if location_id in(12) then do; HealthCentre='TESO DH';end;
else if location_id in(17) then do; HealthCentre='ITEN DH';end;
else if location_id in(18) then do; HealthCentre='KABARNET DH';end;
else if location_id in(20) then do; HealthCentre='PORT VICTORIA HOSP';end;
else if location_id in(23) then do; HealthCentre='KHUNYANGU DH';end;
else if location_id in(28) then do; HealthCentre='ZIWA SDH';end;
else if location_id in(30) then do; HealthCentre='ANDERSON';end;
else if location_id in(31) then do; HealthCentre='UASIN GISHU DH';end;
else if location_id in(35) then do; HealthCentre='TURBO POLICE STATION';end;
else if location_id in(54) then do; HealthCentre='PLATEAU MISSION HOSP';end;
else if location_id in(55) then do; HealthCentre='BUMALA A HC';end;
else if location_id in(56) then do; HealthCentre='GK PRISONS DISP ELDORET';end;
else if location_id in(57) then do; HealthCentre='GK FARM PRISONS DISP KITALE';end;
else if location_id in(58) then do; HealthCentre='GK PRISONS DISP NGERIA';end;
else if location_id in(59) then do; HealthCentre='MAUTUMA SDH';end;
else if location_id in(60) then do; HealthCentre='CHEPSAITA DISP';end;
else if location_id in(61) then do; HealthCentre='KAPTAGAT FOREST DISP';end;
else if location_id in(62) then do; HealthCentre='KESSES HC';end;
else if location_id in(63) then do; HealthCentre='LUKOLIS DISP';end;
else if location_id in(64) then do; HealthCentre='BOKOLI SDH';end;
else if location_id in(65) then do; HealthCentre='ANGURAI HC';end;
else if location_id in(66) then do; HealthCentre='CHEPTAIS SDH';end;
else if location_id in(67) then do; HealthCentre='CHESIKAKI DISP';end;
else if location_id in(68) then do; HealthCentre='MARIGAT SDH';end;
else if location_id in(69) then do; HealthCentre='HURUMA DH';end;
else if location_id in(70) then do; HealthCentre='PIONEER HC';end;
else if location_id in(71) then do; HealthCentre='MOIS BRIDGE HC';end;
else if location_id in(72) then do; HealthCentre='MOI UNIVERSITY HC';end;
else if location_id in(73) then do; HealthCentre='SOY HC';end;
else if location_id in(74) then do; HealthCentre='MIHUU DISP';end;
else if location_id in(75) then do; HealthCentre='SINOKO DISP';end;
else if location_id in(76) then do; HealthCentre='MILO HC';end;
else if location_id in(77) then do; HealthCentre='MOIBEN HC';end;
else if location_id in(78) then do; HealthCentre='MUKHOBOLA HC';end;
else if location_id in(79) then do; HealthCentre='NAMBALE HC';end;
else if location_id in(80) then do; HealthCentre='MOI BARRACKS HC';end;
else if location_id in(81) then do; HealthCentre='GK PRISONS DISP BUSIA';end;
else if location_id in(82) then do; HealthCentre='SABOTI SDH';end;
else if location_id in(83) then do; HealthCentre='BUMALA B HC';end;
else if location_id in(84) then do; HealthCentre='MTRH MAIN HOSPITAL';end;
else if location_id in(85) then do; HealthCentre='MAKUTANO DISP';end;
else if location_id in(86) then do; HealthCentre='KAPTAMA (FRIENDS) HC';end;
else if location_id in(87) then do; HealthCentre='SIO PORT HOSP';end;
else if location_id in(88) then do; HealthCentre='TULWET HC';end;
else if location_id in(89) then do; HealthCentre='KOPSIRO HC';end;
else if location_id in(90) then do; HealthCentre='CHANGARA DISP';end;
else if location_id in(91) then do; HealthCentre='MALABA DISP';end;
else if location_id in(92) then do; HealthCentre='AMASE DISP';end;
else if location_id in(93) then do; HealthCentre='OBEKAI DISP';end;
else if location_id in(94) then do; HealthCentre='TAMBACH SDH';end;
else if location_id in(95) then do; HealthCentre='TENGES HC';end;
else if location_id in(96) then do; HealthCentre='KIBISI DISP';end;
else if location_id in(97) then do; HealthCentre='SANGO DISP';end;
else if location_id in(98) then do; HealthCentre='DIGUNA DISP';end;
else if location_id in(99) then do; HealthCentre='LUPIDA HC';end;
else if location_id in(100) then do; HealthCentre='OSIEKO DISP';end;
else if location_id in(102) then do; HealthCentre='ELGEYO BORDER DISP';end;
else if location_id in(103) then do; HealthCentre='RIAT DISP';end;
else if location_id in(104) then do; HealthCentre='SUNGA DISP';end;
else if location_id in(105) then do; HealthCentre='SIRIBA DISP';end;
else if location_id in(106) then do; HealthCentre='KAMOLO DISP';end;
else if location_id in(107) then do; HealthCentre='KAPTEREN HC';end;
else if location_id in(108) then do; HealthCentre='MADENDE HC';end;
else if location_id in(109) then do; HealthCentre='RAI-PLYWOOD';end;


else if location_id not in(31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53) then do; HealthCentre=name;end;


/* County */

If HealthCentre in('MTRH','ELGEYO BORDER DISP','BURNT FOREST SDH','CHEPSAITA DISP','GK PRISONS DISP ELDORET',
'GK PRISONS DISP NGERIA','HURUMA DH','KESSES HC','MOI UNIVERSITY HC','MOIBEN HC','MOIS BRIDGE HC','PIONEER HC',
'PLATEAU MISSION HOSP','SOY HC','TURBO HC','UASIN GISHU DH','ZIWA SDH','RAI-PLYWOOD','MOIS BRIDGE','MOI BARRACKS HC') 
then county='UASIN GISHU    ';

else if HealthCentre in('BOKOLI SDH','MIHUU DISP','MILO HC','SINOKO DISP','WEBUYE DH','KAPTAMA (FRIENDS) HC',
'MT. ELGON DH','NAITIRI SDH','CHEPTAIS SDH','CHESIKAKI DISP','MAKUTANO DISP','KOPSIRO HC','KIBISI DISP',
'SANGO DISP') then county='BUNGOMA    ';

else if HealthCentre in('BUSIA DH','GK PRISONS DISP BUSIA','MUKHOBOLA HC','OSIEKO DISP','PORT VICTORIA HOSP',
'BUMALA A HC','BUMALA B HC','KHUNYANGU DH','ANGURAI HC','CHANGARA DISP','MALABA DISP','TESO DH','AMUKURA HC'
'KAMOLO DISP','LUKOLIS DISP','NAMBALE HC','SIO PORT HOSP','AMASE DISP','OBEKAI DISP','LUPIDA HC',
'MADENDE HC') then county='BUSIA    ';

else if HealthCentre in('ITEN DH','TAMBACH SDH','KAPTEREN HC') then county='ELGEYO MARAKWET    ';

else if HealthCentre in('CHULAIMBO  SDH ','RIAT DISP','SUNGA DISP','SIRIBA DISP') then county='KISUMU ';

else if HealthCentre in('MOSORIOT HC','DIGUNA DISP') then county='NANDI ';

else if HealthCentre in('ANDERSON','GK FARM PRISONS DISP KITALE','KITALE DH','SABOTI SDH','TULWET HC')
then county='TRANS NZOIA ';

else if HealthCentre in('MAUTUMA SDH') then county='KAKAMEGA';


else if HealthCentre in('KAPENGURIA DH') then county='WEST POKOT';

else if HealthCentre in('KABARNET DH','MARIGAT SDH','TENGES HC') then county='BARINGO';

else if HealthCentre in('KAPTEREN HC') then county='BARINGO';









/* district */

if location_id in(1,13,14,56,15,4,31,54,84,102) then do; District='ELDORET EAST';end;
else if location_id in(77,3,35,60,69,71,73,28,80,109) then do; District='ELDORET WEST';end;
else if location_id in(58,70,62,72) then do; District='WARENG';end;
else if location_id in(2,98) then do; District='NANDI NORTH';end;
else if location_id in(10) then do; District='WEST POKOT';end;
else if location_id in(59) then do; District='LUGARI';end;
else if location_id in(5,63,92,93) then do; District='TESO SOUTH';end;
else if location_id in(6,85,96,97) then do; District='BUNGOMA NORTH';end;
else if location_id in(7,24,25,103,104,105) then do; District='KISUMU WEST';end;
else if location_id in(8,64,75,76,74) then do; District='BUNGOMA EAST';end;
else if location_id in(12,65,90,91,106) then do; District='TESO NORTH';end;
else if location_id in(11,30,82,57,88) then do; District='TRANS NZOIA WEST   ';end;
else if location_id in(9,86) then do; District='MT. ELGON';end;
else if location_id in(66,67,89) then do; District='CHEPTAIS';end;
else if location_id in(17,94,107) then do; District='KEIYO NORTH';end;
else if location_id in(18,95) then do; District='BARINGO';end;
else if location_id in(68) then do; District='MARIGAT';end;
else if location_id in(19,26,27,81) then do; District='BUSIA';end;
else if location_id in(79,99,108) then do; District='NAMBALE';end;
else if location_id in(20,78,100) then do; District='BUNYALA';end;
else if location_id in(23,83,55) then do; District='BUTULA';end;
else if location_id in(87) then do; District='SAMIA';end;
else if location_id not in(31,32,33,34,35,36,37,38,39,40,41,42,43,44,45,46,47,48,49,50,51,52,53,16,21,22,30,80) then do; District=name;end;

/* Province */

if location_id in(1,13,77,14,56,58,15,2,70,3,60,4,62,54,11,82,57,17,18,10,68,30,31,35,61,69,71,80,72,84,73,28,88,94,95,98,102,107,109) then do; Province='NORTH RIFT  ';end;
else if location_id in(5,63,6,85,8,64,75,76,74,12,65,9,66,67,59,19,26,27,81,20,23,83,79,78,55,86,87,89,90,91,92,93,96,97,99,100,106,108) then do; Province='WESTERN';end;
else if location_id in(7,24,25,103,104,105) then do; Province='NYANZA';end;
rename name=location;
run;

proc sort data=location1 nodupkey out=location_final; by location_id;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE location location1;
		RUN;
   		QUIT;


%Mend create_location;
%Create_location;
