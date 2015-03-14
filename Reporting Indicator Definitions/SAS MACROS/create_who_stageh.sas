


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_Who_stage();	

PROC IMPORT OUT= WORK.who
            DATAFILE= "C:\DATA\CSV DATASETS\WHO_stage_adultped.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

PROC IMPORT OUT= WORK.who_conditions
            DATAFILE= "C:\DATA\CSV DATASETS\who_conditions.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data who1;
format who_date ddmmyy10.;
set who;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
who_date=mdy(mm,dd,yy);

drop concept_id dd mm yy obs_datetime value_numeric ;
run;

data Who3;
set who1;
if value_coded=1220 then WHOSTAGEx='whostage1 Ped    ';
	else if value_coded=1221 then WHOSTAGEx='whostage2 Ped';
	else if value_coded=1222 then WHOSTAGEx='whostage3 Ped';
	else if value_coded=1223 then WHOSTAGEx='whostage4 Ped';
	else if value_coded=1204 then WHOSTAGEx='whostage1 Adult';
	else if value_coded=1205 then WHOSTAGEx='whostage2 Adult';
	else if value_coded=1206 then WHOSTAGEx='whostage3 Adult';
	else if value_coded=1207 then WHOSTAGEx='whostage4 Adult';
drop encounter_id value_coded location_id;
RUN;

proc sort  data=who3 out=who2 nodupkey; by person_id who_date;run;



data condition1;
Format conDate ddmmyy10.;
set who_conditions;

dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
conDate=mdy(mm,dd,yy);

keep person_id concept_id conDate value_coded;
run;

proc sort data=condition1; by value_coded;run;

data condition2;
set condition1;
if value_coded in(5327,5328) then Staging='whostage1 Adult';
else if value_coded in(5332,5012,836,1441,2091,1249,2092,2093,5330,5329) then Staging='whostage2 Adult';
else if value_coded in(5030,2094,2095,2096,2097,2098,2099,2100,2101,2102,2103,2104,2105,2106,2107,
2110,2111,2108,2109,5338,5339) then Staging='whostage3 Adult';
else if value_coded in(2112,2113,2114,2115,2116,2117,2118,2119,2120,2121,2122,2123,2124,2125,
2126,2127,2128,2129,2130,2131,2132,2133,2134,2135,2136,2137,2138,
2139,2140,2141,2142,2143,2144,2145,2146,2147,2148,2149,2150,2151,2152,2153,990,5041,5350,5354,5344) 
then Staging='whostage4 Adult';
else if value_coded in(5327,825,5328)
then Staging='whostage1 Ped';
else if value_coded in(119,836,5020,5012,1214,1213,1212,114,1210,1211,2209,2091,1249,
2093,2210,1447,2211,825) then Staging='whostage2 Ped';
else if value_coded in(5018,5050,5340,4024,1216,5022,1215,1217,5029,42,1218,2212,5027,
5334,5337,2213,2214,2215,2216) then Staging='whostage3 Ped';
else if value_coded in(204,5016,1219,5345,5032,5033,5034,5035,5038,5043,1216,5333,507,
5025,882,5046,5355,823,1435,5042,2217,2218) then Staging='whostage4 Ped';
rename condate=who_date;
*if value_coded='.' then delete;
drop value_coded;
run;

proc sort  data=condition2 out=condition nodupkey; by person_id who_date;run;


proc sort  data=condition; by person_id who_date;run;
proc sort  data=who2; by person_id who_date ;run;

data who_n_condition1;
merge who2 condition;
by person_id who_date;
run;

data who_n_condition;
set who_n_condition1;
if WHOSTAGEx ne'' then WHOSTAGE=WHOSTAGEx;
else if WHOSTAGEx ='' then WHOSTAGE=Staging;
drop WHOSTAGEx concept_id  staging;
run;



proc sort data=who_n_condition out=WHO_Stage_final nodupkey; by person_id who_date;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE  who  Who1 Who2 who3 condition condition1  who_n_condition  who_n_condition1    who_conditions;
		RUN;
   		QUIT;


%MEND create_Who_stage;
%create_Who_stage;
