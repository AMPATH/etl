


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_feeding();	


PROC IMPORT OUT= WORK.feeding 
            DATAFILE= "C:\DATA\CSV DATASETS\feeding.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


proc freq data=feeding; table value_coded; run;


data cow_milk(keep=person_id feeding_date cow_milk)
Xpressed_breast_milk(keep=person_id feeding_date Xpressed_breast_milk)
breast_milk(keep=person_id feeding_date breast_milk)
other_fluids(keep=person_id feeding_date other_fluids)
water(keep=person_id feeding_date water)
solid_food(keep=person_id feeding_date solid_food)
infant_formular(keep=person_id feeding_date infant_formular)
weaned(keep=person_id feeding_date weaned)
mixed_feeding(keep=person_id feeding_date mixed_feeding)
Pred_breast_feeding(keep=person_id feeding_date Pred_breast_feeding)
xclussive_breast_feeding(keep=person_id feeding_date xclussive_breast_feeding)
;
Format feeding_date ddmmyy10.;
set feeding;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
feeding_date=mdy(mm,dd,yy);

if value_coded=968 then cow_milk=1;
if cow_milk eq 1 then output cow_milk;

if value_coded=1173 then Xpressed_breast_milk=1;
if Xpressed_breast_milk eq 1 then output Xpressed_breast_milk;

if value_coded=1402 then breast_milk=1;
if breast_milk eq 1 then output breast_milk;

if value_coded=1403 then other_fluids=1;
if other_fluids eq 1 then output other_fluids;

if value_coded=1404 then water=1;
if water eq 1 then output water;

if value_coded=1405 then solid_food=1;
if solid_food eq 1 then output solid_food;

if value_coded=5254 then infant_formular=1;
if infant_formular eq 1 then output infant_formular;

if value_coded=1152 then weaned=1;
if weaned eq 1 then output weaned;

if value_coded=6046 then mixed_feeding=1;
if mixed_feeding eq 1 then output mixed_feeding;

if value_coded=1150 then Pred_breast_feeding=1;
if Pred_breast_feeding eq 1 then output Pred_breast_feeding;

if value_coded=5526 then xclussive_breast_feeding=1;
if xclussive_breast_feeding eq 1 then output xclussive_breast_feeding;
run;

proc sort data=cow_milk nodupkey; by person_id feeding_date;
proc sort data=Xpressed_breast_milk nodupkey; by person_id feeding_date;
proc sort data=breast_milk nodupkey; by person_id feeding_date;
proc sort data=other_fluids nodupkey; by person_id feeding_date;
proc sort data=water nodupkey; by person_id feeding_date;
proc sort data=solid_food nodupkey; by person_id feeding_date;
proc sort data=infant_formular nodupkey; by person_id feeding_date;
proc sort data=weaned nodupkey; by person_id feeding_date;
proc sort data=mixed_feeding nodupkey; by person_id feeding_date;
proc sort data=Pred_breast_feeding nodupkey; by person_id feeding_date;
proc sort data=xclussive_breast_feeding nodupkey; by person_id feeding_date;
run;


data feeding_final;
merge cow_milk Xpressed_breast_milk breast_milk other_fluids 
water solid_food infant_formular weaned mixed_feeding Pred_breast_feeding
xclussive_breast_feeding;
by person_id feeding_date;
run;




PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE feeding cow_milk Xpressed_breast_milk breast_milk other_fluids 
water solid_food infant_formular weaned mixed_feeding Pred_breast_feeding
xclussive_breast_feeding;
		RUN;
   		QUIT;


%MEND create_feeding;
%create_feeding;
