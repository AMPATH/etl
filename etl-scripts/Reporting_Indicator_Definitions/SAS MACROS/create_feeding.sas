


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_feeding();	


PROC IMPORT OUT= WORK.feeding 
            DATAFILE= "C:\DATA\CSV DATASETS\feeding.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


/*proc freq data=feeding; table value_coded; run;*/


data feeding1;
Format feeding $30. feeding_date ddmmyy10.;
set feeding;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
feeding_date=mdy(mm,dd,yy);
if value_coded=968 then feeding='cow milk';
if value_coded=1173 then feeding='Xpressed breast milk';
if value_coded=1402 then feeding='breast milk';
if value_coded=1403 then feeding='other fluids';
if value_coded=1404 then feeding='water';
if value_coded=1405 then feeding='solid food';
if value_coded=5254 then feeding='infant formular';
if value_coded=1152 then feeding='weaned';
if value_coded=1401 then feeding='regular diet ';
if value_coded=6046 then feeding='mixed feeding ';
if value_coded=1150 then feeding='predominate breastfeeding ';
if value_coded=6820 then feeding='Uji ';
if value_coded=6985 then feeding='complementary feeding ';
if value_coded=5526 then feeding='xclusive breastfeeding ';

keep person_id feeding_date feeding value_coded;
run;


/*proc freq data=feeding1; tables feeding; run;*/

proc sort data=feeding1 nodupkey  out=feed_final dupout=dupfeed; by person_id feeding_date feeding  ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE feeding feeding1;
		RUN;
   		QUIT;


%MEND create_feeding;
%create_feeding;
