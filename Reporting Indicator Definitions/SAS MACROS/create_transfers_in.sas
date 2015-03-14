


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_transfers_in();

PROC IMPORT OUT= WORK.transfers_in 
            DATAFILE= "C:\DATA\CSV DATASETS\transfers_in.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;



data transfers_in1;
Format app_date ddmmyy10.;
set transfers_in;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);


drop dd mm yy  obs_datetime  obs_group_id obs_id ;
run;



data transfers_in2;
set transfers_in1;
if  concept_id= 7015 and value_coded= 1286 then transfer_from='Ampath      ';
else if concept_id =7015 and value_coded= 1287 then transfer_from='Non_Ampath ';

drop encounter_id value_coded concept_id;
run;



proc sort data=transfers_in2 out=transfers_in_final nodupkey; by person_id app_date;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE transfers_in transfers_in1 transfers_in2;
		RUN;
   		QUIT;


%MEND create_transfers_in;

%create_transfers_in;
