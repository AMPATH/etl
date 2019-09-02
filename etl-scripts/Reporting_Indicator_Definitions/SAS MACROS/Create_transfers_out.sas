
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_transfers_out();


PROC IMPORT OUT= Transfers_out
            DATAFILE= "C:\DATA\CSV DATASETS\Transfers_out.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data Transfers_out1;
format transfer_date ddmmyy10.;
set Transfers_out;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
transfer_date=mdy(mm,dd,yy);
if value_coded=1286 then transfer='ampath       ';
if value_coded in(1287,1594) then transfer='non-ampath';
drop  value_coded dd mm yy obs_datetime;
run;

proc sort data=Transfers_out1 out=Transfers_out_final nodupkey; by person_id transfer_date transfer;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE Transfers_out1 Transfers_out;
		RUN;
   		QUIT;


%Mend create_transfers_out;
%create_transfers_out;
