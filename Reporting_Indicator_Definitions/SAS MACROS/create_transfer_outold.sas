


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_transfer_out();	

PROC IMPORT OUT= WORK.transfers_out 
            DATAFILE= "C:\DATA\DATA SETS\transfers_out.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data transfers;
format app_date ddmmyy10. transfer_out;
set transfers_out;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
transfer_out=1;
keep person_id app_date transfer_out;
run;

proc sort data=transfers out=transfers_out_final nodupkey; by person_id app_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE transfers transfers_out ;
		RUN;
   		QUIT;


%MEND create_transfer_out;
%create_transfer_out;
