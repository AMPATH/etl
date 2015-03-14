


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_discordant();	

PROC IMPORT OUT= WORK.discordant_out 
            DATAFILE= "C:\DATA\DATA SETS\discordant.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data discordant;
format app_date ddmmyy10. ;
set discordant_out;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
discordant_out=1;
keep person_id app_date location_id discordant_out value_coded;
run;

proc sort data=discordant out=discordant_out_final nodupkey; by person_id app_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE discordant discordant_out ;
		RUN;
   		QUIT;


%MEND create_discordant;
%create_discordant;
