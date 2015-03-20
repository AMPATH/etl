


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_Eligible_No_ARVS();	

PROC IMPORT OUT= WORK.Eligible 
            DATAFILE= "C:\DATA\CSV DATASETS\EligibleNoARVS.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data elig;
format app_date ddmmyy10. ;
set Eligible;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
app_date=mdy(mm,dd,yy);
eligible_NOarvs=1;
keep person_id app_date eligible_NOarvs   ;
run;

proc sort data=elig out=EligibleNOARVS_Final nodupkey; by person_id app_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE civil civil_out ;
		RUN;
   		QUIT;


%MEND create_Eligible_No_ARVS;
%create_Eligible_No_ARVS;
