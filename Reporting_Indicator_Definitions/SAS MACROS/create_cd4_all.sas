


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_cd4();	

PROC IMPORT OUT= WORK.cd4_out 
            DATAFILE= "C:\DATA\DATA SETS\cd4.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data cd4;
format cd4date date9. ;
set cd4_out;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
cd4date=mdy(mm,dd,yy);

keep person_id   cd4date value_numeric ;
run;

proc sort data=cd4 out=cd4_final /*nodupkey*/; by person_id ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE cd4 cd4_out ;
		RUN;
   		QUIT;


%MEND create_cd4;
%create_cd4;
