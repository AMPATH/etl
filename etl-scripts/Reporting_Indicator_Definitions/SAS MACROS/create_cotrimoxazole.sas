


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_cotrimoxazole();	
/*
PROC IMPORT OUT= WORK.cotri
            DATAFILE= "C:\DATA\CSV DATASETS\cotrimoxazole.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/
Libname tmp 'C:\DATA\CSV DATASETS'; 

data cotri1;
format cotri_date ddmmyy10.;
set tmp.Contrimaxole;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
cotri_date=mdy(mm,dd,yy);

drop concept_id dd mm yy obs_datetime  ;
run;




proc sort data=cotri1 out=cotri_final nodupkey; by person_id cotri_date ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE  cotri  cotri1  ;
		RUN;
   		QUIT;


%MEND create_cotrimoxazole;
%create_cotrimoxazole;
