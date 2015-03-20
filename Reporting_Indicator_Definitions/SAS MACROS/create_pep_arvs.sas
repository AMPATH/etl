%MACRO create_PEP_arvs();

PROC IMPORT OUT= WORK.pep_arvs
            DATAFILE= "C:\DATA\CSV DATASETS\PEP_arvs.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
	

data pep2;
format pepdate ddmmyy10.;
set pep_arvs;
dd=substr(pep_date,9,2);
mm=substr(pep_date,6,2);
yy=substr(pep_date,1,4);
pepdate=mdy(mm,dd,yy);
pep_arvs=1;
keep person_id  pepdate pep_arvs ;
run;

proc sort data=pep2 nodupkey out=pep_arvs_final; by person_id pepdate  ; run;





PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE pep2 pep_arvs ;
		RUN;
   		QUIT;


%MEND create_PEP_arvs;

%create_PEP_arvs;
