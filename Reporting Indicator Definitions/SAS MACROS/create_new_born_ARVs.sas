


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_new_born_arvs();	

PROC IMPORT OUT= WORK.new_born_arvs 
            DATAFILE= "C:\DATA\CSV DATASETS\new_born_ARVs.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data new_born_arvs1;
format new_born_arvsdate date9. ;
set new_born_arvs;
yy=scan(obs_datetime,1, '-');
mm=scan(obs_datetime,2, '-');
dd=scan(scan(obs_datetime,1, ' '),-1, '-');
new_born_arvsdate=mdy(mm,dd,yy);

new_born_arvs='Yes';
keep person_id   new_born_arvsdate  new_born_arvs ;
run;


proc sort data=new_born_arvs1 out=new_born_arvs_final nodupkey; by person_id new_born_arvsdate ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE new_born_arvs1 new_born_arvs ;
		RUN;
   		QUIT;


%MEND create_new_born_arvs;
%create_new_born_arvs;
