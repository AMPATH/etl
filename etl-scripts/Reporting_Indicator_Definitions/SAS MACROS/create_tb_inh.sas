


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_tb_inh();	

PROC IMPORT OUT= WORK.tb_inh 
            DATAFILE= "C:\DATA\CSV DATASETS\tb_inh.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data tb_inh1;
Format  inh_date ddmmyy10.;
set tb_inh;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
inh_date=mdy(mm,dd,yy);
keep person_id inh_date concept_id;
run;


proc sort data=tb_inh1 nodupkey out=tb_inh_final ; by person_id inh_date; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE tb_inh1 tb_inh ;
		RUN;
   		QUIT;


%MEND create_tb_inh;
%create_tb_inh;
