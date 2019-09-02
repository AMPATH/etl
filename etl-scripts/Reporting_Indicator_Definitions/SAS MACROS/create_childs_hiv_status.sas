


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_Childs_hiv_status();	


PROC IMPORT OUT= WORK.Childs_hiv_status
            DATAFILE= "C:\DATA\CSV DATASETS\Childs_hiv_status.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data Childs_hiv_status1;
Format app_date ddmmyy10.;
set Childs_hiv_status;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
app_date=mdy(mm,dd,yy);
if value_coded=1169 then childs_hiv_status='hiv infected   ';
else if value_coded=664 then childs_hiv_status='negative';
else if value_coded=703 then childs_hiv_status='positive';
else if value_coded=822 then childs_hiv_status='exposed to hiv';
if value_coded in(1169,703) then hiv_status='positive          ';
else if value_coded in(664) then hiv_status='negative';
else if value_coded in(822) then hiv_status='exposed to hiv';

drop value_coded dd mm yy obs_datetime concept_id;
run;



proc sort data=Childs_hiv_status1 nodupkey  out=Childs_hiv_status_final dupout=dup_childs_status; by person_id app_date  ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE Childs_hiv_status Childs_hiv_status1;
		RUN;
   		QUIT;


%MEND create_Childs_hiv_status;
%create_Childs_hiv_status;
