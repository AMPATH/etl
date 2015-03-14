
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_adherence();


PROC IMPORT OUT= WORK.adherence 
            DATAFILE= "C:\DATA\CSV DATASETS\adherence.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data adh;
format adherence_date ddmmyy10.;
set adherence;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
adherence_date=mdy(mm,dd,yy);
drop obs_datetime dd mm yy;
if value_numeric=1 then adherence='imperfect';
else if value_coded in(1163) then adherence='perfect';
else if value_coded not in(1163) then adherence='imperfect';
run;

proc sort data=adh out=adherence_final nodupkey; by person_id adherence_date adherence; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE adherence adh;
		RUN;
   		QUIT;



%Mend create_adherence;
%Create_adherence;
