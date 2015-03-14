


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_modern_contraceptive();


PROC IMPORT OUT= WORK.modern_contraceptives
            DATAFILE= "C:\DATA\CSV DATASETS\modern_contraceptive.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;




data all_contraceptives;
set modern_contraceptives;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
contracept_date=mdy(mm,dd,yy);
format contracept_date ddmmyy10.;
drop dd mm yy obs_datetime obs_id;
run;

proc sort data=all_contraceptives;by person_id contracept_date;run;

data modern_contracept condoms_provided ;
set all_contraceptives;
if value_coded in (190, 6718,6717)  then output condoms_provided;
else if value_coded  not in (190, 6718,6717) then output modern_contracept;
run;


proc sort data=modern_contracept;by person_id ;run;

data modern_contracept1;
set modern_contracept;
by person_id;
if value_coded in (6700,6701) then method ='Non-Regular';

else if value_coded not in (6700,6701) then method = 'Regular   ' ;
drop value_coded concept_id;
run;

data condoms_provided1;
set condoms_provided;
by person_id;
condom_provided='Yes';
drop value_coded concept_id;
run;


proc sort data=condoms_provided1 out=condoms_provided_final nodupkey; by person_id contracept_date;run;
proc sort data=modern_contracept1 out=modern_contracept_final nodupkey; by person_id contracept_date;run; 

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE modern_contracept1 modern_contracept all_contraceptives modern_contraceptives condoms_provided1
;
		RUN;
   		QUIT;

	

%MEND create_modern_contraceptive;
%create_modern_contraceptive;
