


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_height();	


PROC IMPORT OUT= WORK.height 
            DATAFILE= "C:\DATA\CSV DATASETS\height.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data height1;
Format height 3.1 height_date ddmmyy10.;
set height;
if concept_id=5090;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
height_date=mdy(mm,dd,yy);
height=value_numeric;
keep person_id height_date height;
run;


proc sort data=height1 nodupkey  out=height_final dupout=dupheight; by person_id height_date height  ; run;
proc sort data=height1 nodupkey  out=height_final dupout=dupheight1; by person_id height_date   ; run;



data diff_height;
merge dupheight(in=a) dupheight1(in=b);
by person_id height_date;
if b and not a;
rename height_date=app_date;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE height height1 dupheight dupheight1;
		RUN;
   		QUIT;


%MEND create_height;
%create_height;
