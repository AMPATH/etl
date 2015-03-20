


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_weight();	


PROC IMPORT OUT= WORK.weight1 
            DATAFILE= "C:\DATA\CSV DATASETS\weight.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data weight2;
Format weight 3.1 weight_date ddmmyy10.;
set weight1;
if concept_id=5089;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
weight_date=mdy(mm,dd,yy);
weight=value_numeric;
keep person_id weight_date weight;
run;


proc sort data=weight2 nodupkey  out=weight_final dupout=dupweight; by person_id weight_date weight  ; run;
proc sort data=weight2 nodupkey  out=weight_final dupout=dupweight1; by person_id weight_date   ; run;



data diff_weight;
merge dupweight(in=a) dupweight1(in=b);
by person_id weight_date;
if b and not a;
run; 


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE weight weight1 dupweight ;
		RUN;
   		QUIT;


%MEND create_weight;
%create_weight;
