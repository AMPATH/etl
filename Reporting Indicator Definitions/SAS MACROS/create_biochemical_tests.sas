


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_biochemical_tests();	


PROC IMPORT OUT= WORK.biochemical_tests 
            DATAFILE= "C:\DATA\CSV DATASETS\biochemical_tests.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data biochem ;
Format test_date ddmmyy10.;
set biochemical_tests;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
test_date=mdy(mm,dd,yy);
drop  dd mm yy obs_datetime ;
run;

/*Obtaining Hb Tests, SGPT Tests, SGOT Tests and Creatinine Tests*/
data Hb(rename=value_numeric=Hb_results keep=person_id test_date value_numeric)
Sgpt (rename=value_numeric=Sgpt_results keep=person_id test_date value_numeric)
Sgot (rename=value_numeric=Sgot_results keep=person_id test_date value_numeric)
Creatinine (rename=value_numeric=creatine_results keep=person_id test_date value_numeric);
set biochem;
if concept_id=21 then output Hb;else
if concept_id=654 then output Sgpt;else
if concept_id=790 then output Sgot;else
if concept_id=653 then output Creatinine;
run;


proc sort data=Hb nodupkey  out=Hb_final ; by person_id test_date; run;
proc sort data=Sgpt nodupkey  out=Sgpt_final ; by person_id test_date; run;
proc sort data=Sgot nodupkey  out=Sgot_final ; by person_id test_date; run;
proc sort data=Creatinine nodupkey  out=Creatinine_final; by person_id test_date; run;

/*Merging all the Hb and Biochem tests*/
data Biochem_tests_final;
merge Hb_final Sgpt_final Sgot_final Creatinine_final;
by person_id test_date;
run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE Hb Sgpt Sgot Creatinine biochem biochemical_tests;
		RUN;
   		QUIT;


%MEND create_biochemical_tests;

%create_biochemical_tests;


