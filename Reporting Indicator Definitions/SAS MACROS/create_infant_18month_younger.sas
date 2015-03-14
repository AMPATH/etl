


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_infant_18months_younger();	


PROC IMPORT OUT= WORK.infant_18 
            DATAFILE= "C:\DATA\DATA SETS\infant_18_month_younger.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

proc freq data=infant_18; table concept_id; run;


data infant_181;
Format mother $50. report_date ddmmyy10.;
set infant_18;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
report_date=mdy(mm,dd,yy);
if concept_id =1996then Mother='Mother with infant less 18mnt';
if concept_id =1997then Mother='was/is the infant breastfed';
if value_coded=1065 then Answer='yes';
if value_coded=1066 then Answer='No';

keep person_id report_date mother  Answer;
run;

/*proc freq data=feeding1; tables feeding; run;
proc freq data=infant_18; table value_coded; run;*/
proc sort data=infant_181 nodupkey  out=Mumfed_baby_final dupout=dupinfantfed; by person_id report_date Mother  ; run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE infant_18 infant_181;
		RUN;
   		QUIT;


%MEND create_infant_18months_younger;
%create_infant_18months_younger;
