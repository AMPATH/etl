


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_express_care();

PROC IMPORT OUT= WORK.express_persons
            DATAFILE= "C:\DATA\CSV DATASETS\express_persons.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


data express_persons1;
Format app_date ddmmyy10.;
set express_persons;
if encounter_type =17 then patient_type='stable      ';
if encounter_type =19 then patient_type='high risk ';
dd=substr(encounter_datetime,9,2);
mm=substr(encounter_datetime,6,2);
yy=substr(encounter_datetime,1,4);
app_date=mdy(mm,dd,yy);
rename patient_id=person_id;
drop  dd mm yy encounter_datetime encounter_type;
run;


proc sort data=express_persons1 nodupkey  out=express_persons_final dupout=dupexpress; by person_id app_date patient_type ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE express_persons express_persons1 ;
		RUN;
   		QUIT;


%MEND create_express_care;

%create_express_care;
