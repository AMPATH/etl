
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_reason_attempted_contact();


PROC IMPORT OUT= reason_attemp
            DATAFILE= "C:\DATA\CSV DATASETS\reason_attemp_contact.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data reason_attemp1;
format appdate   ddmmyy10.;
set reason_attemp;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
appdate=mdy(mm,dd,yy);
if value_coded in(1143) then reason_of_contact='research         ';
if value_coded in(1448) then reason_of_contact='missed app        ';
if value_coded in(1589) then reason_of_contact='pharmacy request ';
if value_coded in(1590) then reason_of_contact='clinical issues ';
if value_coded in(5622) then reason_of_contact='Other    ';
drop obs_datetime mm yy dd concept_id value_coded;
run;

proc sort data=death_outreach out=death_outreach_final  nodupkey; by person_id appdate dod ;run;

PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE outreach_dates death_outreach;
		RUN;
   		QUIT;


%Mend create_reason_attempted_contact;
%create_reason_attempted_contact;
