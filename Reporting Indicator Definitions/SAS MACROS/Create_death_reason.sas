
PROC OPTIONS OPTION = MACRO; RUN;

%Macro create_death_reason();


PROC IMPORT OUT= death_reason
            DATAFILE= "C:\DATA\CSV DATASETS\death_reason.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

data death_reason1;
format appdate ddmmyy10.;
set death_reason;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
appdate=mdy(mm,dd,yy);
if value_coded in(1548) then death_reason='health issues';
if value_coded in(1571) then death_reason='suicide';
if value_coded in(1572) then death_reason='murder';
if value_coded in(1593) then death_reason='patient died';
if value_coded in(5622) then death_reason='other';
if value_coded in(84) then death_reason='accident';
drop obs_datetime dd mm yy value_coded concept_id;
run;

proc sort data=death_reason1 out=death_reason_final nodupkey; by person_id appdate death_reason;run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE death_reason death_reason1;
		RUN;
   		QUIT;


%Mend create_death_reason;
%create_death_reason;
