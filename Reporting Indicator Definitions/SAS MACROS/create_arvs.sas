


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_arvs();


Libname tmp 'C:\DATA\CSV DATASETS';

data arv;
format arv_date ddmmyy10.;
set tmp.arvs;
dd=substr(date_obs_datetime_,9,2);
mm=substr(date_obs_datetime_,6,2);
yy=substr(date_obs_datetime_,1,4);
arv_date=mdy(mm,dd,yy);
if value_coded=1256 then do; arv_plan='start drugs           ';end;
else if value_coded=1257 then do; arv_plan='continue regimen';end;
else if value_coded=1258 then do; arv_plan='change formulation';end;
else if value_coded=1259 then do; arv_plan='change regimen';end;
else if value_coded=1260 then do; arv_plan='stop all';end;
else if value_coded=1406 then do; arv_plan='refilled';end;
else if value_coded=1407 then do; arv_plan='not refilled';end;
else if value_coded=1849 then do; arv_plan='drug substitution';end;
else if value_coded=1850 then do; arv_plan='drugs restart';end;
else if value_coded=981 then do; arv_plan='dosing change';end;
drop  value_coded dd mm yy date_obs_datetime_ obs_id;
run;


proc sort data=arv out=arv1 dupout=arvdiff1 nodupkey; by person_id arv_date arv_plan;run;


proc sort data=arv out=arv_final; by person_id arv_date;run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE arv arv1;
		RUN;
   		QUIT;

	

%MEND create_arvs;
%create_arvs;
