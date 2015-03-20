


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_pointofentry();


Libname tmp 'C:\DATA\DATA SETS';



PROC IMPORT OUT= WORK.point 
            DATAFILE= "C:\DATA\DATA SETS\point_of_entry_name.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

/*data arv;
format arv_date ddmmyy10.;
set tmp.arvs;
*dd=substr(date_obs_datetime_,9,2);
*mm=substr(date_obs_datetime_,6,2);
*yy=substr(date_obs_datetime_,1,4);
*arv_date=mdy(mm,dd,yy);
arv_date=date_obs_datetime_;


/* convert the person_id for those on ARV to be as though they are for the parents...
this is to help us merge this with the parent_baby pairs*/


proc sort nodupkey data=point out=pointofentry_final; by person_id;run;



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE point ;
		RUN;
   		QUIT;

	

%MEND create_pointofentry;

%create_pointofentry;
