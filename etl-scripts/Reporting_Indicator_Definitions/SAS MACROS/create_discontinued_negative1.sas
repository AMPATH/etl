


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO create_discontinued_negative();	

PROC IMPORT OUT= WORK.discontinued1 
            DATAFILE= "C:\DATA\CSV DATASETS\disccontinue_hiv_neg1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


/*data disc_n;*/
/*format disc_date1 ddmmyy10.;*/
/*set discontinued;*/
/*/*dd=substr(obs_datetime,9,2);*/*/
/*/*mm=substr(obs_datetime,6,2);*/*/
/*/*yy=substr(obs_datetime,1,4);*/*/
/*/*disc_date1=mdy(mm,dd,yy);*/*/
/*result='Hiv Negative';*/
/*keep person_id disc_date1 result;*/
/*rename disc_date1=disc_date ;*/
/**if disc_date ne '';*/
/*run;

data disc_n;
set discontinued;
result='Hiv Negative';
keep person_id obs_datetime result;
rename obs_datetime=disc_date ;
*if disc_date ne '';
run;


/*To merge discontinuation tables above */

proc sort data=disc_n out=disco_final1 nodupkey ; by person_id  ; run;


PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE disco1 disc_n1 disc_n discontinued1 discontinued ;
		RUN;
   		QUIT;


%MEND create_discontinued_negative;
%create_discontinued_negative;
