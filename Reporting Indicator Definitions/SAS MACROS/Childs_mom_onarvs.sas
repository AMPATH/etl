


PROC OPTIONS OPTION = MACRO; RUN;


%MACRO Childs_mom_onarvs();

PROC IMPORT OUT= WORK.mom_arvs
            DATAFILE= "C:\DATA\CSV DATASETS\Childs_Mom_onARVs.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


/*MOTHER ON ANTIRETROVIRAL DRUGS (2198)
YES (1065)
NO (1066) */

data mom_arvs1;
Format apptdate ddmmyy10.;
set mom_arvs;
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
apptdate=mdy(mm,dd,yy);
drop dd mm yy order_id encounter_id--value_boolean value_coded_name_id--uuid;
run;

data mom_on_arvs(keep=person_id obs_id apptdate mom_on_arvs) ;
set mom_arvs1(where=(concept_id=2198));
if value_coded=1065 then mom_on_arvs=1;
else if value_coded=1066 then mom_on_arvs=0;
run;

/*  	PARTUM ANTIRETROVIRAL USE(1176)
 	 OTHER NON-CODED (5622)
STAVUDINE (625)
d4T-30 (STAVUDINE) (625)
d4T-40 (STAVUDINE) (625)
LAMIVUDINE (628)
NEVIRAPINE (631)
NELFINAVIR (635)
LOPINAVIR AND RITONAVIR (794)
ZIDOVUDINE (797)*/

data STAV(keep=person_id obs_id apptdate STAVUDINE)
LAMI(keep=person_id obs_id apptdate LAMIVUDINE)
NEVI(keep=person_id obs_id apptdate NEVIRAPINE)
NELFI(keep=person_id obs_id apptdate NELFINAVIR)
LOPIRITO(keep=person_id obs_id apptdate LOPINAVIR RITONAVIR)
ZIDO(keep=person_id obs_id apptdate ZIDOVUDINE) ;
set mom_arvs1(where=(concept_id=1176));

if value_coded=625 then STAVUDINE=1;
IF STAVUDINE NE . THEN OUTPUT STAV;


if value_coded=628 then LAMIVUDINE=1;
IF LAMIVUDINE NE . THEN OUTPUT LAMI;


if value_coded=631 then NEVIRAPINE=1;
IF NEVIRAPINE NE . THEN OUTPUT NEVI ;

if value_coded=635 then NELFINAVIR=1;
IF NELFINAVIR NE . THEN OUTPUT NELFI;

if value_coded=794 then LOPINAVIR=1;
if value_coded=794 then RITONAVIR=1 ;
IF LOPINAVIR NE . and RITONAVIR ne . THEN OUTPUT LOPIRITO;

if value_coded=797 then ZIDOVUDINE=1;
IF ZIDOVUDINE NE . THEN OUTPUT ZIDO;
run;

/* 	 
ANTIRETROVIRAL DOSE QUANTIFICATION(1181)
Describes the quantity, or style of antiretroviral drug use

TOTAL MATERNAL TO CHILD TRANSMISSION PROPHYLAXIS (1148)
ONE DOSE (1182)
TWO DOSES (1183)
MORE THAN TWO DOSES (1184)
TREATMENT (1185)
MORE THAN OR EQUAL TO TWO DOSES (2168)

*/

data T_PMTCT(keep=person_id obs_id apptdate T_PMTCT)
ONE_DOSE(keep=person_id obs_id apptdate ONE_DOSE)
TWO_DOSES(keep=person_id obs_id apptdate TWO_DOSES)
MANY_DOSES(keep=person_id obs_id apptdate MANY_DOSES)
TREATMENT(keep=person_id obs_id apptdate TREATMENT);
set mom_arvs1(where=(concept_id=1181));

if value_coded=1148 then T_PMTCT=1;
IF T_PMTCT NE . THEN OUTPUT T_PMTCT;

if value_coded=1182 then ONE_DOSE=1;
IF ONE_DOSE NE . THEN OUTPUT ONE_DOSE;

if value_coded=1183 then TWO_DOSES=1;
IF TWO_DOSES NE . THEN OUTPUT TWO_DOSES ;

if value_coded IN(1184,2168) then MANY_DOSES=1;
IF MANY_DOSES NE . THEN OUTPUT MANY_DOSES;

if value_coded=1185 then TREATMENT=1;
IF TREATMENT NE . THEN OUTPUT TREATMENT;
run;




PROC SORT DATA=T_PMTCT; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=ONE_DOSE; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=TWO_DOSES; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=MANY_DOSES; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=TREATMENT; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=mom_on_arvs; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=STAV; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=LAMI; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=NEVI; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=LOPIRITO; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=NELFI; BY PERSON_ID APPTDATE obs_id;
PROC SORT DATA=ZIDO; BY PERSON_ID APPTDATE obs_id;
run;


data T_PMTCT1;
set T_PMTCT;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;


data ONE_DOSE1;
set ONE_DOSE;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;


data TWO_DOSES1;
set TWO_DOSES;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;


data MANY_DOSES1;
set MANY_DOSES;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;


data TREATMENT1;
set TREATMENT;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;



data mom_on_arvs1;
set mom_on_arvs;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;

data STAV1;
set STAV;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;

data LAMI1;
set LAMI;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;

data NEVI1;
set NEVI;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;

data LOPIRITO1;
set LOPIRITO;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;

data NELFI1;
set NELFI;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;

data ZIDO1;
set ZIDO;
BY PERSON_ID APPTDATE obs_id;
if last.person_id;
run;

data momonarvs_final(DROP=obs_id);
merge mom_on_arvs1 STAV1 LAMI1 NEVI1 LOPIRITO1 NELFI1 ZIDO1
T_PMTCT1 ONE_DOSE1 TWO_DOSES1 MANY_DOSES1 TREATMENT1 ; 
by person_id apptdate;
run; 



PROC DATASETS LIBRARY=WORK NOLIST;
		DELETE om_on_arvs STAV LAMI NEVI LOPIRITO NELFI ZIDO
		T_PMTCT ONE_DOSE TWO_DOSES MANY_DOSES TREATMENT 
		mom_on_arvs1 STAV1 LAMI1 NEVI1 LOPIRITO1 NELFI1 ZIDO1
		T_PMTCT1 ONE_DOSE1 TWO_DOSES1 MANY_DOSES1 TREATMENT1
		Lopi Mom_arvs Mom_arvs1 Mom_on_arvs Rito;
		RUN;
   		QUIT;


%MEND Childs_mom_onarvs;

%Childs_mom_onarvs;
