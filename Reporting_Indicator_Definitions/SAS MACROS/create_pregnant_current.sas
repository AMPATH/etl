

PROC OPTIONS OPTION = MACRO; RUN;

%MACRO create_pregnant_current();	
/*
PROC IMPORT OUT= WORK.pregnant_current 
            DATAFILE= "C:\DATA\CSV DATASETS\pregnant_current.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;
*/
Libname tmp 'C:\DATA\CSV DATASETS'; 
/*extracting encounters*/

PROC IMPORT OUT= WORK.encounters1 
            DATAFILE= "C:\DATA\CSV DATASETS\encounters1.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


PROC IMPORT OUT= WORK.encounters2 
            DATAFILE= "C:\DATA\CSV DATASETS\encounters2.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;

/*Renaming/recoding formid into numeric*/
data enc1;
set encounters1;
formid=form_id*1;
run;

data enc2;
set encounters2;
formid=form_id*1;
run;

/*Merging all the encounters*/

data encounters1a (rename=patient_id=person_id);
format encounter_date ddmmyy10.;
set enc1(drop=form_id) enc2(drop=form_id);
dd=substr(encounter_datetime,9,2); 
mm=substr(encounter_datetime,6,2); 
yy=substr(encounter_datetime,1,4); 
encounter_date=mdy(mm,dd,yy);
drop dd mm yy encounter_datetime encounter_id location_id provider_id;
run;


/*Pregnant with delivery dates*/
data preg(drop=concept_id obs_datetime);
format del_date pregdate ddmmyy10.;
set tmp.pregnant_current;
if value_datetime ne '';
dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
dd1=substr(obs_datetime,9,2);
mm1=substr(obs_datetime,6,2);
yy1=substr(obs_datetime,1,4);
pregdate=mdy(mm1,dd1,yy1);
del_date=mdy(mm,dd,yy);
drop dd mm yy value_datetime dd1 mm1 yy1;
run;

/*Defining the pregnant concepts*/
data pregstatus(keep=person_id pregstatus pregdate)
probpreg (keep=person_id probpreg pregdate)
testpreg (keep=person_id testpreg pregdate)
durpreg (keep=person_id durpreg pregdate)
fundpreg (keep=person_id fundpreg pregdate)
reasnvispreg (keep=person_id reasnvispreg pregdate)
dangerpreg (keep=person_id dangerpreg pregdate)
ancpreg (keep=person_id ancpreg pregdate);
Format /*pregnant $3.*/ pregdate ddmmyy10.;
set tmp.pregnant_current;
if value_datetime='';
dd=substr(obs_datetime,9,2);
mm=substr(obs_datetime,6,2);
yy=substr(obs_datetime,1,4);
pregdate=mdy(mm,dd,yy);
if concept_id=5272  then pregstatus='yes';
if pregstatus ne '' then output pregstatus;
if concept_id in(6042,1790)  then probpreg='yes';
if probpreg ne '' then output probpreg;
if concept_id in (45,1856)  then testpreg='yes';
if testpreg ne '' then output testpreg;
if concept_id in(1279,5992)  then durpreg='yes';
if durpreg ne '' then output durpreg;
if concept_id in(1855)  then fundpreg='yes';
if fundpreg ne '' then output fundpreg;
if concept_id in(1835,1834)  then reasnvispreg='yes';
if reasnvispreg ne '' then output reasnvispreg;
if concept_id in(2298)  then dangerpreg='yes';
if dangerpreg ne '' then output dangerpreg;
if concept_id in(2055)  then ancpreg='yes';
if ancpreg ne '' then output ancpreg;
run;


proc sort data=pregstatus nodupkey;by person_id pregdate; run;
proc sort data=preg nodupkey; by person_id pregdate del_date; run;
proc sort data=probpreg nodupkey;by person_id pregdate; run;
proc sort data=testpreg nodupkey;by person_id pregdate; run;
proc sort data=durpreg nodupkey;by person_id pregdate; run;
proc sort data=fundpreg nodupkey;by person_id pregdate; run;
proc sort data=reasnvispreg nodupkey;by person_id pregdate; run;
proc sort data=dangerpreg nodupkey;by person_id pregdate; run;
proc sort data=ancpreg nodupkey;by person_id pregdate; run;


data preg1;
merge pregstatus(in=a) preg(in=b) probpreg testpreg dangerpreg reasnvispreg
durpreg fundpreg ancpreg;
by person_id pregdate;
run;


proc sort data=encounters1a ; by person_id encounter_date ; run;
proc sort data=preg1 ; by person_id pregdate ; run;

data enc_preg245 enc_preg248 enc_pregothers;
merge encounters1a(rename=encounter_date=pregdate) preg1(in=a);
by person_id pregdate;
if a and formid not in(245,248) then output enc_pregothers;
else if a and formid in(245) then output enc_preg245;
else if a and formid in(248) then output enc_preg248;
run;
 

data preg_use(keep=person_id pregdate pregnant) 
deleted;
merge enc_pregothers(rename=formid=otherenc) enc_preg248(rename=formid=pmtctenc) enc_preg245(rename=formid=newenc);
pregnant='Yes';
by person_id pregdate;
if pregstatus='yes' and newenc=245 and otherenc eq . and del_date eq . and probpreg eq '' and testpreg eq '' and testpreg eq . then output deleted ;
else output preg_use;

run;

proc sort data=preg_use;by person_id;run;
proc sort data=Person_final out=Person_final1(keep=person_id gender);by person_id;run;

data preg_use1(where=(gender='F'));
merge preg_use(in=a)  Person_final1(in=b);
by person_id;
if a;
run;


proc sort data=preg_use1 nodupkey out=pregnant__current_final(drop=gender); by person_id pregdate; run;


/*
data test;
set pregnant__current_final;
if &startdate <=pregdate<=&enddate;
run;
proc sort data=test  nodupkey; by person_id ; run;
*/
PROC DATASETS LIBRARY=WORK NOLIST;
DELETE pregnant pregnant_current Person_final1 preg preg1 enc_preg_new enc_preg_old enc_preg_old1 encounters1a encounters encounters1
encounters2 preg1_pmtct pmtct_enc preg_use1 preg_use;
		RUN;
   		QUIT;



%MEND create_pregnant_current;


%create_pregnant_current;
