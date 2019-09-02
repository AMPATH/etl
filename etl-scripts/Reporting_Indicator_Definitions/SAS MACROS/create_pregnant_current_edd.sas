


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

%Include 'C:\DATA\SAS MACROS\Create_person.sas';/*pass through starttransfer*/

/*Pregnant with expected delivery dates(EDD)*/
data preg(where=(value_datetime ne '' or value_numeric ne .));
format edd edd_vn edd_fh edd_wkmn pregdate ddmmyy10.;
set tmp.pregnant_current1;

dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
dd1=substr(obs_datetime,9,2);
mm1=substr(obs_datetime,6,2);
yy1=substr(obs_datetime,1,4);
pregdate=mdy(mm1,dd1,yy1);
edd_vn=mdy(mm,dd,yy);
if concept_id in (1279,1855) then days=value_numeric*7;
if concept_id=5992 and value_numeric>9 then days=value_numeric*7;else
if concept_id=5992 and value_numeric<=9 then days=value_numeric*30;
if concept_id in (1279) then do; edd_wkmn=(pregdate+(280-days));end;
if concept_id in (5992)  then do; edd_wkmn=(pregdate+(280-days));end;
if concept_id in (1855)  then do; edd_fh=(pregdate+(280-days));end;

if edd_vn ne . then edd=edd_vn;else
if edd_wkmn ne . then edd=edd_wkmn;else
if edd_fh ne . then edd=edd_fh;
drop dd mm yy  dd1 mm1 yy1 obs_datetime;
*drop dd mm yy value_datetime dd1 mm1 yy1 days edd_vn edd_wkmn edd_fh concept_id value_coded value_numeric;
run;

proc sort data=preg;by person_id pregdate;run;


PROC IMPORT OUT= WORK.lmp_dates 
            DATAFILE= "C:\DATA\CSV DATASETS\lmp_dates.csv" 
            DBMS=CSV REPLACE;
     GETNAMES=YES;
     DATAROW=2; 
RUN;


/*---Last LMP Dates---*/
data lmp_dates1;
format pregdate lmp_pregdate lmp_pregdate1 ddmmyy10.;
set lmp_dates;
if concept_id=1836;
dd=substr(value_datetime,9,2);
mm=substr(value_datetime,6,2);
yy=substr(value_datetime,1,4);
dd1=substr(obs_datetime,9,2);
mm1=substr(obs_datetime,6,2);
yy1=substr(obs_datetime,1,4);
pregdate=mdy(mm1,dd1,yy1);
lmp_pregdate1=mdy(mm,dd,yy);
lmp_pregdate=(lmp_pregdate1+(280+7));
drop dd mm yy value_datetime dd1 mm1 yy1 obs_datetime concept_id lmp_pregdate1;
run;

proc sort data=lmp_dates1;by person_id pregdate;run;

/*data lmp_dates_use(rename=);*/
/*set lmp_dates1;*/
/*by person_id;*/
/*if first.person_id;*/
/*run;*/


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
proc sort data=preg nodupkey; by person_id pregdate ; run;
proc sort data=probpreg nodupkey;by person_id pregdate; run;
proc sort data=testpreg nodupkey;by person_id pregdate; run;
proc sort data=durpreg nodupkey;by person_id pregdate; run;
proc sort data=fundpreg nodupkey;by person_id pregdate; run;
proc sort data=reasnvispreg nodupkey;by person_id pregdate; run;
proc sort data=dangerpreg nodupkey;by person_id pregdate; run;
proc sort data=ancpreg nodupkey;by person_id pregdate; run;

/*--Pregnancy on last LMP dates*/
data lmp_pregdates;
merge lmp_dates1(in=a) pregstatus(in=b);
by person_id pregdate;
if b and (lmp_pregdate-pregdate)<=280;
*diff=(lmp_pregdate-pregdate);
*if 0<=diff<=300  then delete;
drop pregstatus ;*concept_id value_coded value_numeric ;
run;


data preg1;
format edd1 ddmmyy10.;
merge pregstatus(in=a) preg(in=b) probpreg testpreg dangerpreg reasnvispreg
durpreg fundpreg ancpreg lmp_pregdates;
by person_id pregdate;
if edd1 eq . and edd ne . then edd1=edd;
run;


proc sort data=encounters1a ; by person_id encounter_date ; run;
proc sort data=preg1 ; by person_id pregdate ; run;

data enc_preg245 enc_preg248 enc_pregothers;
merge encounters1a(in=b rename=(encounter_date=pregdate)) preg1(in=a);
by person_id pregdate;
if a and b and formid not in(245,248) then output enc_pregothers;
else if a and formid in(245) then output enc_preg245;
else if a and formid in(248) then output enc_preg248;
run;
 

data preg_use(keep=person_id pregdate pregnant edd1) 
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
merge preg_use(in=a)  lmp_pregdates(in=c) Person_final1(in=d);
by person_id;
if a ;
if (lmp_pregdate-pregdate<=300 and edd1 eq . and lmp_pregdate ne .) then edd1=lmp_pregdate;
if pregdate>edd1 and lmp_pregdate ne . then do;edd1=lmp_pregdate;end;
difedd1=(edd1-pregdate);
difpregdate=(lmp_pregdate-pregdate);
if difedd1>300 or difedd1<0 then do;edd1=.;end;
if difpregdate>300 or difpregdate<0 then do;lmp_pregdate=.;end;
if lmp_pregdate ne . and edd1 eq . then edd1=lmp_pregdate;
if lmp_pregdate ne . and lmp_pregdate ne edd1 then do;edd1=lmp_pregdate;end;
run;

proc sort data=preg_use1(drop=difedd1 difpregdate gender);by person_id pregdate; run;
proc sort data=encounters1a(rename=(encounter_date=pregdate));by person_id pregdate; run;

data preg_use1a;
merge preg_use1(in=a) encounters1a(in=b keep=person_id pregdate);
by  person_id pregdate;
if a ;
run;

proc sort data=preg_use1a nodupkey out=preg_episode_use(rename=(edd1=edd lmp_pregdate=lmp_edddate )); by person_id pregdate; run;

/* Retaining to fill in missing edd Dates*/

proc sort data=preg_episode_use  out=preg_episode_use1; by person_id descending pregdate; run;

 /* retaining in descending*/
data retain_preg(drop= edd_x);
set preg_episode_use1;
by person_id ;

retain edd_x;
if first.person_id then do ; edd_x=edd;end;
else do ;
if missing(edd) then do ;edd=edd_x;

end;else do;
	edd_x=edd;
  end;
end;
run;


proc sort data =retain_preg  ;by person_id pregdate;run;

 /* reining in ascending */
data retain_asc_preg(drop= edd_x);
set retain_preg;
by person_id ;

retain edd_x;
if first.person_id then do ; edd_x=edd;end;
else do ;
if missing(edd) then do ;edd=edd_x;

end;else do;
	edd_x=edd;
  end;
end;
run;

/*======= end retain =====================================*/

proc sort data=retain_asc_preg;by person_id pregdate;run;
proc sort data=preg1;by person_id pregdate;run;

data preg_current_use(drop=edd_fh edd_wkmn);
merge retain_asc_preg(in=a) preg1(in=b keep=person_id  edd_fh edd_wkmn pregdate);
by person_id pregdate;
if a and b;
if edd eq . and edd_wkmn ne . then edd=edd_wkmn;else
if edd eq . and edd_fh ne . then edd=edd_fh;
run;


data preg_current_use1;
set preg_current_use;
if pregdate-edd>=28 then edd=.;/*Visits after 21 days of edd is a false edd*/
run;


proc sort data=preg_current_use1 nodupkey out=pregnant_current_edd_final; by person_id pregdate; run;


PROC DATASETS LIBRARY=WORK NOLIST;
DELETE pregnant pregnant_current Person_final1 preg preg1 enc_preg_new enc_preg_old enc_preg_old1 encounters1a encounters encounters1
encounters2 preg1_pmtct pmtct_enc preg_use1 preg_use pregstatus testpreg lmp_dates1 lmp_pregdates fundpreg deleted dangerpreg ancpreg enc1 enc2 
enc_preg245 enc_preg248 enc_pregothers probpreg reasnvispreg durpreg preg_use1a;
		RUN;
   		QUIT;



%MEND create_pregnant_current;


%create_pregnant_current;
