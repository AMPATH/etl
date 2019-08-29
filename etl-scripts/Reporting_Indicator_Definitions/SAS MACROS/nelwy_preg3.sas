

%GLOBAL STARTDATE ENDDATE period ;

%LET STARTDATE=MDY(11,01,2011);

%LET ENDDATE=MDY(11,30,2011);

libname n 'C:\DATA\REPORTS\nesh problems';

%INCLUDE 'C:\DATA\SAS MACROS\Create_encounters.sas';
%INCLUDE 'C:\DATA\SAS MACROS\Create_person.sas';
%Include 'C:\DATA\SAS MACROS\create_person_voided.sas';
%INCLUDE 'C:\DATA\SAS MACROS\create_pregnant_current.sas';/*pass through starttransfer*/
%Include 'C:\DATA\SAS MACROS\Create_location v6.sas';/*pass through starttransfer*/
%INCLUDE 'C:\DATA\SAS MACROS\create_arvs7.sas';
%INCLUDE 'C:\DATA\SAS MACROS\Create_identifiers3.sas';

proc sort data=Identifier_final (drop=location_id)nodupkey out=identifier; by person_id; run;
proc sort data=Encounters_final ; by person_id; run;

data Encounter_final2;
merge Encounters_final(in=a WHERE=(encounter_date<=&ENDDATE and encounter_type in(1,2,3,4,10,13,14,15,17,19,22,23,26,47) ))
Person_voided_final(in=b);
by person_id;
if a and not b;
if person_id=89895 and encounter_date=mdy(03,12,2009) then do; location_id=3;end;
if person_id=103218 and encounter_date=mdy(07,31,2009) then do;location_id=71;end;
if person_id=179514 and encounter_date=mdy(02,26,2010) then do;location_id=71;end;
if person_id=174420 and encounter_date=mdy(02,04,2010) then do; location_id=65;end;

/*some clean-ups on test patients*/
if person_id in(3998,4545,16584,17850,20473,36477,43362,59604,60644,95788,130084,130559,
170740,246371,249546,262981,29673,35968,311,48609,52516,52517,52518,53894,54493,59502,73806,76124,80775,
87398,87753,87754,87755,87756,88936,89364,48609,90486,90510,90527,90668,97484,97485,106185,107592,110064,
119894,120619,133790,133956,133980,139870,139881,154991,145992,147884,147886,148173,148176,148177,152885,
160857,164428,166137,176996,183275,202983,202984,202985,202986,202983,204495,209564,209569,216881,225497,
229567,229569,241077,241305,241780,251408,145991,266518,266520,266523,270463,12800,31467,283196,83860,
70692,58826,6454,10545,17938,20757,25181,26285,26672,30234,32441,34999,36504,38172,38387,39636,39723,324444,
386322,145991,79795,402332,312609,395837,409240,405264,402330,405264,402330)then delete;
/*some clean-ups on wrong encounter entries*/
if person_id=277444 and encounter_type in(1,2,3,4) and encounter_date=mdy(05,17,2011) then delete;
run;


proc sort data=Encounter_final2 nodupkey ;by person_id encounter_date;run;
data first_location(keep=person_id location_id);
set Encounter_final2(where=(encounter_date<=&enddate and location_id ne .));
by person_id encounter_date;
if first.person_id;
run;

proc  sort data=first_location; by location_id;
data location_enrolled_use(keep=person_id site location location_id rename=(site=site_enrolled location=location_enrolled));
merge first_location(in=a) Location_final(drop=site rename=HealthCentre=site);
by location_id;
if a;
run;


data Encounter_final3;
set Encounter_final2;
by person_id;
retain no_visits ;
if first.person_id then do;
no_visits=0;
end;
no_visits=no_visits+1;
run;


data Encounter_visits(rename=(no_visits=last_no_visits)) Encounter_visit2(rename=(encounter_date=sec_app_date));
set Encounter_final3;
by person_id;
if last.person_id  then output Encounter_visits;
if no_visits=2 and last.person_id then output Encounter_visit2;
keep person_id encounter_date no_visits;
run;

/*Extracting Outreach encouters type 21*/
data Outreach_enc(rename=(encounter_date=Outreach_date));
set Encounters_final;
by person_id;
if encounter_type=21;
Outreach_enc='Yes';
keep Outreach_enc person_id encounter_date;
run;

proc sort data=Encounter_visits ; by person_id ; run;
proc sort data=Encounter_visit2 ; by person_id ; run;
proc sort  data=Encounter_final3 ; by person_id; run;
proc sort  data=Outreach_enc ; by person_id; run;

data Encounter_final4;
merge Encounter_final3(in=a) Encounter_visits(in=b) Encounter_visit2(in=c) Outreach_enc(in=d);
by person_id;
if a;
run;

/* Limiting  encounters prior to end of reporting period           */
data  Encounter_final1 ;
set Encounter_final2(rename=encounter_date=app_date);
if app_date<=&enddate;
run;

 

data latest_enc;
set Encounter_final1;
drop encounter_type  encounter_id;
/*delete test patients*/
run;

proc sort  data=latest_enc ; by  person_id descending app_date   ; run;

/* Take  most frequent 3 location      */

data encount_locc;
set latest_enc;
count + 1;
by person_id;
if first.person_id then count = 1;
if count<=3;
run;

proc freq data=encount_locc noprint;
table location_id/ out=freqlocation;
by person_id;
run;

proc sort data=freqlocation; by person_id COUNT; run;
proc sort data=location_final; by location_id; run;

data highcount;
set freqlocation;
by person_id COUNT;
if last.person_id;
keep person_id location_id;
run;


proc sort data=highcount;by location_id;
run;


data location_Active_use(keep=person_id  location_id site location rename=(site=site_Active location=location_Active));
merge highcount(in=a) location_final(drop=site rename=HealthCentre=site);
by location_id;
if a;
run;

proc freq data=location_Active_use;
table site_Active;
run;


/* Getting Newly pregnant women */


data first_pregant(rename=pregdate=first_pregdate);
set pregnant__current_final(where=(pregdate<=&enddate));
by person_id pregdate;
if first.person_id;
run;


data last_pregant(rename=pregdate=last_pregdate);
set pregnant__current_final(where=(pregdate<=&enddate));
by person_id pregdate;
if last.person_id;
run;

proc sort data=location_enrolled_use;by person_id;
Data newly_pregnant_positive(keep=person_id first_pregdate);
merge first_pregant(in=a) location_enrolled_use(in=b);
by person_id;
if a and b and &STARTDATE<=first_pregdate<=&ENDDATE;
run;

data preg_current;
set pregnant__current_final;
if &startdate<=pregdate<=&enddate;
run;

proc sort data=preg_current nodupkey;by person_id; run;

Data current_pregnant_positive(keep=person_id first_pregdate last_pregdate);
merge preg_current(in=a) location_enrolled_use(in=b) 
newly_pregnant_positive(keep=person_id in=c) first_pregant(keep=person_id first_pregdate) 
last_pregant(keep=person_id last_pregdate);
by person_id;
if (a and b) and not c;
if int((last_pregdate-first_pregdate)/30.45)<=2 then delete;;
run;
/* applying LAG*/
data pregnant_current_all(keep=person_id pregdate);
format lagdate date9.;
merge current_pregnant_positive(in=a) pregnant__current_final;
by person_id;
if a;
lagpersonid=lag(person_id);
lagdate=lag(pregdate);
if person_id ne lagpersonid then lagdate=.;
difference=int((pregdate-lagdate)/30.45);
if difference=>9 and &STARTDATE<=pregdate<=&ENDDATE ;
run;

proc sort data=pregnant_current_all out=newpregnancy(rename=pregdate=first_pregdate) nodupkey; by person_id; run;

data new_pregnant_use;
set newly_pregnant_positive newpregnancy;
run;

data arvs_current(keep=person_id arv_date onarv);
set arv_final(where=(onarv=1 and &startdate<=arv_date<=&enddate));
If person_id=7270 and arv_date=mdy(05,18,2006) then delete; If person_id=36608 and arv_date=mdy(10,05,2006) then delete;
If person_id=43895 and arv_date=mdy(02,12,2007) then delete;If person_id=55158 and arv_date=mdy(07,09,2007) then delete;
If person_id=54488 and arv_date=mdy(07,04,2007) then delete;If person_id=16963 and arv_date=mdy(08,06,2006) then delete;
If person_id=59229 and arv_date=mdy(09,06,2007) then delete;If person_id=69868 and arv_date=mdy(02,05,2008) then delete;
If person_id=32606 and arv_date=mdy(10,22,2007) then delete;If person_id=71546 and arv_date=mdy(02,04,2008) then delete;
If person_id=78110 and arv_date=mdy(08,07,2008) then delete;If person_id=79955 and arv_date=mdy(09,17,2008) then delete;
If person_id=85105 and arv_date=mdy(12,03,2008) then delete;If person_id=84741 and arv_date=mdy(12,16,2008) then delete;
If person_id=84200 and arv_date=mdy(12,08,2008) then delete;If person_id=84185 and arv_date=mdy(02,04,2009) then delete;
If person_id=81120 and arv_date=mdy(12,05,2008) then delete;If person_id=85321 and arv_date=mdy(12,16,2008) then delete;
If person_id=144354 and arv_date=mdy(04,23,2009) then delete;If person_id=96474 and arv_date=mdy(05,20,2009) then delete;
If person_id=93537 and arv_date=mdy(06,15,2009) then delete;If person_id=90447 and arv_date=mdy(03,24,2009) then delete;
If person_id=89744 and arv_date=mdy(03,13,2009) then delete;If person_id=103131 and arv_date=mdy(07,30,2009) then delete;
If person_id=97791 and arv_date=mdy(07,07,2009) then delete;If person_id=80492 and arv_date=mdy(01,26,2009) then delete;
If person_id=91310 and arv_date=mdy(04,08,2009) then delete;If person_id=112946 and arv_date=mdy(09,24,2009) then delete;
If person_id=84619 and arv_date=mdy(11,20,2008) then delete;If person_id=89658 and arv_date=mdy(03,26,2009) then delete;
If person_id=117932 and arv_date=mdy(07,24,2009) then delete;If person_id=90020 and arv_date=mdy(03,10,2009) then delete;
If person_id=139135 and arv_date=mdy(11,18,2009) then delete;If person_id=95717 and arv_date=mdy(06,03,2009) then delete;
If person_id=76929 and arv_date=mdy(11,12,2009) then delete;If person_id=144356 and arv_date=mdy(12,16,2009) then delete;
If person_id=84657 and arv_date=mdy(12,16,2008) then delete;If person_id=77244 and arv_date=mdy(08,06,2008) then delete;
If person_id=159621 and arv_date=mdy(01,25,2010) then delete;If person_id=153367 and arv_date=mdy(12,17,2009) then delete;
If person_id=106818 and arv_date=mdy(12,10,2009) then delete;If person_id=200354 and arv_date=mdy(04,12,2010) then delete;
If person_id=189985 and arv_date=mdy(03,17,2010) then delete;If person_id=103036 and arv_date=mdy(08,16,2010) then delete;
If person_id=107781 and arv_date=mdy(09,04,2009) then delete;If person_id=185625 and arv_date=mdy(03,17,2010) then delete;
If person_id=130058 and arv_date=mdy(10,27,2009) then delete;If person_id=203577 and arv_date=mdy(04,20,2010) then delete;
If person_id=220767 and arv_date=mdy(12,15,2010) then delete;If person_id=207594 and arv_date=mdy(04,26,2010) then delete;
If person_id=207247 and arv_date=mdy(04,19,2010) then delete;If person_id=228489 and arv_date=mdy(04,26,2010) then delete;
If person_id=83855 and arv_date=mdy(11,17,2008) then delete;If person_id=226845 and arv_date=mdy(05,25,2010) then delete;
If person_id=223607 and arv_date=mdy(05,26,2010) then delete;If person_id=241951 and arv_date=mdy(07,15,2010) then delete;
If person_id=247052 and arv_date=mdy(07,23,2010) then delete;If person_id=246716 and arv_date=mdy(07,26,2010) then delete;
If person_id=233926 and arv_date=mdy(06,24,2010) then delete;If person_id=233964 and arv_date=mdy(06,24,2010) then delete;
If person_id=254366 and arv_date=mdy(08,03,2010) then delete;If person_id=252204 and arv_date=mdy(07,28,2010) then delete;
If person_id=259290 and arv_date=mdy(08,25,2010) then delete;If person_id=264876 and arv_date=mdy(09,10,2010) then delete;
If person_id=221478 and arv_date=mdy(06,15,2010) then delete;If person_id=297041 and arv_date=mdy(10,08,2010) then delete;

If person_id=14810 and arv_date=mdy(09,28,2000) then delete;If person_id=34331 and arv_date=mdy(10,02,2006) then delete;
If person_id=42622 and arv_date=mdy(01,30,2007) then delete;If person_id=47252 and arv_date=mdy(02,27,2007) then delete;
If person_id=89744 and arv_date=mdy(04,10,2009) then delete;If person_id=96474 and arv_date=mdy(06,02,2009) then delete;
If person_id=97750 and arv_date=mdy(05,02,2008) then delete;If person_id=103036 and arv_date=mdy(03,16,2010) then delete;
If person_id=107688 and arv_date=mdy(08,26,2009) then delete;If person_id=130058 and arv_date=mdy(11,03,2009) then delete;
If person_id=139135 and arv_date=mdy(12,02,2009) then delete;If person_id=153367 and arv_date=mdy(01,28,2010) then delete;
If person_id=167359 and arv_date=mdy(02,04,2010) then delete;If person_id=182918 and arv_date=mdy(04,12,2010) then delete;
If person_id=183955 and arv_date=mdy(02,23,2010) then delete;If person_id=192110 and arv_date=mdy(01,06,2011) then delete;

If person_id=221478 and arv_date=mdy(06,15,2010) then delete;If person_id=297041 and arv_date=mdy(10,08,2010) then delete;
If person_id=200810 and arv_date=mdy(04,20,2010) then delete;If person_id=202697 and arv_date=mdy(03,31,2010) then delete;
If person_id=220633 and arv_date=mdy(05,11,2010) then delete;If person_id=234146 and arv_date=mdy(06,22,2011) then delete;
If person_id=239249 and arv_date=mdy(07,02,2010) then delete;If person_id=246998 and arv_date=mdy(08,25,2011) then delete;
If person_id=252204 and arv_date=mdy(08,26,2010) then delete;If person_id=270021 and arv_date=mdy(07,18,2011) then delete;
If person_id=286497 and arv_date=mdy(11,08,2010) then delete;If person_id=298122 and arv_date=mdy(12,07,2010) then delete;
If person_id=300822 and arv_date=mdy(12,10,2011) then delete;If person_id=303355 and arv_date=mdy(12,14,2010) then delete;
If person_id=306750 and arv_date=mdy(01,13,2011) then delete;If person_id=306764 and arv_date=mdy(12,15,2010) then delete;
If person_id=309451 and arv_date=mdy(01,07,2011) then delete;If person_id=310687 and arv_date=mdy(01,11,2011) then delete;
If person_id=313725 and arv_date=mdy(01,18,2011) then delete;If person_id=328315 and arv_date=mdy(02,15,2011) then delete;
If person_id=331209 and arv_date=mdy(11,29,2010) then delete;If person_id=333494 and arv_date=mdy(03,09,2011) then delete;
If person_id=336757 and arv_date=mdy(03,14,2011) then delete;If person_id=339265 and arv_date=mdy(03,17,2011) then delete;
If person_id=345108 and arv_date=mdy(03,29,2011) then delete;If person_id=345829 and arv_date=mdy(04,05,2011) then delete;
If person_id=355789 and arv_date=mdy(04,28,2011) then delete;If person_id=357948 and arv_date=mdy(05,10,2011) then delete;
If person_id=360919 and arv_date=mdy(05,10,2011) then delete;If person_id=363861 and arv_date=mdy(05,18,2011) then delete;
If person_id=364742 and arv_date=mdy(03,07,2011) then delete;If person_id=365889 and arv_date=mdy(05,18,2011) then delete;
If person_id=367575 and arv_date=mdy(04,06,2011) then delete;If person_id=372832 and arv_date=mdy(06,13,2011) then delete;
If person_id=373771 and arv_date=mdy(10,26,2010) then delete;If person_id=380404 and arv_date=mdy(06,16,2011) then delete;
If person_id=385074 and arv_date=mdy(07,17,2011) then delete;If person_id=385148 and arv_date=mdy(07,04,2011) then delete;
If person_id=386839 and arv_date=mdy(08,05,2011) then delete;If person_id=387799 and arv_date=mdy(08,10,2011) then delete;
If person_id=390445 and arv_date=mdy(08,24,2011) then delete;If person_id=390849 and arv_date=mdy(07,20,2011) then delete;
If person_id=407595 and arv_date=mdy(09,01,2011) then delete;If person_id=407634 and arv_date=mdy(08,25,2011) then delete;
If person_id=407910 and arv_date=mdy(09,09,2011) then delete;If person_id=409442 and arv_date=mdy(08,30,2011) then delete;
If person_id=411374 and arv_date=mdy(09,30,2011) then delete;If person_id=413111 and arv_date=mdy(06,14,2011) then delete;
If person_id=414718 and arv_date=mdy(11,17,2010) then delete;If person_id=424026 and arv_date=mdy(11,17,2011) then delete;
If person_id=435152 and arv_date=mdy(08,23,2011) then delete;
run;

proc sort data=new_pregnant_use nodupkey; by person_id;run;
proc sort data=arvs_current nodupkey; by person_id;run;
proc sort data=Location_active_use nodupkey; by person_id;run;

data pregnant_onARVs;
merge new_pregnant_use(in=a) arvs_current Location_active_use;
by person_id;
if a;
if onarv=1 then pregnant_onARVs='Newly Preg OnARV    ';
else pregnant_onARVs='Newly Preg_NoTARVs';
run;

data n.november_final;
set pregnant_onARVs;
run;


ods rtf;
proc freq data=pregnant_onARVs  ;
title 'November 2012';
table site_Active*pregnant_onARVs/norow nopercent nocol;
run;
ods rtf close;



proc freq data=pregnant_onARVs  ;
table pregnant_onARVs/nopercent nocum;
run;

