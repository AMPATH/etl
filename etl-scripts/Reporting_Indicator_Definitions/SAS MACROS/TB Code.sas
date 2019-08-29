libname Tb'C:\DATA\REPORTS\BUSIA  TB Monthly';

%Global enddate startdate   ;

%let startdate=mdy(01,01,2010); /*THIS IS THE BEGINING OF THE REPORTING PERIOD */
%let enddate=mdy(01,31,2010);   /*THIS IS THE END OF THE REPORTING PERIOD      */
 

%Include 'C:\DATA\REPORTS\BUSIA  TB Monthly\create_encounters.sas';
%Include 'C:\DATA\REPORTS\BUSIA  TB Monthly\Create_person.sas';
%Include 'C:\DATA\REPORTS\BUSIA  TB Monthly\create_tb.sas';

proc sort data=encounter_final;by person_id;run;
data firstEnc;
set encounter_final;
by  person_id app_date;
if first.person_id;
firstDate=app_date;
if location_id in('19','26','27','81','55');/*Busia site only*/
format firstDate date9.;
keep firstDate person_id ptype location_id;
run;

proc sort data=firstenc;by person_id;run;

data TBonly;
set Tb_final;
if tb=1;
keep tb tb_date person_id;
run;
proc sort data=TBonly;by person_id;run;


data firstTB;
set tbonly;
by  person_id tb_date;
if first.person_id;
firstTBDate=tb_date;
format firstTBDate date9.;
keep firstTBDate person_id ;
run;
proc sort data=firstTB;by person_id;run;
proc sort data=person_final;by person_id;Run;

data enctb;
merge firstEnc(in=A) firstTB person_final;
by person_id;
if a;
if firsttbdate> firstdate;
if &startdate<=firsttbdate<=&enddate;
month=month(firsttbdate);
if location_id in('19','26','27','81') then clinic='Busia clinic';
else if  location_id ='81' then clinic='Busia prison';
else if  location_id ='55' then clinic='Bumala A';
count=1;
run;

data montlyTB;
set enctb;
if month=1 then monthOfyear='Jananuary 2010'     ;
else if  month=2 then monthOfyear='February 2010';
else if  month=3 then monthOfyear='March 2010';
else if  month=4 then monthOfyear='April 2010';
else if  month=5 then monthOfyear='May 2010';
else if  month=6 then monthOfyear='June 2010';
else if  month=7 then monthOfyear='July 2010';
else if  month=8 then monthOfyear='August 2010';
else if  month=9 then monthOfyear='September 2010';
else if  month=10 then monthOfyear='October 2010';
else if  month=11 then monthOfyear='November 2010';
else if  month=12 then monthOfyear='December 2010';
run;


proc sort data=montlyTB;by month;run;

proc freq data= montlyTB;table clinic*gender* ptype*month/ norow nocol nopercent  out=xfinal ;by month;run;



OPTIONS NOXWAIT NOXSYNC;
/* Invoke Microsoft Excel. */
X "C:\Program Files\Microsoft Office\OFFICE11\EXCEL.EXE";


data _null_;
rc = sleep(2);
run;



/*TB template  SPITTER*/


/*JAN*/
/*peads*/
filename ddedata dde 'excel|TB_output!R5C2:R5C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds' and month=1 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R5C3:R5C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=1 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R5C4:R5C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=1 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R5C5:R5C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=1 ;
put count ;
run;

/*BumalaA*/

filename ddedata dde 'excel|TB_output!R5C6:R5C6';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds' and month=1 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R5C7:R5C7';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=1 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R5C8:R5C8';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=1 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R5C9:R5C9';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=1 ;
put count ;
run;


/***************************************************************************************/

/*FEB*/
filename ddedata dde 'excel|TB_output!R6C2:R6C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=2 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R6C3:R6C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds' and month=2;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R6C4:R6C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=2 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R6C5:R6C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult' and month=2;
put count ;
run;



/*MAR*/
/*peads*/
filename ddedata dde 'excel|TB_output!R7C2:R7C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=3 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R7C3:R7C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=3 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R7C4:R7C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=3 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R7C5:R7C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=3 ;
put count ;
run;


/*APR*/
/*peads*/
filename ddedata dde 'excel|TB_output!R8C2:R8C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=4 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R8C3:R8C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=4 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R8C4:R8C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=4 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R8C5:R8C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=4 ;
put count ;
run;


/*May*/
/*peads*/
filename ddedata dde 'excel|TB_output!R9C2:R9C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=5 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R9C3:R9C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=5 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R9C4:R9C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=5 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R9C5:R9C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=5 ;
put count ;
run;

/*June*/
/*peads*/
filename ddedata dde 'excel|TB_output!R10C2:R10C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=6 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R10C3:R10C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=6 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R10C4:R10C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=6 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R10C5:R10C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=6 ;
put count ;
run;

/*jULY*/
/*peads*/
filename ddedata dde 'excel|TB_output!R11C2:R11C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=7 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R11C3:R11C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=7 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R11C4:R11C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=7 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R11C5:R11C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=7 ;
put count ;
run;


/*AUG*/
/*peads*/
filename ddedata dde 'excel|TB_output!R12C2:R12C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=8 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R12C3:R12C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=8 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R12C4:R12C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=8 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R12C5:R12C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=8;
put count ;
run;


/*SEP*/
/*peads*/
filename ddedata dde 'excel|TB_output!R13C2:R13C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=9 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R13C3:R13C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=9 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R13C4:R13C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=9 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R13C5:R13C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=9;
put count ;
run;



/*OCT*/
/*peads*/
filename ddedata dde 'excel|TB_output!R14C2:R14C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=10 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R14C3:R14C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=10 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R14C4:R14C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=10 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R14C5:R14C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=10;
put count ;
run;


/*NOV*/
/*peads*/
filename ddedata dde 'excel|TB_output!R15C2:R15C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=11 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R15C3:R15C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=11 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R15C4:R15C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=11 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R15C5:R15C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=11;
put count ;
run;



/*DEC*/
/*peads*/
filename ddedata dde 'excel|TB_output!R16C2:R16C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='paeds'and month=12 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R16C3:R16C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='paeds'and month=12 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R16C4:R16C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Busia clinic' and ptype='adult'and month=12 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R16C5:R16C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Busia clinic' and ptype='adult'and month=12;
put count ;
run;



/*BUMALA A ......BUMALA A ........BUMALA A ........BUMALA A ........BUMALA A ........BUMALA A ..........*

NOT YET COMPLETE FOR POPULATING TEMP/


/*JAN*/
/*peads*/
filename ddedata dde 'excel|TB_output!R5C2:R5C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds' and month=1 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R5C3:R5C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=1 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R5C4:R5C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=1 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R5C5:R5C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=1 ;
put count ;
run;
/*FEB*/
filename ddedata dde 'excel|TB_output!R6C2:R6C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=2 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R6C3:R6C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds' and month=2;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R6C4:R6C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=2 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R6C5:R6C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult' and month=2;
put count ;
run;



/*MAR*/
/*peads*/
filename ddedata dde 'excel|TB_output!R7C2:R7C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=3 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R7C3:R7C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=3 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R7C4:R7C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=3 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R7C5:R7C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=3 ;
put count ;
run;


/*APR*/
/*peads*/
filename ddedata dde 'excel|TB_output!R8C2:R8C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=4 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R8C3:R8C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=4 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R8C4:R8C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=4 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R8C5:R8C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=4 ;
put count ;
run;


/*May*/
/*peads*/
filename ddedata dde 'excel|TB_output!R9C2:R9C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=5 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R9C3:R9C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=5 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R9C4:R9C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=5 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R9C5:R9C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=5 ;
put count ;
run;

/*June*/
/*peads*/
filename ddedata dde 'excel|TB_output!R10C2:R10C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=6 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R10C3:R10C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=6 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R10C4:R10C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=6 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R10C5:R10C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=6 ;
put count ;
run;

/*jULY*/
/*peads*/
filename ddedata dde 'excel|TB_output!R11C2:R11C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=7 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R11C3:R11C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=7 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R11C4:R11C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=7 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R11C5:R11C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=7 ;
put count ;
run;


/*AUG*/
/*peads*/
filename ddedata dde 'excel|TB_output!R12C2:R12C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=8 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R12C3:R12C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=8 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R12C4:R12C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=8 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R12C5:R12C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=8;
put count ;
run;


/*SEP*/
/*peads*/
filename ddedata dde 'excel|TB_output!R13C2:R13C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=9 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R13C3:R13C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=9 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R13C4:R13C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=9 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R13C5:R13C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=9;
put count ;
run;



/*OCT*/
/*peads*/
filename ddedata dde 'excel|TB_output!R14C2:R14C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=10 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R14C3:R14C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=10 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R14C4:R14C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=10 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R14C5:R14C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=10;
put count ;
run;


/*NOV*/
/*peads*/
filename ddedata dde 'excel|TB_output!R15C2:R15C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=11 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R15C3:R15C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=11 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R15C4:R15C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=11 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R15C5:R15C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=11;
put count ;
run;



/*DEC*/
/*peads*/
filename ddedata dde 'excel|TB_output!R16C2:R16C2';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='paeds'and month=12 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R16C3:R16C3';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='paeds'and month=12 ;
put count ;
run;

/*Adults*/
filename ddedata dde 'excel|TB_output!R16C4:R16C4';

data _null_;
file ddedata ;
set xfinal end=eof;
where gender='M' and clinic='Bumala A' and ptype='adult'and month=12 ;
put count ;
run;

filename ddedata dde 'excel|TB_output!R16C5:R16C5';
data _null_;
file ddedata ;
set xfinal end=eof;
where gender='F' and clinic='Bumala A' and ptype='adult'and month=12;
put count ;
run;
