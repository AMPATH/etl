# HIV encounter_type_ids : (1,2,3,4,10,11,12,17,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV non-pmtct encounter_type_ids : (1,2,3,4,17,26)

# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)

# Build LTFU table

drop temporary table if exists enc;
create temporary table enc (encounter_id int, encounter_datetime datetime, index encounter_id (encounter_id), index person_id (person_id))
(select e.encounter_id, e.encounter_datetime, e.location_id, e.provider_id, e.patient_id as person_id, e.encounter_type
from amrs.encounter e
where e.voided=0
);

drop temporary table if exists n_obs;
create temporary table n_obs (encounter_id int, obs_datetime datetime, rtc_date datetime, index encounter_id (encounter_id))
(select person_id, obs_datetime, encounter_id,
min(if(concept_id in (1941,1088) and value_coded in (794),1,null)) as on_lopinavir, 
min(if(concept_id in (1941,1088) and value_coded in (6467,6964,792,633,631),1,null)) as on_nrti, 
min(if(concept_id in (5096,1502,1777),value_datetime,null)) as rtc_date,
max(if( (concept_id = 1285 and value_coded=1287) or (concept_id=1596 and value_coded=1594) or (concept_id=6206 and value_coded=1595),1,0)) as transfer_out,
max(if( (concept_id = 1285 and value_coded=1286) or (concept_id=1733 and value_coded=1732),1,0)) as transfer_in,
max(if(concept_id in (1042,1040) and value_coded=703,1,0)) as is_hiv_positive

from amrs.obs o
where concept_id in (1088,1941,5096,1777,1285,1596,6206,1733,1042,1040) and o.voided=0
group by person_id, obs_datetime, encounter_id
order by person_id
);

# query to create table of patients who have died and associated dates based on concept defintion in wiki. 
# Note the query (used from the wiki) does not map to the definition on the wiki. This should be addressed at a later date (7/1/2013). For now will use the
# earliest date as the death date
drop temporary table if exists death_dates;
create temporary table death_dates (person_id int, death_date datetime, index person_id (person_id))
(select person_id, min(death_date) as death_date from
(
select person_id,obs_datetime as death_date, 'o' as type from amrs.obs o where o.voided=0 and ((o.concept_id=6206 and o.value_coded=159) or  o.concept_id in(1570,1734,1573) or (o.concept_id=1733 and o.value_coded=159) or (o.concept_id=1596 and o.value_coded=1593)) 
union
select e.patient_id as person_id, encounter_datetime, 'e' as type from amrs.encounter e where e.voided=0 and e.encounter_type=31 
union 
select p.person_id, p.death_date, 'p' as type from amrs.person p where p.voided=0 and p.death_date is not null) t0
where person_id in (select patient_id from amrs.patient where voided=0)
group by person_id);


drop temporary table if exists ltfu_1;
create temporary table ltfu_1 (id MEDIUMINT NOT NULL AUTO_INCREMENT, person_id int, obs_datetime datetime, encounter_datetime datetime)
(select *
from enc e1 
left outer join n_obs n1 using (encounter_id,person_id)
left outer join death_dates d using (person_id)
left outer join last_appts l using (person_id)

order by e1.person_id, e1.encounter_datetime);


select @prev_id := null;
select @cur_id := null;
select @prev_plan := null;
select @cur_plan := null;
select @prev_cd4 := null;
select @cur_cd4 := null;
select @prev_encounter_type := null;
select @cur_encounter_type := null;
select @prev_appt_date :=null;
select @cur_appt_date :=null;
select @prev_rtc_date :=null;
select @cur_rtc_date :=null;
select @days_from_rtc_date := null;
select @hiv_positive_date := null;

drop temporary table if exists ltfu_2;
create temporary table ltfu_2 (encounter_datetime datetime, person_id int, prev_appt_date datetime, cur_appt_date datetime, prev_rtc_date datetime, cur_rtc_date datetime,
					 prev_cd4 int, plan int)


(select *,
year(encounter_datetime) as year,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

if(@prev_id = @cur_id, @prev_plan := @cur_plan, @prev_plan:=null) as prev_plan,
if(on_lopinavir=1, @cur_plan := 2, if(on_nrti=1,@cur_plan:=1,@cur_plan:=@prev_plan)) as plan,

if(@prev_id = @cur_id, if(@cur_cd4,@prev_cd4 := @cur_cd4,@prev_cd4), @prev_cd4 :=null) as prev_cd4,
if(cd4_count,@cur_cd4:=cd4_count,@cur_cd4:=null) as cur_cd4,

if(@prev_id = @cur_id,@prev_encounter_type := @cur_encounter_type, @prev_encounter_type:=encounter_type) as prev_encounter_type,
@cur_encounter_type := encounter_type as cur_encounter_type,

cast(if(@prev_id = @cur_id, if(@prev_encounter_type not in (5,6,7,8,9,45,87),@prev_appt_date := @cur_appt_date,@prev_appt_date), @prev_appt_date:=null) as datetime) as prev_appt_date,
@cur_appt_date := encounter_datetime as cur_appt_date,

cast(if(@prev_id = @cur_id, @prev_rtc_date := @cur_rtc_date, @prev_rtc_date:=null) as datetime) as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,
@days_from_rtc_date := if(@prev_id = @cur_id,timestampdiff(day, @prev_rtc_date,encounter_datetime),null) as days_from_rtc_date,

cast(if(@prev_id = @cur_id,
		if(encounter_datetime < @hiv_start_date and (tested_hiv_positive or encounter_type in (1,2,3,4)), @hiv_start_date := encounter_datetime,@hiv_start_date),
		if(tested_hiv_positive or encounter_type in (1,2,3,4),@hiv_start_date := encounter_datetime, @hiv_start_date:=null)) as datetime) as hiv_start_date

from ltfu_1 order by person_id, encounter_datetime);


alter table ltfu_2 drop prev_id, drop cur_id, drop cur_appt_date, drop cur_rtc_date, drop cur_cd4, drop prev_encounter_type, drop cur_encounter_type;


select @prev_id := null;
select @cur_id := null;
select @prev_appt_date := null;
select @cur_appt_date := null;
select @prev_encounter_type := null;
select @cur_encounter_type := null;

drop table if exists ltfu_3;
create table ltfu_3 (next_appt datetime, index person_id (person_id), index encounter_datetime (encounter_datetime), primary key(id))
(select * from
((select *,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,

if(@prev_id = @cur_id,@prev_encounter_type := @cur_encounter_type, @prev_encounter_type:=encounter_type) as prev_encounter_type,
@cur_encounter_type := encounter_type as cur_encounter_type,

cast(if(@prev_id = @cur_id, if(@prev_encounter_type not in (5,6,7,8,9,45,87),@prev_appt_date := @cur_appt_date,@prev_appt_date),@prev_appt_date:=null) as datetime) as next_appt_date,
@cur_appt_date := encounter_datetime as cur_appt_date


from (select * from ltfu_2 order by person_id, encounter_datetime desc) t0
)) t2 order by person_id, encounter_datetime);

alter table ltfu_3 drop prev_id, drop cur_id, drop cur_appt_date, drop prev_encounter_type, drop cur_encounter_type;


# START queries to identify appts per patient

# of encounter visits stratified by num encounters and year
select num_encounters, count(distinct person_id) as num_patients,
sum(if(year = 2010,1,null)) as 2010_patients,
sum(if(year = 2011, 1,null)) as 2011_patients,
sum(if(year=2012,1,null)) as 2012_patients,
sum(if(year=2013,1,null)) as 2013_patients
from
(select year(obs_datetime) as year, person_id, count(*) as num_encounters from
lost_to_follow_up
where encounter_type in (1,2,3,4,10,11,17,26,47)
group by year,person_id) t1;


# of patients seen in YYYY, how many have had n encounters that YYYY
select @start_date := '2013-01-01';
select @end_date := '2014-01-01';
select @total_enc := count(distinct person_id) from lost_to_follow_up where encounter_type in (1,2,3,4,17,26) and obs_datetime >= @start_date and obs_datetime < @end_date;

select num_encounters, count(distinct person_id)/@total_enc as num_patients
from
(select person_id, count(*) as num_encounters from
lost_to_follow_up
where encounter_type in (1,2,3,4,17,26) 
and obs_datetime >= @start_date and obs_datetime < @end_date
group by person_id) t1
group by num_encounters with rollup;


# # of patients who came to clinic within n weeks of the RTC date, system-wide stratified by year
select @prev_id := -2;
select @cur_id := -1;
select @prev_appt_date :=null;
select @cur_appt_date :=null;
select @prev_rtc_date :=null;
select @cur_rtc_date :=null;


select 
year,
sum(if(days_from_rtc_date is not null,1,0)) as denom,
sum(if(days_from_rtc_date < 0,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as times_pt_came_before_rtc_date,
sum(if(days_from_rtc_date = 0,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as times_came_on_rtc_date,
sum(if(days_from_rtc_date >= 1 and days_from_rtc_date < 8 ,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_within_7_days,
sum(if(days_from_rtc_date >= 8 and days_from_rtc_date < 30 ,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_after_1_week_less_1_month,
sum(if(days_from_rtc_date >= 31,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_after_1_month,
count(cur_rtc_date)/count(*) as perc_with_rtc_date

from
(select year(obs_datetime) as year,
person_id, 
obs_datetime, 
encounter_type,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,
cast(if(@prev_id = @cur_id, @prev_appt_date := @cur_appt_date, @prev_appt_date:=null) as datetime) as prev_appt_date,
@cur_appt_date := obs_datetime as cur_appt_date,
cast(if(@prev_id = @cur_id, @prev_rtc_date := @cur_rtc_date, @prev_rtc_date:=null) as datetime) as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,
if(@prev_id = @cur_id,timestampdiff(day, @prev_rtc_date,obs_datetime),null) as days_from_rtc_date
from lost_to_follow_up where encounter_type in (1,2,3,4,17,26)
order by person_id, obs_datetime
) t1
group by year;




# # of patients who came to a particular clinic within n weeks of the RTC date, stratified by clinic, parameterized by datetime
select @prev_id := -2;
select @cur_id := -1;
select @prev_appt_date :=null;
select @cur_appt_date :=null;
select @prev_rtc_date :=null;
select @cur_rtc_date :=null;
select @start_date := '2013-01-01';
select @end_date := '2014-01-01';


select 
cur_location_id as clinic,
count(*),
sum(if(days_from_rtc_date is not null,1,0)) as denom,
sum(if(days_from_rtc_date < 0,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as times_pt_came_before_rtc_date,
sum(if(days_from_rtc_date = 0,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as times_came_on_rtc_date,
sum(if(days_from_rtc_date >= 1 and days_from_rtc_date < 8 ,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_within_7_days,
sum(if(days_from_rtc_date >= 8 and days_from_rtc_date < 30 ,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_after_1_week_less_1_month,
sum(if(days_from_rtc_date >= 31,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_after_1_month,
sum(if(cur_rtc_date,1,0))/count(*) as has_rtc_date


from
(select year(encounter_datetime) as year,
person_id, 
encounter_datetime, 
location_id,
encounter_type,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,
cast(if(@prev_id = @cur_id, @prev_appt_date := @cur_appt_date, @prev_appt_date:=null) as datetime) as prev_appt_date,
@cur_appt_date := obs_datetime as cur_appt_date,
cast(if(@prev_id = @cur_id, @prev_rtc_date := @cur_rtc_date, @prev_rtc_date:=null) as datetime) as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,
if(@prev_id = @cur_id,timestampdiff(day, @prev_rtc_date,encounter_datetime),null) as days_from_rtc_date
from ltfu_2 where encounter_type in (1,2,3,4,17,26)
order by person_id, encounter_datetime
) t1
where encounter_datetime >= @start_date and encounter_datetime < @end_date
group by clinic;


#query to count number of appointmets per patient stratified by year
select year,num_appts,all_pts,num_pts, num_pts/all_pts from
(select year(obs_datetime) as year, count(distinct person_id) as all_pts
from arv_enc_with_plan
where encounter_type in (1,2,3,4,10,11,12,17,26,44,46,47)
group by year) t1

join

(select year, num_appts, count(*) as num_pts
from
(select person_id, year(obs_datetime) as year, count(*) as num_appts from arv_enc_with_plan where encounter_type in (1,2,3,4,10,11,12,17,26,44,46,47) group by person_id, year) t1
group by year, num_appts) t2 using (year)
order by num_appts,year;


#query to count number of appointmets per patient all time
select @all_pts := count(distinct person_id) from arv_enc_with_plan where encounter_type in (1,2,3,4,10,11,12,17,26,44,46,47) ;

select sum(if(num_appts<=3,percent,null)) as percent_with_3_or_less_appts,
sum(if(num_appts>3,percent,null)) as percent_with_4_or_more_appts
from
(
select num_appts, count(*) as num_pts, count(*)/@all_pts as percent
from
(select person_id, count(*) as num_appts from arv_enc_with_plan where encounter_type in (1,2,3,4,10,11,12,17,26,44,46,47) group by person_id) t1
group by num_appts) t2;


# query to identify patients who have not been seen within past three months, who has not died, not transferred, 
# query to identify patients LTFU stratified by last appt date




# example query showing that clinicians are not filling out transfer in/out correctly. notice the multiple transfer ins without transfer outs
select * from ltfu where person_id = 145991;



# query to identify number of patients who return to clinic stratified by month
select @prev_id := -2;
select @cur_id := -1;
select @prev_appt_date :=null;
select @cur_appt_date :=null;
select @prev_rtc_date :=null;
select @cur_rtc_date :=null;


select @total := count(*) from

(select year(obs_datetime) as year,
person_id, 
obs_datetime, 
encounter_type,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,
cast(if(@prev_id = @cur_id, @prev_appt_date := @cur_appt_date, @prev_appt_date:=null) as datetime) as prev_appt_date,
@cur_appt_date := obs_datetime as cur_appt_date,
cast(if(@prev_id = @cur_id, @prev_rtc_date := @cur_rtc_date, @prev_rtc_date:=null) as datetime) as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,
if(@prev_id = @cur_id,timestampdiff(day, @prev_rtc_date,obs_datetime),null) as days_from_rtc_date,
if(@prev_id = @cur_id,timestampdiff(month, @prev_rtc_date,obs_datetime),null) as months_from_rtc_date
from arv_enc_with_plan where encounter_type in (1,2,3,4,17,26)
order by person_id, obs_datetime
) t1

where days_from_rtc_date > 90;

select @prev_id := -2;
select @cur_id := -1;
select @prev_appt_date :=null;
select @cur_appt_date :=null;
select @prev_rtc_date :=null;
select @cur_rtc_date :=null;

select months_from_rtc_date, count(*) / @total
from
(select year(obs_datetime) as year,
person_id, 
obs_datetime, 
encounter_type,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,
cast(if(@prev_id = @cur_id, @prev_appt_date := @cur_appt_date, @prev_appt_date:=null) as datetime) as prev_appt_date,
@cur_appt_date := obs_datetime as cur_appt_date,
cast(if(@prev_id = @cur_id, @prev_rtc_date := @cur_rtc_date, @prev_rtc_date:=null) as datetime) as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,
if(@prev_id = @cur_id,timestampdiff(day, @prev_rtc_date,obs_datetime),null) as days_from_rtc_date,
if(@prev_id = @cur_id,timestampdiff(month, @prev_rtc_date,obs_datetime),null) as months_from_rtc_date

from arv_enc_with_plan where encounter_type in (1,2,3,4,17,26)
order by person_id, obs_datetime
) t1
where days_from_rtc_date > 90
group by months_from_rtc_date;


# number of patients per year who have not been to any clinic in at least three months from ltfu_date;
select year(encounter_datetime) as year, 
count(*) as total,
sum(if(encounter_type in (1,2,3,4,17,26),1,null)) as care_visit,
sum(if(encounter_type in (14,15),1,null)) as refill_visit,
sum(if(encounter_type=21,1,null)) as outreach_visit,
count(distinct person_id) as num_patients,
count(distinct if(plan in (1,2),person_id,null)) as on_arvs,
count(distinct if(plan=1,person_id,null)) as on_first_line,
count(distinct if(plan=2,person_id,null)) as on_second_line
from 
(select @ltfu_date := '2014-01-01') t0,ltfu_2 t1
join 
(select person_id, max(encounter_datetime) as encounter_datetime from ltfu_2 where encounter_datetime < @ltfu_date and encounter_type in (1,2,3,4,14,15,17,21,26) group by person_id) t2 using (person_id,encounter_datetime)
join amrs.location l on t1.location_id = l.location_id
where timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
group by year with rollup;


# number of patients per clinic who have not come in at least three months from ltfu_date;
select name,count(*) as total,
sum(if(encounter_type in (1,2,3,4,17,26),1,null)) as care_visit,
sum(if(encounter_type in (14,15),1,null)) as refill_visit,
sum(if(encounter_type=21,1,null)) as outreach_visit,
count(distinct person_id) as num_patients,
count(distinct if(plan in (1,2),person_id,null)) as on_arvs,
count(distinct if(plan=1,person_id,null)) as on_first_line,
count(distinct if(plan=2,person_id,null)) as on_second_line
from 
(select @ltfu_date := '2014-01-01') t0,ltfu_2 t1
join 
(select person_id, max(encounter_datetime) as encounter_datetime from ltfu_2 where encounter_datetime < @ltfu_date and encounter_type in (1,2,3,4,14,15,17,21,26) group by person_id) t2 using (person_id,encounter_datetime)
join amrs.location l on t1.location_id = l.location_id
where timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
group by t1.location_id;


# list of patients that are LTFU parameterized by clinic.
select person_id, if(plan in (1,2),plan,'') as arv_regimen, 
	if(cd4_count,cd4_count,prev_cd4) as last_cd4,
	date(encounter_datetime) as encounter_datetime, 
et.name, date(rtc_date) as rtc_date, timestampdiff(day,rtc_date,ltfu_date) as days_from_rtc_date

from 
(select @ltfu_date := '2014-01-01') t0, reporting_jd.ltfu_2 t1
join 
(select person_id, max(encounter_datetime) as encounter_datetime from reporting_jd.ltfu_2 where encounter_datetime < @ltfu_date and encounter_type in (1,2,3,4,14,15,17,21,26) group by person_id) t2 using(person_id,encounter_datetime)
join amrs.encounter_type et on et.encounter_type_id = t1.encounter_type
where timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
and location_id = 1
order by encounter_datetime desc;

# list of patients that are x days past RTC but less than Y days past RTC, default is 60 and 90.
select person_id, if(plan in (1,2),plan,null) as arv_regimen, 
	if(cd4_count,cd4_count,prev_cd4) as last_cd4,
	date(encounter_datetime) as encounter_datetime, et.name, 
	date(rtc_date) as rtc_date,
	timestampdiff(day,rtc_date,curdate()) as days_from_rtc_date
from 
(select @min_days := 60) t0a, (select @max_days := 90) t0b,reporting_jd.ltfu_2 t1
join amrs.encounter_type et on et.encounter_type_id = t1.encounter_type
where encounter_datetime=last_appt and timestampdiff(day,rtc_date,curdate()) > @min_days and timestampdiff(day,rtc_date,curdate()) < @max_days 
and location_id = 1
order by encounter_datetime desc;





# find number of patients per year who arrive 30 days or more after rtc_date
select @prev_id := -2;
select @cur_id := -1;
select @prev_appt_date :=null;
select @cur_appt_date :=null;
select @prev_rtc_date :=null;
select @cur_rtc_date :=null;


select year(obs_datetime) as year, count(distinct person_id), count(distinct if(days_from_rtc_date>30,person_id,null))/count(distinct person_id) as pts_late

from

(select year(obs_datetime) as year,
person_id, 
obs_datetime, 
encounter_type,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,
cast(if(@prev_id = @cur_id, @prev_appt_date := @cur_appt_date, @prev_appt_date:=null) as datetime) as prev_appt_date,
@cur_appt_date := obs_datetime as cur_appt_date,
cast(if(@prev_id = @cur_id, @prev_rtc_date := @cur_rtc_date, @prev_rtc_date:=null) as datetime) as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,
if(@prev_id = @cur_id,timestampdiff(day, @prev_rtc_date,obs_datetime),null) as days_from_rtc_date,
if(@prev_id = @cur_id,timestampdiff(month, @prev_rtc_date,obs_datetime),null) as months_from_rtc_date
from arv_enc_with_plan where encounter_type in (1,2,3,4,17,26)
order by person_id, obs_datetime
) t1

group by year;


# percentage of patients who come to clinic after outreach visit

select 
sum(if(days_from_rtc_date > 0 and encounter_type=21,1,0)) / sum(if(days_from_rtc_date > 0 or (encounter_datetime = last_appt and curdate() > rtc_date),1,0)) as percent_outreach_0,
sum(if(days_from_rtc_date >= 7 and encounter_type=21,1,0)) / sum(if(days_from_rtc_date or (encounter_datetime = last_appt and curdate() > rtc_date> 7),1,0)) as percent_outreach_7,
sum(if(days_from_rtc_date >= 14 and encounter_type=21,1,0)) / sum(if(days_from_rtc_date > 14 or (encounter_datetime = last_appt and curdate() > rtc_date> 7),1,0)) as percent_outreach_14,
sum(if(days_from_rtc_date >= 30 and encounter_type=21,1,0)) / sum(if(days_from_rtc_date > 30 or (encounter_datetime = last_appt and curdate() > rtc_date> 7),1,0)) as percent_outreach_30
from ltfu_2;



# ltfu stats - using ltfu_3
select name, count(*) as total,
sum(if(encounter_type in (1,2,3,4,17,26),1,null)) as care_visit,
sum(if(encounter_type in (14,15),1,null)) as refill_visit,
sum(if(encounter_type=21,1,null)) as outreach_visit,
count(distinct person_id) as num_patients,
count(distinct if(plan in (1,2),person_id,null)) as on_arvs,
count(distinct if(plan=1,person_id,null)) as on_first_line,
count(distinct if(plan=2,person_id,null)) as on_second_line
from 
(select @ltfu_date := '2014-01-01') p0,ltfu_3 t1
join amrs.location l on t1.location_id = l.location_id
where @ltfu_date > encounter_datetime and (@ltfu_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
group by t1.location_id with rollup;

select count(*), sum(if(total=1, 1,0)) as one_visit, sum(if(total > 1, 1,0)) as more_than_one
from
(select person_id, count(*) as total
from 
(select @ltfu_date := '2014-01-01') p0,ltfu_3 t1
where @ltfu_date > encounter_datetime and (@ltfu_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
group by person_id) t0;

select count(distinct person_id) from ltfu_3 where encounter_type in (1,2,3,4)
