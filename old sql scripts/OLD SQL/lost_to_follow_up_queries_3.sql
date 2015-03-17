# HIV encounter_type_ids : (1,2,3,4,10,11,12,17,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV non-pmtct encounter_type_ids : (1,2,3,4,17,26)

# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,
#                      69,70,71,73,75,76,78,79,80,83)

# START queries to identify appts per patient

# of encounter visits stratified by num encounters and year
select num_encounters, count(distinct person_id) as num_patients,
sum(if(year = 2010 and num_encounters=1,1,null)) as 2010_patients,
sum(if(year = 2011, 1,null)) as 2011_patients,
sum(if(year=2012,1,null)) as 2012_patients,
sum(if(year=2013,1,null)) as 2013_patients
from
(select year(obs_datetime) as year, person_id, count(*) as num_encounters from
arv_enc_with_plan
where encounter_type in (1,2,3,4,10,11,17,26,47)
group by year,person_id) t1;


# of patients seen in YYYY, how many have had n encounters that YYYY
select @start_date := '2013-01-01';
select @end_date := '2014-01-01';
select @total_enc := count(distinct person_id) from arv_enc_with_plan where encounter_type in (1,2,3,4,17,26) and obs_datetime >= @start_date and obs_datetime < @end_date;

select num_encounters, count(distinct person_id)/@total_enc as num_patients
from
(select person_id, count(*) as num_encounters from
arv_enc_with_plan
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
count(*),
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
from arv_enc_with_plan where encounter_type in (1,2,3,4,17,26)
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
(select year(obs_datetime) as year,
person_id, 
obs_datetime, 
cur_location_id,
encounter_type,
@prev_id := @cur_id as prev_id, 
@cur_id := person_id as cur_id,
cast(if(@prev_id = @cur_id, @prev_appt_date := @cur_appt_date, @prev_appt_date:=null) as datetime) as prev_appt_date,
@cur_appt_date := obs_datetime as cur_appt_date,
cast(if(@prev_id = @cur_id, @prev_rtc_date := @cur_rtc_date, @prev_rtc_date:=null) as datetime) as prev_rtc_date,
@cur_rtc_date := rtc_date as cur_rtc_date,
if(@prev_id = @cur_id,timestampdiff(day, @prev_rtc_date,obs_datetime),null) as days_from_rtc_date
from arv_enc_with_plan where encounter_type in (1,2,3,4,17,26)
order by person_id, obs_datetime
) t1
where obs_datetime >= @start_date and obs_datetime < @end_date
group by clinic;


# query to find patients who have not returned to clinic by 60 days from last rtc date
select cur_location_id, person_id, obs_datetime, rtc_date,last_appt 
from arv_enc_with_plan 
where obs_datetime=last_appt and timestampdiff(day,rtc_date,curdate()) > 60 and timestampdiff(day,rtc_date,curdate()) < 90 
order by person_id,obs_datetime;

# query to find patients who have not returned to clinic by 60 days from last rtc date, stratified by clinic, parameterized by date

select cur_location_id,
sum(if(timestampdiff(day,rtc_date,curdate()) > 60 and timestampdiff(day,rtc_date,curdate()) < 90,1,0)) as num_missing
from arv_enc_with_plan 
where obs_datetime=last_appt
group by cur_location_id
order by cur_location_id, person_id,obs_datetime;

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




# OLD QUERIES ********************************************************************
drop temporary table if exists appointments;
create temporary table appointments
(select patient_id, encounter_datetime, e.encounter_id, value_datetime as next_appointment, e.location_id
from encounter e
left outer join obs o on e.encounter_id = o.encounter_id
where o.concept_id = 5096
and e.voided = 0 and e.encounter_type in (1,2,14,17) and o.voided=0 
);

drop table if exists follow_up;
create table follow_up
(select l.name as clinic, 
a.*, 
e.encounter_datetime as enc_on_exp_return_date, 
e.encounter_id as exp_return_enc_id, 
e2.encounter_id as enc_within_14_enc_id, 
e2.encounter_datetime as enc_within_14_days

from arv_enc_with_plan a
left outer join encounter e on a.next_appointment = e.encounter_datetime and e.patient_id = a.patient_id and e.encounter_type in (1,2,14,17) and e.voided=0 
left outer join encounter e2 on e2.patient_id = a.patient_id and abs(timestampdiff(DAY,a.next_appointment,e2.encounter_datetime)) < 14 and e2.encounter_id <> a.encounter_id and e2.encounter_type in (1,2,14,17) and e2.voided=0
inner join location l on a.location_id = l.location_id
order by a.patient_id, encounter_datetime asc
);



drop table if exists lost_to_follow_up;
create table lost_to_follow_up
(select timestampdiff(day,next_appointment,curdate()) as days_since_exp_return_date, f1.*, f2.max_date, f3.max_next_date
from follow_up f1
left outer join (select patient_id,max(encounter_datetime) as max_date from follow_up group by patient_id) f2 on f1.patient_id= f2.patient_id
left outer join (select patient_id,max(next_appointment) as max_next_date from follow_up group by patient_id) f3 on f1.patient_id= f3.patient_id
where f1.encounter_datetime = f2.max_date and f1.next_appointment = f3.max_next_date
and timestampdiff(day,next_appointment,curdate()) > 60
order by clinic, patient_id);


(select f1.*, f2.max_date, f3.max_next_date
from follow_up f1
left outer join (select patient_id,max(encounter_datetime) as max_date from follow_up group by patient_id) f2 on f1.patient_id= f2.patient_id
left outer join (select patient_id,max(next_appointment) as max_next_date from follow_up group by patient_id) f3 on f1.patient_id= f3.patient_id
where f1.encounter_datetime = f2.max_date and f1.next_appointment = f3.max_next_date
and timestampdiff(day,next_appointment,curdate()) > 60
order by clinic, patient_id);


select a.clinic, made_appt, did_not_make_appt, round(made_appt/(made_appt + did_not_make_appt)*100,1) as percent, lost_to_follow_up from
(select clinic, count(*) as made_appt from follow_up where enc_within_14_days is not null group by clinic) as a
left outer join (select clinic, count(*) as lost_to_follow_up from lost_to_follow_up group by clinic) as b on a.clinic=b.clinic
left outer join (select clinic, count(*) as did_not_make_appt from follow_up where enc_within_14_days is null group by clinic) as c on a.clinic=c.clinic
