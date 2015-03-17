# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26) 
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)

# AFFIA Plus sites : (5,67,60,66,18,63,99,68,59,85)

# START queries to identify appts per patient

# # of LTFU patients across AMPATH stratified by year parameterized by ltfu_date

select * from
(select year(encounter_datetime) as year,
sum(if(encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47),1,0)) as hiv_clinic_visits,
count(distinct if(hiv_start_date, person_id,null)) as hiv_patients
from lost_to_follow_up
where location_id not in (5,67,60,66,18,63,99,68,59,85)
group by year) t0
join
(select year(encounter_datetime) as year,
count(*) as ltfu_total,
count(distinct if(plan in (1,2),person_id,null)) as ltfu_on_arvs,
sum(if(encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47),1,null)) as ltfu_clinic_visits,
sum(if(encounter_type in (1,2,14,17,19),1,null)) as adult,
sum(if(encounter_type in (3,4,15,26),1,null)) as peds,
sum(if(encounter_type in (10,11,12,44,46,47),1,null)) as pmtct,
sum(if(encounter_type=21,1,null)) as outreach_visit,
sum(if(plan in (1,2) and encounter_type=21,1,null)) as outreach_visit_on_arvs
from 
(select @ltfu_date := '2014-01-01') p0,reporting_jd.lost_to_follow_up t1
where @ltfu_date > encounter_datetime and (@ltfu_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
and hiv_start_date and (encounter_datetime < '2013-01-01' or location_id not in (5,67,60,66,18,63,99,68,59,85))
and (encounter_type!=21 or outreach_found_patient=1)
group by year with rollup) t1 using (year);

# # of LTFU patients stratified by clinic parameterized by ltfu_date
select location_id, name as clinic,
count(*) as ltfu_total,
count(distinct if(plan in (1,2),person_id,null)) as ltfu_on_arvs,
sum(if(encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47),1,null)) as hiv_clinic_visit,
sum(if(encounter_type in (1,2,14,17,19),1,null)) as adult,
sum(if(encounter_type in (3,4,15,26),1,null)) as peds,
sum(if(encounter_type in (10,11,12,44,46,47),1,null)) as pmtct,
sum(if(encounter_type=21,1,null)) as outreach_visit,
sum(if(plan in (1,2) and encounter_type=21,1,null)) as outreach_visit_on_arvs

from 
(select @ltfu_date := '2014-01-01') p0, (select @start_date := '2002-01-01') p1, (select @end_date := '2014-01-01') p2,
reporting_jd.lost_to_follow_up t1
join amrs.location using (location_id)
where @ltfu_date > encounter_datetime and (@ltfu_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
and hiv_start_date and (encounter_type!=21 or outreach_found_patient)
and encounter_datetime >= @start_date and encounter_datetime <= @end_date and (encounter_datetime < '2013-01-01' or location_id not in (5,67,60,66,18,63,99,68,59,85))
group by location_id with rollup;


select name, count(*) as total,
sum(if(encounter_type in (1,2,3,4,17,26),1,null)) as care_visit,
sum(if(encounter_type in (14,15),1,null)) as refill_visit,
sum(if(encounter_type=21,1,null)) as outreach_visit,
count(distinct person_id) as num_patients,
count(distinct if(plan in (1,2),person_id,null)) as on_arvs,
count(distinct if(plan=1,person_id,null)) as on_first_line,
count(distinct if(plan=2,person_id,null)) as on_second_line
from 
(select @ltfu_date := '2014-01-01') p0,lost_to_follow_up t1
join amrs.location l on t1.location_id = l.location_id
where @ltfu_date > encounter_datetime and (@ltfu_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
and hiv_start_date and (encounter_datetime < '2013-01-01' or t1.location_id not in (5,67,60,66,18,63,99,68,59,85))
group by t1.location_id with rollup;


# LIST OF LTFU PATIENTS PARAMETERIZED BY CLINIC
select person_id, if(plan in (1,2),plan,'') as arv_regimen, 
	date(encounter_datetime) as encounter_datetime, 
case 
	when timestampdiff(day,arv_start_date,encounter_datetime) between 0 and 90 then '3 high risk'
	when timestampdiff(day,arv_start_date,encounter_datetime) > 90 then '2 medium risk'
	else '1 low risk'
end as risk_category,
et.name, date(rtc_date) as rtc_date, timestampdiff(day,rtc_date,@ltfu_date) as days_from_rtc_date
from 
(select @ltfu_date := '2014-01-01') p0,(select @location_id := 1) p1,
reporting_jd.lost_to_follow_up t1
join amrs.encounter_type et on t1.encounter_type = et.encounter_type_id
where @ltfu_date > encounter_datetime and (@ltfu_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
and hiv_start_date
and location_id = @location_id;


# LIST OF PATIENTS ABOUT TO BE LTFU PARAMETERIZED BY CLINIC
select person_id, if(plan in (1,2),plan,null) as arv_regimen, 
	date(encounter_datetime) as encounter_datetime, et.name, 
	date(rtc_date) as rtc_date,
	timestampdiff(day,rtc_date,curdate()) as days_from_rtc_date
from 
(select @start_range := 60) p0a, (select @end_range := 90) p0b, (select @location_id := 1) p0c,
reporting_jd.ltfu_3 t1
join amrs.encounter_type et on et.encounter_type_id = t1.encounter_type
where next_appt_date is null and timestampdiff(day,rtc_date,curdate()) > @start_range and timestampdiff(day,rtc_date,curdate()) < @end_range 
and location_id = @location_id
and hiv_start_date
order by encounter_datetime desc;


# LIST OF LTFU STATS (INITIAL LTFU, CURRENT LTFU, % CHANGE, RANK) STRATIFED BY CLINIC

select location_id, name, ltfu_at_start_date, ltfu, percent_change, abs_change, @cur_rank := @cur_rank + 1 as rank
from 
(select @cur_rank := 0) r,
(select location_id, ltfu_at_start_date, ltfu, (ltfu - ltfu_at_start_date)/ltfu_at_start_date as percent_change, ltfu - ltfu_at_start_date as abs_change
from
(
select location_id, count(*) as ltfu_at_start_date
from 
(select @ltfu_start_date := '2013-01-01') p0,ltfu_3 t1
where @ltfu_start_date > encounter_datetime and (@ltfu_start_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_start_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_start_date)
and hiv_start_date
group by t1.location_id) t0

join 

(select location_id, count(*) as ltfu
from 
(select @ltfu_date := '2014-01-01') p0,ltfu_3 t1
where @ltfu_date > encounter_datetime and (@ltfu_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
and hiv_start_date
group by t1.location_id) t1 using (location_id)
order by percent_change 
) t1
join amrs.location using (location_id);

# # OF PATIENTS WHO GET OUTREACH VISITS AFTER MISSING AN APPOINTMENT STRATIFED BY CLINIC


# # OF PATIENTS WHO RETURN TO CLINIC AFTER OUTREACH VISIT STRATIFIED BY CLINIC
select 
sum(if(days_from_rtc_date > 0 and encounter_type=21,1,0)) / sum(if(days_from_rtc_date > 0 or (encounter_datetime = last_appt and curdate() > rtc_date),1,0)) as percent_outreach_0,
sum(if(days_from_rtc_date >= 7 and encounter_type=21,1,0)) / sum(if(days_from_rtc_date or (encounter_datetime = last_appt and curdate() > rtc_date> 7),1,0)) as percent_outreach_7,
sum(if(days_from_rtc_date >= 14 and encounter_type=21,1,0)) / sum(if(days_from_rtc_date > 14 or (encounter_datetime = last_appt and curdate() > rtc_date> 7),1,0)) as percent_outreach_14,
sum(if(days_from_rtc_date >= 30 and encounter_type=21,1,0)) / sum(if(days_from_rtc_date > 30 or (encounter_datetime = last_appt and curdate() > rtc_date> 7),1,0)) as percent_outreach_30
from lost_to_follow_up;

# LIST OF STATS FOR ATTENDANCE OF HIV CARE VISITS BASED ON RTC DATE STRATIFIED BY CLINIC
select location_id, name,
sum(if(days_from_rtc_date is not null,1,0)) as total,
sum(if(days_from_rtc_date < 0,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as times_pt_came_before_rtc_date,
sum(if(days_from_rtc_date = 0,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as times_came_on_rtc_date,
sum(if(days_from_rtc_date >= 1 and days_from_rtc_date < 8 ,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_within_7_days,
sum(if(days_from_rtc_date >= 8 and days_from_rtc_date < 30 ,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_after_1_week_less_1_month,
sum(if(days_from_rtc_date >= 31,1,0))/sum(if(days_from_rtc_date is not null,1,0)) as came_after_1_month,
count(cur_rtc_date)/count(*) as perc_with_rtc_date,
sum(if(next_appt_date is null,1,0)) as did_not_return
from lost_to_follow_up
join amrs.location using (location_id) 
where encounter_type in (1,2,3,4,17,26)
group by location_id with rollup;


# QUERIES TO CREATE CSV FILES OF HIV CASES AND LTFU FOR PURPOSES OF MAPPING
SELECT f.location_id, l.name as clinic_name, f.county_district, f.location, f.sublocation
FROM lost_to_follow_up f
join amrs.location l using (location_id)
where f.next_appt_date is null and hiv_start_date
INTO OUTFILE '/tmp/foo.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';

select person_id, if(plan in (1,2),plan,'') as arv_regimen, year(rtc_date) as year, f.location_id, l.name as clinic_name, f.county_district, f.location, f.sublocation
from 
(select @ltfu_date := '2014-01-01') p0,
reporting_jd.lost_to_follow_up f
join amrs.location l using (location_id)
where @ltfu_date > encounter_datetime and (@ltfu_date < next_appt_date or next_appt_date is null)
and timestampdiff(day,rtc_date,@ltfu_date) > 90 and (transfer_out is null or transfer_out=0) and (death_date is null or death_date > @ltfu_date)
and hiv_start_date
order by rtc_date desc
INTO OUTFILE '/tmp/foo_ltfu.csv'
FIELDS TERMINATED BY ','
ENCLOSED BY '"'
LINES TERMINATED BY '\n';


# QUERIES TO COMPARE THE RATE OF TIME TO RETURN AFTER OUTREACH FOLLOW-UP

# query to assess time between outreach contact and missed appointment date
select case 
	when timestampdiff(day,arv_start_date,encounter_datetime) between 0 and 90 then '3 high risk'
	when timestampdiff(day,arv_start_date,encounter_datetime) > 90 then '2 medium risk'
	else '1 low risk'
end as risk_category,
count(*), 
sum(if(rtc_date is null,1,0)) as no_rtc_date,
sum(if(timestampdiff(day,rtc_date,next_appt_date) <= 7,1,0))/count(*) as less_7,
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 8 and 14,1,0))/count(*) as '8_to_14',
sum(if(timestampdiff(day,rtc_date,next_appt_date) <= 30,1,0))/count(*)  as '0_to_30',
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 31 and 90,1,0))/count(*)  as '31_to_90',
sum(if(timestampdiff(day,rtc_date,next_appt_date) > 90,1,0))/count(*)  as 'greater_than_90'
from lost_to_follow_up 
where next_encounter_type=21 and encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47)
group by risk_category;



# query to assess time to return after missed appointment with no outreach attempted
select
case 
	when timestampdiff(day,arv_start_date,encounter_datetime) between 0 and 90 then '3 high risk'
	when timestampdiff(day,arv_start_date,encounter_datetime) > 90 then '2 medium risk'
	else '1 low risk'
end as risk_category,
count(*),
sum(if(timestampdiff(day,rtc_date,next_appt_date) >0 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47),1,0))/count(*) as num_returning_to_clinic,
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 1 and 7 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47),1,0)) / count(*) as '1_less_7',
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 8 and 14 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*)  as '8_to_14',
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 0 and 30 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*)as '0_to_30',
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 31 and 90 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*)as '31_to_90',
sum(if(timestampdiff(day,rtc_date,next_appt_date) > 90 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*) as 'greater_than_90'
from
lost_to_follow_up 
where encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
and rtc_date is not null and 
((next_appt_date is null and timestampdiff(day,encounter_datetime,curdate()) > 0) or timestampdiff(day,rtc_date,next_appt_date) >0)
group by risk_category;



# query to look at effect of outreach independent of when outreach contact made relative to missed appt. 
select 
case 
	when timestampdiff(day,arv_start_date,encounter_datetime) between 0 and 90 then '3 high risk'
	when timestampdiff(day,arv_start_date,encounter_datetime) > 90 then '2 medium risk'
	else '1 low risk'
end as risk_category,
count(*), 
sum(if(next_appt_date and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*) as percent_returning,
sum(if(timestampdiff(day,rtc_date,next_appt_date) <= 7 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*) as less_7,
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 8 and 14 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*)  as '7_to_14',
sum(if(timestampdiff(day,rtc_date,next_appt_date) <= 30 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*)  as '0_to_30',
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 31 and 90 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*)  as '31_to_90',
sum(if(timestampdiff(day,rtc_date,next_appt_date) > 90 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) ,1,0))/count(*)  as 'greater_than_90'
from lost_to_follow_up 
where encounter_type=21 and outreach_found_patient and rtc_date and timestampdiff(day,prev_rtc_date,encounter_datetime) > 0 and prev_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
group by risk_category with rollup;


select if(plan in (1,2),1,0) as plan_2, count(*), 
sum(if(next_appt_date,1,0))/count(*),
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 0 and 7,1,0))/count(*) as less_7,
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 8 and 14,1,0))/count(*)  as '7_to_14',
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 0 and 30,1,0))/count(*)  as '0_to_30',
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 31 and 90,1,0))/count(*)  as '31_to_90',
sum(if(timestampdiff(day,rtc_date,next_appt_date) > 90,1,0))/count(*)  as 'greater_than_90'
from lost_to_follow_up 
where encounter_type=21 and outreach_found_patient and rtc_date
group by plan_2 with rollup;



#query looking at effect of time from outreach contact to time to return to clinic

select *, weekly_total/total
from
(select timestampdiff(week,prev_rtc_date,encounter_datetime) as week,
case 
	when timestampdiff(day,arv_start_date,encounter_datetime) between 0 and 90 then '3 high risk'
	when timestampdiff(day,arv_start_date,encounter_datetime) > 90 then '2 medium risk'
	else '1 low risk'
end as risk_category,
count(*) as weekly_total, 
sum(if(next_appt_date,1,0))/count(*) as returned_to_clinic,
sum(if(timestampdiff(day,encounter_datetime,next_appt_date) between 0 and 7,1,0))/count(*) as less_7,
sum(if(timestampdiff(day,encounter_datetime,next_appt_date) between 8 and 14,1,0))/count(*)  as '8_to_14',
sum(if(timestampdiff(day,encounter_datetime,next_appt_date) between 0 and 30,1,0))/count(*)  as '0_to_30',
sum(if(timestampdiff(day,encounter_datetime,next_appt_date) between 31 and 90,1,0))/count(*)  as '31_to_90',
sum(if(timestampdiff(day,encounter_datetime,next_appt_date) > 90,1,0))/count(*)  as 'greater_than_90'
from lost_to_follow_up 
where encounter_type=21 and outreach_found_patient and timestampdiff(week,prev_rtc_date,encounter_datetime) between 0 and 8 
group by week, risk_category) t0
join
(select 
case 
	when timestampdiff(day,arv_start_date,encounter_datetime) between 0 and 90 then '3 high risk'
	when timestampdiff(day,arv_start_date,encounter_datetime) > 90 then '2 medium risk'
	else '1 low risk'
end as risk_category,
count(*) as total
from lost_to_follow_up
where encounter_type=21 and outreach_found_patient
group by risk_category
) t1 using (risk_category)
;


# comparing effect of outreach on returning on new rtc date as recorded by outreach team at time of contact

select *, weekly_total/total
from
(select timestampdiff(week,prev_rtc_date,encounter_datetime) as week,
case 
	when timestampdiff(day,arv_start_date,encounter_datetime) between 0 and 90 then '3 high risk'
	when timestampdiff(day,arv_start_date,encounter_datetime) > 90 then '2 medium risk'
	else '1 low risk'
end as risk_category,
count(*) as weekly_total, 
sum(if(next_appt_date,1,0))/count(*) as returned_to_clinic,
sum(if(timestampdiff(day,rtc_date,next_appt_date) <= 7,1,0))/count(*) as less_7,
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 8 and 14,1,0))/count(*)  as '7_to_14',
sum(if(timestampdiff(day,rtc_date,next_appt_date) <= 30,1,0))/count(*)  as '0_to_30',
sum(if(timestampdiff(day,rtc_date,next_appt_date) between 31 and 90,1,0))/count(*)  as '31_to_90',
sum(if(timestampdiff(day,rtc_date,next_appt_date) > 90,1,0))/count(*)  as 'greater_than_90'
from lost_to_follow_up 
where encounter_type=21 and outreach_found_patient and rtc_date and timestampdiff(week,prev_rtc_date,encounter_datetime) between 0 and 8 
group by week, risk_category) t0
join
(select 
case 
	when timestampdiff(day,arv_start_date,encounter_datetime) between 0 and 90 then '3 high risk'
	when timestampdiff(day,arv_start_date,encounter_datetime) > 90 then '2 medium risk'
	else '1 low risk'
end as risk_category,
count(*) as total
from lost_to_follow_up
where encounter_type=21 and outreach_found_patient and rtc_date
group by risk_category
) t1 using (risk_category)
;


# OUTREACH WORKER WORK HABITS
select year(encounter_datetime) as year, month(encounter_datetime) as month, day(encounter_datetime), provider_id,
count(*)
from lost_to_follow_up,
(select @year := 2013) p0, (select @month := 10) p1
where year(encounter_datetime) = @year and month(encounter_datetime)=@month
and encounter_type=21
group by year, month, provider_id;


# rates of missed appointments for patients who were >= 7 days late to at least one appointment 

select l.location_id, name,
count(*) as num_patients,
sum(if(num_missed_appts=0,1,0))/count(*) as missed_none,
sum(if(num_missed_appts=1,1,0))/count(*) as missed_one,
sum(if(num_missed_appts >1,1,0))/count(*) as missed_more_than_1,
sum(if(num_missed_appts >1,1,0))/sum(if(num_missed_appts>=1,1,0)) as perc_of_missing_1_missing_more_than_1,

sum(if(num_missed_appts=2,1,0))/count(*) as missed_two,
sum(if(num_missed_appts >= 3,1,0))/count(*) as missed_3_or_more
from
(select location_id, person_id, sum(if((timestampdiff(day,rtc_date,next_appt_date) > 7) or (timestampdiff(day,rtc_date,curdate()) > 0 and next_appt_date is null),1,0)) as num_missed_appts 
	from lost_to_follow_up 
	where year(encounter_datetime) = 2013 and encounter_type in (1,2,14,17,19) and arv_start_date and transfer_out=0 and location_id not in (5,67,60,66,18,63,99,68,59,85)
	group by location_id,person_id) t0
join amrs.location l using (location_id)
group by l.location_id
order by num_patients desc;

select name, value_coded, count(distinct encounter_id), count(distinct encounter_id)/ from amrs.obs o join concept_name c on o.value_coded = c.concept_id where o.concept_id = 1733 group by value_coded;

select 
    name,
    value_coded,
    count(distinct encounter_id),
    count(distinct encounter_id) / 109620
from
    amrs.obs o
        join
    concept_name c ON o.value_coded = c.concept_id
where
    o.concept_id = 1733
	and encounter_id not in (select encounter_id from amrs.obs where concept_id=1733 and value_coded=1575)
group by value_coded;


select 
	location_id,
	sum(if(value_coded=l.name as clinic,
    c.name as concept,
    value_coded,
    count(distinct encounter_id),
    count(distinct encounter_id) / 109620
from
    amrs.obs o
		join
	location l on using (location_id)
        join
    concept_name c ON o.value_coded = c.concept_id
where
    o.concept_id = 1733
	and encounter_id not in (select encounter_id from amrs.obs where concept_id=1733 and value_coded=1575)
group by location_id



# QUERIES RELATING VIRAL LOAD TO LOST TO FOLLOW-UP

select year(vl1_date) as year, month(vl1_date) as month,
count(*),
sum(if(vl1 between 0 and 1000,1,0)) as suppressed,
sum(if(vl1 between 0 and 1000,1,0))/count(*) as perc_suppressed,
sum(if(vl1 between 1001 and 5000,1,0))/count(*) as '1k to 5k',
sum(if(vl1 between 1001 and 10000,1,0))/count(*) as '1k to 10k',
sum(if(vl1 between 1001 and 50000,1,0))/count(*) as '1k to 50k',
sum(if(vl1 between 1001 and 100000,1,0))/count(*) as '1k to 100k',
sum(if(vl1 >= 100001,1,0))/count(*) as greater_100_k
from reporting_jd.lost_to_follow_up l
where vl1_date >= encounter_datetime and (vl1_date < next_appt_date or next_appt_date is null) and encounter_datetime >= '2012-01-01'
and encounter_type in (1,2,14,17,19)
group by year,month with rollup;


select location_id, name,
count(*),
sum(if(vl1 between 0 and 1000,1,0)) as suppressed,
sum(if(vl1 between 0 and 1000,1,0))/count(*) as perc_suppressed,
sum(if(vl1 between 1001 and 5000,1,0))/count(*) as '1k to 5k',
sum(if(vl1 between 1001 and 10000,1,0))/count(*) as '1k to 10k',
sum(if(vl1 between 1001 and 50000,1,0))/count(*) as '1k to 50k',
sum(if(vl1 between 1001 and 100000,1,0))/count(*) as '1k to 100k',
sum(if(vl1 >= 100001,1,0))/count(*) as greater_100_k
from reporting_jd.lost_to_follow_up l
join amrs.location using (location_id)
where vl1_date >= encounter_datetime and (vl1_date < next_appt_date or next_appt_date is null) and encounter_datetime >= '2013-09-01' and hiv_start_date and arv_start_date
group by location_id with rollup;



select year(vl1_date) as year, month(vl1_date) as month,
count(*),
sum(if(vl1 between 0 and 1000,1,0)) as suppressed,
sum(if(vl1 between 0 and 1000,1,0))/count(*) as perc_suppressed,
sum(if(vl1 between 1001 and 5000,1,0))/count(*) as '1k to 5k',
sum(if(vl1 between 1001 and 10000,1,0))/count(*) as '1k to 10k',
sum(if(vl1 between 1001 and 50000,1,0))/count(*) as '1k to 50k',
sum(if(vl1 between 1001 and 100000,1,0))/count(*) as '1k to 100k',
sum(if(vl1 >= 100001,1,0))/count(*) as greater_100_k
from lost_to_follow_up l
	where hiv_start_date and arv_start_date 
		and ((timestampdiff(day,prev_rtc_date,encounter_datetime) > 60) or (next_appt_date is null and timestampdiff(day,rtc_date,curdate()) > 60))
		and vl1_date >= encounter_datetime
group by year,month with rollup;


# QUERIES IDENTIFYING WHEN PATIENTS STARTED ON ARVS

select person_id, encounter_id, encounter_datetime, encounter_type, hiv_start_date, arv_start_date, plan, prev_plan, on_nnrti, on_lopinavir, arv_plan, cd4_1, cd4_1_date
from lost_to_follow_up where person_id in (select person_id from lost_to_follow_up where arv_start_date >= '2011-10-01' and prev_plan is null);

SET group_concat_max_len := 65535;
SET group_concat_max_len := @@max_allowed_packet;

select year, month, total_patients, `not on arvs`, `started on arvs`, `started on arvs`/`not on arvs` as perc_started, median
from
(select year(encounter_datetime) as year, 
month(encounter_datetime) as month, 
count(*) as 'started on arvs',
substring_index(substring_index(group_concat(days_to_start order by days_to_start asc),',',ceiling(count(*)/2)),',',-1) as median
from
(select person_id, encounter_datetime, hiv_start_date,arv_start_date, timestampdiff(day,hiv_start_date,arv_start_date) as days_to_start
from lost_to_follow_up where arv_start_date >= '2011-01-01' and prev_plan is null and encounter_type in (1,2,14,17,19)) t0
group by year, month with rollup) t1
join

(select year(encounter_datetime) as year, month(encounter_datetime) as month,
count(distinct person_id) as total_patients,
count(distinct if(plan is null,person_id,null)) as 'not on arvs'
from lost_to_follow_up
where encounter_type in (1,2,14,17,19) and encounter_datetime >= '2011-01-01'
group by year, month with rollup) t2
using (year, month)
;



# QUERIES IDENTIFYING PATIENTS NEEDING THIRD-LINE
select 
count(distinct person_id),
count(distinct if(cd4_1 <= 200 and vl1 <= 1000,person_id,null)),
count(distinct if(cd4_1 <= 200 and vl1 > 1000,person_id,null)),
count(distinct if(cd4_1 <= 200 and vl1 > 5000,person_id,null)),
count(distinct if(cd4_1 <= 200 and vl1 > 10000,person_id,null)),
count(distinct if(cd4_1 <= 200 and vl1 > 100000,person_id,null))
from lost_to_follow_up where
encounter_type in (3,4,15,26) and encounter_datetime >= '2013-01-01' and plan=2;

select person_id, timestampdiff(year,birthdate,curdate()), encounter_datetime, arv_start_date, prev_plan, plan, vl1, vl1_date, vl2, vl2_date, cd4_1, cd4_1_date, cd4_2, cd4_2_date
from lost_to_follow_up
where person_id in
(select distinct person_id from lost_to_follow_up where cd4_1 <= 200 and vl1 > 100000 and encounter_type in (3,4,15,26) and encounter_datetime >= '2013-01-01' and plan=2);



select year(arv_start_date) as year, count(distinct person_id) as num_started, avg(cd4_1) from lost_to_follow_up where prev_plan is null and plan and arv_start_date and cd4_1 < 1000 and timestampdiff(year,cd4_1_date,arv_start_date) between 0 and 1 group by year

;# BASIC INFO ON OUTREACH ACTIVITIES
select 
	year(encounter_datetime) as year,	
	count(*) as num_outreach_visits,
	sum(if(outreach_found_patient=1,1,0))/count(*) as percent_found,
	sum(if(rtc_date and outreach_found_patient=1 and next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47),1,0)) / sum(if(outreach_found_patient and rtc_date,1,0)) as percent_found_who_returned
	from lost_to_follow_up 
	where encounter_type = 21
	group by year;
