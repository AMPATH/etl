# Patients on defaulters list

select t1.person_id, arv_start_date is not null as on_arvs, 
	date(encounter_datetime) as encounter_datetime, et.name, 
	date(rtc_date) as rtc_date,person_name, phone_number,identifier,arv_start_date,
	@days_since_rtc := timestampdiff(day,rtc_date,curdate()) as days_since_rtc,
	case 
		when encounter_type=21 and ampath_status=9080 then 0
		when encounter_type=21 and ampath_status=6101 and @days_since_rtc >= @start_range_high_risk then 1
		when rtc_date is null and timestampdiff(day,encounter_datetime,curdate()) < 90 then 5
		when (@days_since_rtc > 90 or (rtc_date is null and timestampdiff(day,encounter_datetime,curdate()) >= 90)) then 4
		when @days_since_rtc between @start_range_high_risk and 90 and (timestampdiff(day,encounter_datetime,rtc_date) <= 14 or timestampdiff(day,arv_start_date,encounter_datetime) <= 90) then 1
		when timestampdiff(day,arv_start_date,encounter_datetime) > 90 and @days_since_rtc between @start_range and @end_range then 2
		when @days_since_rtc between @start_range and @end_range then 3
		else null
	end as risk_category
from 
(select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 89) p0c,
reporting_JD.flat_retention_data t1
join amrs.encounter_type et on et.encounter_type_id = t1.encounter_type
where 
	next_appt_date is null 
	and dead=0
	and reason_exited_care is null
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care=1286)
	and ((timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) 
		between if(timestampdiff(day,encounter_datetime,rtc_date) < 14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,@start_range_high_risk,@start_range) and @end_range)
		or (encounter_type=21 and ampath_status=9080)
		or (if(rtc_date,rtc_date,encounter_datetime) > '2014-01-01' and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) >= 90)
		)
	and (if(encounter_type=21 and ampath_status=1286,health_center in (1),location_id in (1)))	
	and (if(encounter_type=21,ampath_status is null or ampath_status in (6101,1286,9080),1))	
order by risk_category,days_since_rtc desc,encounter_datetime desc, person_name;

# Patients on arvs who are LTFU or will be LTFU in next 2 weeks

select t1.person_id, arv_start_date is not null as on_arvs, 
	date(encounter_datetime) as encounter_datetime, et.name, 
	date(rtc_date) as rtc_date,person_name, phone_number,identifier,arv_start_date,
	@days_since_rtc := timestampdiff(day,rtc_date,curdate()) as days_since_rtc,
	case 
		when encounter_type=21 and ampath_status=9080 then 0
        when @days_since_rtc < 90 then 2
		else 4
	end as risk_category
from 
reporting_JD.flat_retention_data t1
join amrs.encounter_type et on et.encounter_type_id = t1.encounter_type
where 
	next_appt_date is null 
	and dead=0
	and reason_exited_care is null
	and arv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care=1286)
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) >= 76
	and (if(encounter_type=21 and ampath_status=1286,health_center in (1),location_id in (1)))	
	and (if(encounter_type=21,ampath_status is null or ampath_status in (6101,1286,9080),1))
	and timestampdiff(month,if(rtc_date,rtc_date,encounter_datetime),curdate()) <= 30
order by risk_category,encounter_datetime desc, person_name



# Number of patients needing follow-up stratified by clinic. 
select d1.location_id, name,
	count(if(risk_category_present >= 0,1,null)) as total,
	count(if(risk_category_present=0,1,null)) as num_being_traced,
	count(if(risk_category_present=1,1,null)) as num_high_risk,
	count(if(risk_category_present=2,1,null)) as num_medium_risk,
	count(if(risk_category_present=3,1,null)) as num_low_risk,
	count(if(risk_category_present=5,1,null)) as no_rtc,
	count(if(risk_category_present in (0,1,2,3,5),1,null)) as num_pre_ltfu,
	count(if(risk_category_present=4,1,null)) as num_ltfu,
	count(if(risk_category_present=6,1,null)) as num_untraceable
from

(select 	
	if(encounter_type=21 and ampath_status=1286,health_center,t1.location_id) as location_id,
	l.name,
	@days_since_rtc := timestampdiff(day,rtc_date,curdate()) as days_since_rtc,
	case 
		when encounter_type=21 and ampath_status=9080 then 0
		when encounter_type=21 and ampath_status=6101 and @days_since_rtc >= @start_range_high_risk then 1
		when encounter_type=21 and ampath_status=9079 then 6
		when rtc_date is null and timestampdiff(day,encounter_datetime,curdate()) < 90 then 5
		when (@days_since_rtc > 90 or (rtc_date is null and timestampdiff(day,encounter_datetime,curdate()) > 90)) then 4
		when @days_since_rtc between @start_range_high_risk and 90 and (timestampdiff(day,encounter_datetime,rtc_date) <= 14 or timestampdiff(day,arv_start_date,encounter_datetime) <= 90) then 1
		when timestampdiff(day,arv_start_date,encounter_datetime) > 90 and @days_since_rtc between cast(@start_range as unsigned) and cast(@end_range as unsigned) then 2
		when @days_since_rtc between cast(@start_range as unsigned) and cast(@end_range as unsigned) then 3
		else null
	end as risk_category_present
from 
(select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 100000) p0c,
reporting_JD.flat_retention_data t1
join amrs.location l on l.location_id = if(encounter_type=21 and ampath_status=1286,t1.health_center,t1.location_id)
where 
	next_appt_date is null 
	and dead=0
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and ((timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) 
		between if(timestampdiff(day,encounter_datetime,rtc_date) < 14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,@start_range_high_risk,@start_range) and @end_range)
		or (encounter_type=21 and ampath_status in (9079,9080)))
	and if(encounter_type=21,ampath_status is null or ampath_status in (6101,1286,9080,9079),1)
) d1
group by d1.location_id;

# KEY INDICATORS
select d1.location_id, name,
	count(if(risk_category_present >= 0,1,null)) as total,
	count(if(risk_category_present=1,1,null)) as num_pre_ltfu,
	count(if(risk_category_present=2,1,null)) as num_ltfu,
	count(if(risk_category_present=0,1,null)) as num_untraceable,
	sum(became_ltfu) as became_ltfu


from
(select 	
	if(encounter_type=21 and ampath_status=1286,health_center,t1.location_id) as location_id,
	l.name,
	case 
		when encounter_type=21 and ampath_status=9079 then 0
		when timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) < 90 then 1
		else 2
	end as risk_category_present,
	if(timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) between 90 and 96,1,null) as became_ltfu
from 
(select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 100000) p0c,
reporting_JD.flat_retention_data t1
join amrs.location l on l.location_id = if(encounter_type=21 and ampath_status=1286,t1.health_center,t1.location_id)
where 
	next_appt_date is null 
	and dead=0
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and ((timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) 
		between if(timestampdiff(day,encounter_datetime,rtc_date) < 14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,@start_range_high_risk,@start_range) and @end_range)
		or (encounter_type=21 and ampath_status in (9079,9080)))
	and if(encounter_type=21,ampath_status is null or ampath_status in (6101,1286,9080,9079),1)
) d1
group by d1.location_id;



select d1.location_id, l.name,
	count(if(risk_category_present >= 0,1,null)) as total,
	count(if(risk_category_present=1,1,null)) as num_pre_ltfu,
	count(if(risk_category_present=2,1,null)) as num_ltfu,
	count(if(risk_category_present=0,1,null)) as num_untraceable

from
(select
	@days_since_rtc := timestampdiff(day,rtc_date,curdate()) as days_since_rtc,
	#if(encounter_type=21 and ampath_status=1286,health_center,t1.location_id) as location_id,
	location_id,
	case 
		when next_appt_date is null and encounter_type=21 and ampath_status=9079 then 0
		when next_appt_date is null and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),if(next_appt_date is null,curdate(),@end_date)) < 90 then 1
		when next_appt_date is null then 2		
	end as risk_category_present#,
/*
	case 
		when encounter_datetime < @end_date and encounter_type=21 and ampath_status=9079 then 0
		when encounter_datetime < @end_date and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),if(next_appt_date is null,curdate(),@end_date)) < 90 then 1
		when encounter_datetime < @end_date then 2
	end as risk_category_past
*/
from
(select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 90) p0c, (select @days_back := 30) p0d, (select @end_date := adddate(curdate(), interval (-1*@days_back) day)) p0e,
reporting_JD.flat_retention_data t1
where 
	(next_appt_date is null or (encounter_datetime < @end_date and next_appt_date > @end_date))
	and if(next_appt_date is null,dead=0,death_date is null or death_date < @end_date)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),if(next_appt_date is null,curdate(),@end_date))
		between if(timestampdiff(day,encounter_datetime,rtc_date) < 14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,@start_range_high_risk,@start_range) and @end_range
#	and location_id=1
) d1
join amrs.location l using (location_id)
group by location_id;

# Count of defaulters during a specific time period
select location_id, name,
	count(if(risk_category_present >= 0,1,null)) as total_present,
	count(if(risk_category_present >= 0,1,null)) - count(if(risk_category_past >=0,1,null)) as total_change,


	count(if(risk_category_present=0,1,null)) as num_being_traced,
	count(if(risk_category_present=0,1,null)) - count(if(risk_category_past=0,1,null)) as num_being_traced_change,

	count(if(risk_category_present=1,1,null)) as num_high_risk,
	count(if(risk_category_present=1,1,null)) - count(if(risk_category_past=1,1,null)) as num_high_risk_change,

	count(if(risk_category_present=2,1,null)) as num_medium_risk,
	count(if(risk_category_present=2,1,null)) - count(if(risk_category_past=2,1,null)) as num_medium_risk_change,

	count(if(risk_category_present=3,1,null)) as num_low_risk,
	count(if(risk_category_present=3,1,null)) - count(if(risk_category_past=3,1,null)) as num_low_risk_change,

	count(if(risk_category_present=5,1,null)) as no_rtc,
	count(if(risk_category_present=5,1,null)) - count(if(risk_category_past=5,1,null)) as num_no_rtc_change,


	count(if(risk_category_present in (0,1,2,3,5),1,null)) as num_pre_ltfu,
	count(if(risk_category_present in (0,1,2,3,5),1,null)) - count(if(risk_category_past in (0,1,2,3),1,null)) as num_pre_ltfu_change,

	count(if(risk_category_present=4,1,null)) as num_ltfu,
	count(if(risk_category_present=4,1,null)) - count(if(risk_category_past=4,1,null)) as num_ltfu_change,


	count(if(risk_category_present=6,1,null)) as num_untraceable,
	count(if(risk_category_present=6,1,null)) - count(if(risk_category_past=6,1,null)) as num_untraceable_change
from

(select location_id, person_id, encounter_datetime,next_appt_date,encounter_type,rtc_date,cur_ampath_status,ampath_status,
	@days_since_rtc := timestampdiff(day,rtc_date,curdate()) as days_since_rtc,
	case 
		when next_appt_date is null and encounter_type=21 and ampath_status=9080 then 0
		when next_appt_date is null and encounter_type=21 and ampath_status=6101 and @days_since_rtc >= @start_range_high_risk then 1
		when next_appt_date is null and encounter_type=21 and ampath_status=9079 then 6		
		when next_appt_date is null and rtc_date is null and timestampdiff(day,encounter_datetime,curdate()) < 90 then 5
		when next_appt_date is null and (@days_since_rtc > 90 or (rtc_date is null and timestampdiff(day,encounter_datetime,curdate()) > 90)) then 4
		when next_appt_date is null and @days_since_rtc between @start_range_high_risk and 90 and (timestampdiff(day,encounter_datetime,rtc_date) <= 14 or timestampdiff(day,arv_start_date,encounter_datetime) <= 90) then 1
		when next_appt_date is null and timestampdiff(day,arv_start_date,encounter_datetime) > 90 and @days_since_rtc between @start_range and @end_range then 2
		when next_appt_date is null and @days_since_rtc between @start_range and @end_range then 3
		else null
	end as risk_category_present,
	@past_days_since_rtc := timestampdiff(day,rtc_date,@end_date) as past_days_since_rtc,
	case 
		when encounter_datetime < @end_date and encounter_type=21 and ampath_status=9080 then 0
		when encounter_datetime < @end_date and encounter_type=21 and ampath_status=6101 and @past_days_since_rtc >= @start_range_high_risk then 1
		when encounter_datetime < @end_date and encounter_type=21 and ampath_status=9079 then 6
		when encounter_datetime < @end_date and timestampdiff(day,encounter_datetime,@end_date) <= 90 and rtc_date is null then 5
		when encounter_datetime < @end_date and (@past_days_since_rtc > 90 or (rtc_date is null and timestampdiff(day,encounter_datetime,@end_date) > 90)) then 4
		when encounter_datetime < @end_date and @past_days_since_rtc >= @start_range_high_risk and @past_days_since_rtc <= 90 and (timestampdiff(day,encounter_datetime,rtc_date) <= 14 or timestampdiff(day,arv_start_date,encounter_datetime) <= 90) then 1
		when encounter_datetime < @end_date and timestampdiff(day,arv_start_date,encounter_datetime) > 90 and @past_days_since_rtc between @start_range and @end_range then 2
		when encounter_datetime < @end_date and @past_days_since_rtc between @start_range and @end_range then 3
		else null
	end as risk_category_past
from 
(select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 90) p0c, (select @days_back := 30) p0d, (select @end_date := adddate(curdate(), interval (-1*@days_back) day)) p0e,
reporting_JD.flat_retention_data t1
where 
	(next_appt_date is null or (encounter_datetime < @end_date and next_appt_date > @end_date))
	and if(next_appt_date is null,dead=0,death_date is null or death_date < @end_date)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),if(next_appt_date is null,curdate(),@end_date))
		between if(timestampdiff(day,encounter_datetime,rtc_date) < 14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,@start_range_high_risk,@start_range) and @end_range
#	and location_id=1
) d1
join amrs.location l using (location_id)
group by location_id;

select 
	count(if((dead=0 or death_date<'2013-09-30') and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),'2013-09-30') >= 90,1,null)) as ltfu_2013_09_30,
	count(if((dead=0 or death_date<'2014-03-31') and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),'2014-03-31') >= 90,1,null)) as ltfu_2014_03_31,
	count(if(timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),'2013-09-30') >= 90,1,null)) -
	count(if(timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),'2014-03-31') >= 90,1,null)) as diff
	
from flat_retention_data
where next_hiv_clinic_date is null and encounter_type!=21

# List of patients who say the did not miss their appointment


select t3.name as clinic, 
	identifier,	
	person_name as name, 
	t2.prev_hiv_clinic_date as 'last clinic appt',
	t2.prev_rtc_date as 'previous rtc date',
	t1.encounter_datetime as 'follow up date',
	t1.rtc_date_outreach as 'rtc date from outreach'
from 
reporting_JD.flat_outreach_data t1
join reporting_JD.flat_retention_data t2 using (encounter_id)
join amrs.location t3 on t3.location_id = t1.location_id
where 
t1.encounter_type=21
and reason_missed_appt regexp '[[:<:]]1574[[:>:]]'
and form_id=457
order by clinic,identifier,t1.encounter_datetime, person_name;

# Get list of outreach workers
select t1.provider_id, given_name, family_name
from flat_outreach_data t1
join amrs.person_name t2 on t1.provider_id=t2.person_id
group by t1.provider_id;

# Number of LTFU at a given time point, using clinic as last appt
select * from amrs.location
select location_id,name,count(*) as num_ltfu
from
(select @date_1 := last_day('2014-03-30')) p0d,
reporting_JD.flat_retention_data t1
join amrs.location using (location_id)
where 
	(encounter_datetime <= @date_1 and (next_hiv_clinic_date is null or next_hiv_clinic_date > @date_1))
	and if(dead,death_date < @date_1,1)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and arv_start_date <= @date_1
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),@date_1) >= 90
	and encounter_type != 21
	and location_id in (65,64,55,83,4,19,26,27,90,60,7,24,25,98,102,57,81,56,58,69,17,86,107,62,23,11,91,74,76,72,77,71,2,9,1,13,14,15,78,130,100,70,54,20,109,103,82,75,105,73,104,94,12,88,3,31,8,28)	
group by location_id;


# Number of LTFU at a given time point, using any encounter as last appt

select location_id,name,count(*) as num_ltfu, count(if(encounter_type=21,1,null)) as num_outreach
from
(select @date_1 := last_day('2014-05-31')) p0d,
reporting_JD.flat_retention_data t1
join amrs.location using (location_id)
where 
	(encounter_datetime <= @date_1 and (next_hiv_clinic_date is null or next_hiv_clinic_date > @date_1))
	and if(dead,death_date < @date_1,1)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
#	and arv_start_date <= @date_1
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),@date_1) >= 90
	and encounter_type != 21
	and location_id in (65,64,55,83,4,19,26,27,90,60,7,24,25,98,102,57,81,56,58,69,17,86,107,62,23,11,91,74,76,72,77,71,2,9,1,13,14,15,78,130,100,70,54,20,109,103,82,75,105,73,104,94,12,88,3,31,8,28)	
group by location_id;


# test
select person_id, encounter_type, encounter_datetime, rtc_date, next_appt_date, next_hiv_clinic_date
from
(select @date_1 := last_day('2014-05-31')) p0d,
reporting_JD.flat_retention_data t1
join amrs.location using (location_id)
where 
	(encounter_datetime <= @date_1 and (next_hiv_clinic_date is null or next_hiv_clinic_date > @date_1))
	and if(dead,death_date < @date_1,1)
	and hiv_start_date	
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
#	and arv_start_date <= @date_1
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),@date_1) >= 90
#	and encounter_type != 21
#	and location_id in (65,64,55,83,4,19,26,27,90,60,7,24,25,98,102,57,81,56,58,69,17,86,107,62,23,11,91,74,76,72,77,71,2,9,1,13,14,15,78,130,100,70,54,20,109,103,82,75,105,73,104,94,12,88,3,31,8,28)	
and location_id=1
	and if(encounter_type=21,ampath_status is null or ampath_status not in (159,9036,1287),1)
group by location_id;

