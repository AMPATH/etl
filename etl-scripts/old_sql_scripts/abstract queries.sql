# of patients followed-up
select year(encounter_datetime) as year, month(encounter_datetime) as month, count(*)
	from amrs.encounter #flat_outreach_data
	where encounter_type=21
		and encounter_datetime >= '2013-01-01'
		and location_id in (1,13,14,15)
		#and if(ampath_status_outreach,ampath_status_outreach != 9080,1)
	group by year, month
	order by year, month;



# LTFU ever
select count(*) as num_ltfu, count(if(arv_start_date,if(arv_start_date <= @date_1,1,null),null)) as num_ltfu_on_arvs
from
(select @date_1 := last_day('2014-04-01')) p0d,
reporting_JD.flat_retention_data t1
left outer join reporting_JD.flat_outreach_defined t2 using (encounter_id)
where 
	(t1.encounter_datetime <= @date_1 
		and (next_appt_date is null or next_appt_date > @date_1)
	) 
	and if(encounter_type=21,timestampdiff(day,date_add(prev_hiv_clinic_date, interval 30 day),@date_1) > 90,1)

	and if(t1.dead,t1.death_date < @date_1,1)	
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),@date_1) >= 90
	and location_id in (1,13,14,15)
	and if(ampath_status_outreach,ampath_status_outreach in (9079,9080,6102),1);


select 
	year(encounter_datetime) as year, 
	month(encounter_datetime) as month,
	count(*) as num_followed_up,
	count(if(encounter_type=21 and next_hiv_clinic_date is not null,1,null)) as num_returned
from flat_outreach_data
where 
	location_id in (1,13,14,15)
	and if(ampath_status_outreach,ampath_status_outreach != 9080,1)
	and encounter_datetime >= '2013-06-01'
group by year, month
order by year, month

# LTFU numbers

#select * from amrs.location
# LTFU ever
# select count(*) as num_ltfu, count(if(arv_start_date,if(arv_start_date <= @date_1,1,null),null)) as num_ltfu_on_arvs

(select t1.person_id, encounter_type, encounter_datetime, rtc_date,next_appt_date, next_hiv_clinic_date
from
(select @date_1 := last_day('2014-04-30')) p0d,
reporting_JD.flat_retention_data t1
left outer join reporting_JD.flat_outreach_defined t2 using (encounter_id)
where 
	(t1.encounter_datetime <= @date_1 
		and (next_appt_date is null or next_appt_date > @date_1)
	) 
	and if(t1.dead,t1.death_date < @date_1,1)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime,interval 30 day)),@date_1) >= 90
	and location_id in (13)
	and if(ampath_status_outreach,ampath_status_outreach in (9079,9080),1)
order by encounter_datetime
)


# LTFU if no HIV clinic
select count(distinct t1.person_id) as num_ltfu, count(distinct if(arv_start_date,if(arv_start_date <= @date_1,t1.person_id,null),null)) as num_ltfu_on_arvs
from
(select @date_1 := last_day('2014-07-30')) p0d,
reporting_JD.flat_retention_data t1
left outer join reporting_JD.flat_outreach_defined t2 using (encounter_id)
where 
	(t1.encounter_datetime <= @date_1 
		and (next_hiv_clinic_date is null or next_hiv_clinic_date > @date_1)
	) 
	and if(encounter_type=21,timestampdiff(day,date_add(prev_hiv_clinic_date, interval 30 day),@date_1) > 90,1)
	and if(t1.dead,t1.death_date < @date_1,1)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),@date_1) >= 90
	and location_id in (1,13,14,15)
	and if(ampath_status_outreach,ampath_status_outreach in (9079,9080,6102),1);



# MOdified LTFU ever
select count(*) as num_ltfu, count(if(arv_start_date,if(arv_start_date <= @date_1,1,null),null)) as num_ltfu_on_arvs
from
(select @date_1 := last_day('2013-03-01')) p0,
reporting_JD.flat_retention_data t1
left outer join reporting_JD.flat_outreach_defined t2 using (encounter_id)
where 
	(t1.encounter_datetime <= @date_1 
		and (next_appt_date is null or next_appt_date > @date_1)
	) 
	and if(encounter_type=21,timestampdiff(day,date_add(prev_hiv_clinic_date, interval 30 day),@date_1) > 90,timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),@date_1) > 90)
	and if(t1.dead,t1.death_date > @date_1,1)	
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and location_id in (1,13,14,15)
	and if(ampath_status_outreach,ampath_status_outreach in (9079,9080,6102),1);


select count(*)
from flat_

select count(*) as num_ltfu, count(if(arv_start_date,if(arv_start_date <= @date_1,1,null),null)) as num_ltfu_on_arvs,
count(if(encounter_type=21,1,null)) as last_enc_outreach

from
(select @date_1 := last_day('2013-03-01')) p0,
reporting_JD.flat_retention_data t1
left outer join reporting_JD.flat_outreach_defined t2 using (encounter_id)
where 
	(t1.encounter_datetime <= @date_1 
		and (next_appt_date is null or next_appt_date > @date_1)
	) 
	and if(encounter_type=21,timestampdiff(day,date_add(prev_hiv_clinic_date, interval 30 day),@date_1) > 90,timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime, interval 30 day)),@date_1) > 90)
	and if(t1.dead,t1.death_date > @date_1,1)	
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and location_id in (1,13,14,15)
	and if(ampath_status_outreach,ampath_status_outreach in (9079,9080,6102),1);


select avg(timestampdiff(day,prev_rtc_date,encounter_datetime))
from flat_outreach_data
where encounter_datetime between '2013-04-01' and '2014-03-31' and prev_encounter_type!= 21

select avg(timestampdiff(day,prev_rtc_date,encounter_datetime))
from flat_outreach_data
where encounter_datetime between '2014-04-01' and '2014-06-31' and prev_encounter_type!= 21

select count(if(timestampdiff(day,rtc_date_outreach,next_hiv_clinic_date)<90,1,null)) as returned_90,
	count(*) as num_outreach_visits
	from flat_outreach_data
	where 
		location_id in (1,13,14,15)
		and encounter_datetime between '2014-04-01' and '2014-06-31'
		and rtc_date_outreach
		and prev_encounter_type!= 21

select count(if(timestampdiff(day,rtc_date_outreach,next_hiv_clinic_date)<90,1,null)) as returned_90,
	count(*) as num_outreach_visits
	from flat_outreach_data
	where 
		location_id in (1,13,14,15)
		and encounter_datetime between '2013-04-01' and '2014-03-31'
		and rtc_date_outreach
		and prev_encounter_type!= 21



# % of patients returning to clinic
select 
	count(if(rtc_date,1,null)) as num_with_rtc_date,
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) < 0 ,1,null)) as before_0,	
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) = 0 ,1,null)) as on_0,	

	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) between 1 and 7,1,null)) as 1_to_7,
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) <= 30,1,null)) as before_31,
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) between 1 and 30,1,null)) as 1_to_30,
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) > 7,1,null)) as after_7,

	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) between 1 and 14,1,null)) as 1_to_14,
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) > 14,1,null)) as after_14,
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) between 14 and 30,1,null)) as 14_30,

	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) between 8 and 30,1,null)) as 8_30,
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) > 30,1,null)) as after_30,

	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) > 0 ,1,null)) as after_0,
	count(if(next_hiv_clinic_date is null,1,null)) as num_no_return

from
	flat_retention_data
where
	encounter_type != 21;


# % of patients found by outreach
select count(*),
	count(if(patient_found_in_field=1065,1,null)) as found
	from flat_outreach_data
	where encounter_datetime between '2013-07-01' and '2014-03-31'

# 13251/18890

select count(*),
	count(if(ampath_status_outreach in (9079,9080),1,null)) as found
	from flat_outreach_data
	where encounter_datetime between '2014-04-01' and '2014-06-30'
		and if(ampath_status_outreach=9080,next_appt_date is null,1)
