select max(enc_date_created) from flat_outreach_data;
select max(date_created) from amrs.encounter

select ampath_status_outreach, count(*), 
	count(if(next_appt_date is null,1,null)) as num_not_back,
	count(if(next_appt_date is null and arv_start_date,1,null)) as num_not_back_on_arvs,
	count(if(encounter_type=21 and arv_start_date,1,null)) as outreach_on_arvs,
	count(if(encounter_type=21 and arv_start_date and next_appt_date is null,1,null)) as outreach_on_arvs

	from flat_outreach_defined t1
	join flat_retention_data t2 using (encounter_id)
	where 
		t2.encounter_datetime >= "2014-09-20"
		and if(ampath_status_outreach=9080,next_appt_date is null,1)
	group by ampath_status_outreach with rollup;




select count(*), 
		count(if(encounter_type=21,1,null)) as num_outreach,
		count(if(encounter_type=21 and rtc_date,1,null)) as num_outreach_with_rtc,
		count(if(encounter_type=21 and encounter_datetime >= "2014-09-20",1,null)) as recent_outreach,
		count(if(prev_encounter_type=21 and prev_appt_date >= "2014-09-20",1,null)) as prev_outreach
		from flat_retention_data
	where arv_start_date
		and if(encounter_type=21,ampath_status=6101,1)
		and next_appt_date is null
		and dead=0
		and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) < 90


select name, count(*)
	from flat_outreach_data t1
	join amrs.location t2 using (location_id)
	where t1.encounter_datetime >= "2014-09-20"
	group by name


select year(encounter_datetime) as year, month(encounter_datetime) as month, 
	count(*),
	count(if(timestampdiff(day,rtc_date,next_hiv_clinic_date) < 30,1,null)) as returned,
	count(if(next_hiv_clinic_date is not null and next_appt_date=next_hiv_clinic_date,1,null)) as returned_2
	from flat_retention_defined t1 use index (enc_datetime)
		join flat_retention_derived t2 using (encounter_id)
		join flat_outreach_defined t3 using (encounter_id)
	where
		if(ampath_status_outreach=9080,next_appt_date is null,1)
	group by year, month;
