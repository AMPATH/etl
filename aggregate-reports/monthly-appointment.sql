

select * 
from
(select 
	date(convert_tz(t1.encounter_datetime,"+00:00","+03:00")) as d, 
	count(distinct t1.person_id) as attended,
	count(distinct if(abs(timestampdiff(day,t1.prev_rtc_date, t1.encounter_datetime)) <= 14,t1.person_id,null)) as attended_and_scheduled_within_14,
	count(distinct if(timestampdiff(day,t1.prev_rtc_date,t1.encounter_datetime) > 14,t1.person_id,null)) as attended_and_scheduled_after_14
	from flat_hiv_summary t1
		join amrs.encounter t2 using (encounter_id)
	where 
		t1.location_uuid = "090050ce-1352-11df-a1f1-0026b9348838"
		and t1.encounter_datetime between "2015-08-01" and "2015-08-31"
		and t2.encounter_type != 21
	group by d
) t1
join
(select 
	date(convert_tz(t1.rtc_date,"+00:00","+03:00")) as d,  
	count(distinct t1.person_id) as scheduled,
	count(distinct if(next_clinic_datetime is not null,t1.person_id,null)) as scheduled_and_attended,
	count(distinct if(next_clinic_datetime is null,t1.person_id,null)) as has_not_returned, 
	count(distinct if(abs(timestampdiff(day,rtc_date,next_clinic_datetime)) <= 14,t1.person_id,null)) as scheduled_and_attended_within_14,
	count(distinct if(timestampdiff(day,rtc_date,next_clinic_datetime) > 14,t1.person_id,null)) as scheduled_and_attended_after_14,
	count(distinct if(next_encounter_type=21,t1.person_id,null)) as next_visit_by_outreach

	from flat_hiv_summary t1
	join derived_encounter t2 using (encounter_id)
	where 
		t1.location_uuid = "090050ce-1352-11df-a1f1-0026b9348838"
		and t1.rtc_date between "2015-08-01" and "2015-08-31"
	group by d
) t2 on t1.d = t2.d