drop table if exists obs_subset;
create temporary table obs_subset (index encounter_id (encounter_id))
(select obs_id, obs_datetime, person_id, encounter_id, concept_id,value_coded, value_numeric,value_datetime,date_created,voided,date_voided
	from amrs.obs o
	where o.voided=0 and concept_id in (1592,9081,9062,9063,9067,9069,1558,1553,1559,1552,1816,9073,1568,1569,1733,1570,1573,1588,9077,9082,1583,5096)
);

drop table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select 
	encounter_id,
	min(if(concept_id=1592,value_datetime,null)) as date_of_missed_appt,
	min(if(concept_id=9081,value_coded,null)) as reason_for_outreach,
	min(if(concept_id=9062,value_numeric,null)) as call_attempts,
	min(if(concept_id=9063,value_coded,null)) as patient_contacted_by_phone,
	min(if(concept_id=9067,value_coded,null)) as patient_referred_for_field_follow_up,
	min(if(concept_id=9069,value_coded,null)) as reason_field_follow_up_not_attempted,
	min(if(concept_id=1558,value_coded,null)) as field_attempt_method,
	min(if(concept_id=1559,value_coded,null)) as patient_found_in_field,
	min(if(concept_id=1552,value_coded,null)) as reason_not_found_in_field,
	min(if(concept_id=1816,value_numeric,null)) as days_meds_remaining,
	min(if(concept_id=9073,value_coded,null)) as final_attempt,
	min(if(concept_id=1568,value_datetime,null)) as date_found,
	min(if(concept_id=1569,value_coded,null)) as contact_location,
	group_concat(if(concept_id=1733,value_coded,null) order by value_coded separator ' // ') as reason_missed_appt,
	min(if(concept_id=1570,value_coded,null)) as death_date,
	min(if(concept_id=1573,value_coded,null)) as death_cause,
	min(if(concept_id=1588,value_coded,null)) as reason_for_refusing_care,
	min(if(concept_id=9077,value_coded,null)) as likelihood_of_returning,
	min(if(concept_id=9082,value_coded,null)) as ampath_status_outreach,
	min(if(concept_id=1583,value_coded,null)) as update_locator_info,
	min(if(concept_id in (5096),value_datetime,null)) as rtc_date_outreach

	from obs_subset o
	group by encounter_id
);


drop table if exists flat_outreach_data;
create table flat_outreach_data (next_encounter_type int, reason_missed_appt varchar(300), index encounter_id (encounter_id))
(select o.*, location_id, person_id, r.provider_id, prev_appt_date, r.encounter_datetime, next_appt_date, prev_encounter_type, r.encounter_type, next_encounter_type, prev_rtc_date, e.form_id
	from n_obs o
	join flat_retention_data r using (encounter_id)
	join amrs.encounter e using (encounter_id)
	where r.encounter_type = 21
);

