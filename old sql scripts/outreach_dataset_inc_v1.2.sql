
create table if not exists flat_outreach_defined (
	encounter_id int,
	person_id int,
	enc_date_created datetime,
	date_of_missed_appt datetime,
	reason_for_outreach int,
	call_attempts int,
	patient_contacted_by_phone int, 
	patient_referred_for_field_follow_up int,
	reason_field_follow_up_not_attempted int,
	field_attempt_method int,
	patient_found_in_field int,
	reason_not_found_in_field int,
	days_meds_remaining int,
	final_attempt int,
	date_found datetime,
	contact_location int,
	reason_missed_appt varchar(1000),
	death_date datetime,
	death_cause int,
	reason_for_refusing_care int,
	likelihood_of_returning int,
	ampath_status_outreach int,
	update_locator_info int,
	rtc_date_outreach datetime,
	obs_date_created datetime,
	index encounter_id (encounter_id),
	index enc_date_created (enc_date_created),
	index obs_date_created (obs_date_created));

select @init_count := count(*) from flat_outreach_defined;

select @last_enc_date_created := max(enc_date_created) from flat_outreach_defined;
select @last_enc_date_created := if(@last_enc_date_created,@last_enc_date_created,'1900-01-01');

select @last_obs_date_created := max(obs_date_created) from flat_outreach_defined;
select @last_obs_date_created := if(@last_obs_date_created,@last_obs_date_created,'1900-01-01');

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created and concept_id in (1592,9081,9062,9063,9067,9069,1558,1553,1559,1552,1816,9073,1568,1569,1733,1570,1573,1588,9077,9082,1583,5096,1777) );

drop temporary table if exists enc;
create temporary table enc (primary key encounter_id (encounter_id), index person_id (person_id))
(select e.patient_id as person_id, e.encounter_id, e.date_created as enc_date_created
from amrs.encounter e
where e.voided=0
and e.date_created > @last_enc_date_created
and encounter_type=21
);

insert ignore into enc
(select e.patient_id as person_id, e.encounter_id, e.date_created as enc_date_created
from amrs.encounter e
join voided_obs v using (encounter_id)
where e.date_created <= @last_enc_date_created and e.voided=0 and v.encounter_id is not null and e.encounter_type=21
);

# add in encounters which have new relevant obs attached to them
insert ignore into enc
(select e.patient_id as person_id, e.encounter_id, e.date_created as enc_date_created
from amrs.encounter e
join amrs.obs o use index (date_created) using (encounter_id) 
where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and concept_id in (1592,9081,9062,9063,9067,9069,1558,1553,1559,1552,1816,9073,1568,1569,1733,1570,1573,1588,9077,9082,1583,5096,1777) and encounter_type=21
);

delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';



drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index encounter_id (encounter_id))
(select * from amrs.obs o use index (date_created) where concept_id in (1592,9081,9062,9063,9067,9069,1558,1553,1559,1552,1816,9073,1568,1569,1733,1570,1573,1588,9077,9082,1583,5096,1777) and o.voided=0 and date_created > @last_obs_date_created);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_obs_date_created and t2.encounter_id is not null and t1.concept_id in (1592,9081,9062,9063,9067,9069,1558,1553,1559,1552,1816,9073,1568,1569,1733,1570,1573,1588,9077,9082,1583,5096,1777));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o use index (date_created) using (encounter_id) where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and concept_id in (1592,9081,9062,9063,9067,9069,1558,1553,1559,1552,1816,9073,1568,1569,1733,1570,1573,1588,9077,9082,1583,5096,1777));

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
	min(if(concept_id=1570,value_datetime,null)) as death_date,
	min(if(concept_id=1573,value_coded,null)) as death_cause,
	min(if(concept_id=1588,value_coded,null)) as reason_for_refusing_care,
	min(if(concept_id=9077,value_coded,null)) as likelihood_of_returning,
	min(if(concept_id=9082,value_coded,null)) as ampath_status_outreach,
	min(if(concept_id=1583,value_coded,null)) as update_locator_info,
	min(if(concept_id in (1777,5096),value_datetime,null)) as rtc_date_outreach,
	max(date_created) as obs_date_created
	from obs_subset o
	group by encounter_id
);

#remove any encounters that have a voided obs. 
drop table if exists encounters_to_be_removed;
create temporary table encounters_to_be_removed (primary key encounter_id (encounter_id))
(select distinct encounter_id from voided_obs);

#remove any encounters that have been voided.
insert ignore into encounters_to_be_removed
(select encounter_id from amrs.encounter where voided=1 and date_created <= @last_enc_date_created and date_voided > @last_enc_date_created);

#remove any encounters with new obs as the entire encounter will be rebuilt and added back
insert ignore into encounters_to_be_removed
(select e.encounter_id from enc e);


delete t1
from flat_outreach_defined t1
join encounters_to_be_removed t2 using (encounter_id);

insert into flat_outreach_defined
(select *
from enc e1 
left outer join n_obs n1 using (encounter_id)
order by e1.person_id
);

drop view if exists flat_outreach_data;
create view flat_outreach_data as
(select t1.*,r.location_id, r.provider_id,prev_appt_date, r.encounter_datetime, next_appt_date, prev_encounter_type, r.encounter_type, next_encounter_type, prev_rtc_date,r.next_hiv_clinic_date,r.prev_hiv_clinic_date,e.form_id  
	from flat_outreach_defined t1
	join flat_retention_data r using (encounter_id)
	join amrs.encounter e using (encounter_id)
);

select (count(*) - @init_count) as 'flat_outreach_defined new rows' from flat_outreach_defined;