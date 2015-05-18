select @start := now();

####################################################

drop table if exists flat_defaulters_0;
create temporary table flat_defaulters_0 (encounter_id int, primary key (encounter_id))
(select 
	encounter_id
from flat_hiv_summary t1
join derived_encounter using (encounter_id)
where next_encounter_datetime is null #and rtc_date <= date_sub(now(),interval 90 day) 
	and if(rtc_date,date_add(rtc_date,interval 90 day),date_add(t1.encounter_datetime,interval 180 day)) <= now()
	and death_date is null
	and transfer_out is null
);


drop table if exists flat_defaulters;
create table flat_defaulters (
	person_id int, 
	location_id int, 
	location_uuid varchar (50), 
	days_since_rtc int, 
	primary key (person_id), 
	index loc_person (location_uuid,days_since_rtc)
)
(select 
	t1.person_id, 
	t1.encounter_id,
	t1.location_id,
	t1.location_uuid,
	date(t1.encounter_datetime) as encounter_datetime, 
	date(rtc_date) as rtc_date,
	arv_start_date,
	t4.name as encounter_type_name,
	concat(t5.given_name,' ',t5.middle_name,' ',t5.family_name) as person_name, 
	t6.value as phone_number,
	group_concat(t7.identifier separator ', ') as identifiers,
	t1.uuid as patient_uuid

from (select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 89) p0c,
	flat_defaulters_0 
	join etl.flat_hiv_summary t1 using (encounter_id)
	join etl.derived_encounter t2 using (encounter_id)
	join amrs.encounter t3 using (encounter_id)
	join amrs.encounter_type t4 on t3.encounter_type = t4.encounter_type_id 
	join amrs.person_name t5 on t1.person_id = t5.person_id and t5.voided=0
	left outer join amrs.person_attribute t6 on t1.person_id = t6.person_id and t6.person_attribute_type_id=10 and t6.voided=0
	left outer join amrs.patient_identifier t7 on t1.person_id = t7.patient_id

where 
	next_encounter_datetime is null 
	and death_date is null
	and out_of_care is null
group by t1.person_id
);

select concat("Time to complete flat_defaulters table: ",timestampdiff(minute, @start, now())," minutes");

