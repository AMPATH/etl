# v2.0 notes: add join to flat obs so that patients with an untraceable status can be excluded
# v2.2 notes : removed flat_obs join after update to out_of_care
# v2.3 notes : excluded encounter_type 116 (TRANSFERFORM). Fixes bug where transferred out patients appears in ‘Defaulters list

select @table_version := "flat_defaulters_v2.3";
select @start := now();
select @last_date_created := (select max(max_date_created) from flat_obs);

####################################################

drop table if exists flat_defaulters_0;
create temporary table flat_defaulters_0 (encounter_id int, primary key (encounter_id))
(select
	encounter_id,
	person_id,
	t1.location_id,
	t1.location_uuid,
	t1.uuid as patient_uuid,
	timestampdiff(day,if(rtc_date,rtc_date,date_add(t1.encounter_datetime,interval 90 day)),curdate()) as days_since_rtc,
	date(t1.encounter_datetime) as encounter_datetime,
	date(rtc_date) as rtc_date,
	arv_start_date,
	t1.encounter_type

from flat_hiv_summary_v15b t1
where next_encounter_datetime_hiv is null #and rtc_date <= date_sub(now(),interval 90 day)
#	and if(rtc_date,date_add(rtc_date,interval 90 day),date_add(t1.encounter_datetime,interval 180 day)) <= now()
	and if(rtc_date,rtc_date,date_add(t1.encounter_datetime,interval 90 day)) < now()
	and death_date is null
  and encounter_type!=116
	and out_of_care is null
	and transfer_out is null
);

drop table if exists universal_ids;
create temporary table universal_ids (person_id int, primary key (person_id))
(select
	person_id,
	group_concat(if(t2.identifier_type = 8,concat(substring(identifier,8,2),"-",substring(identifier,6,2),"-",substring(identifier,4,2),"-",substring(identifier,2,2),"-",substring(identifier,1,1)),null)separator ', ') as filed_id,
	group_concat(distinct t2.identifier separator ', ') as identifiers
	from flat_defaulters_0 t1
	join amrs.patient_identifier t2 on t1.person_id = t2.patient_id
	where t2.voided=0
	group by person_id
);


#drop table if exists flat_defaulters;
#create table flat_defaulters (


drop table if exists flat_defaulters;
create table flat_defaulters (
	person_id int,
	location_id int,
	location_uuid varchar (50),
	primary key (person_id),
	index locuuid_rtc (location_uuid,days_since_rtc),
	index locid_rtc (location_id,days_since_rtc)
)
(select
	t0.patient_uuid,
	t0.person_id,
	t0.encounter_id,
	t0.location_id,
	t0.location_uuid,
	t0.days_since_rtc,
	t0.encounter_datetime,
	t0.rtc_date,
	t0.arv_start_date,
	t1.name as encounter_type_name,
	concat(COALESCE(t2.given_name,''),' ',COALESCE(t2.middle_name,''),' ',COALESCE(t2.family_name,''))  as person_name,
	t3.value as phone_number,
	t4.identifiers,
	t4.filed_id

from flat_defaulters_0 t0
	join amrs.encounter_type t1 on t0.encounter_type = t1.encounter_type_id
	left outer join amrs.person_name t2 on t0.person_id = t2.person_id and t2.voided=0
	left outer join amrs.person_attribute t3 on t0.person_id = t3.person_id and t3.person_attribute_type_id=10 and t3.voided=0
	left outer join universal_ids t4 on t0.person_id = t4.person_id
 group by t0.person_id
);

# Remove test patients
delete t1
from flat_defaulters t1
join amrs.person_attribute t2 using (person_id)
where t2.person_attribute_type_id=28 and value='true' and voided=0;
#select * from flat_defaulters;

select @end := now();
insert into flat_log values (@start, @last_date_created,@table_version,timestampdiff(second,@start,@end));
#insert into flat_log values (@last_date_created,@table_version,timestampdiff(second,@start,@end));



select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");