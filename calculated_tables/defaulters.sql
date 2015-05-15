drop table if exists flat_defaulters;
create table flat_defaulters (
	person_id int, 
	location_id int, 
	location_uuid varchar (50), 
	days_since_rtc int, 
	primary key (person_id), 
	index loc_person (location_uuid,days_since_rtc)
)
(
select 
	t1.person_id, 
	t1.encounter_id,
	t1.location_id,
	t1.location_uuid,
	date(t1.encounter_datetime) as encounter_datetime, 
	date(rtc_date) as rtc_date,
	arv_start_date,
	@days_since_rtc := timestampdiff(day,if(rtc_date,rtc_date,date_add(t1.encounter_datetime,interval 90 day)),curdate()) as days_since_rtc,
	case 
		when encounter_type=21 and obs regexp "!!9082=9080!!" then 0
		when encounter_type=21 and obs regexp "!!9082=6101!!" and @days_since_rtc >= @start_range_high_risk then 1
		when rtc_date is null and timestampdiff(day,t1.encounter_datetime,curdate()) < 90 then 5
		when (@days_since_rtc > 90 or (rtc_date is null and timestampdiff(day,t1.encounter_datetime,curdate()) >= 90)) then 4
		when @days_since_rtc between @start_range_high_risk and 90 and (timestampdiff(day,t1.encounter_datetime,rtc_date) <= 14 or timestampdiff(day,arv_start_date,t1.encounter_datetime) <= 90) then 1
		when timestampdiff(day,arv_start_date,t1.encounter_datetime) > 90 and @days_since_rtc between @start_range and @end_range then 2
		when @days_since_rtc between @start_range and @end_range then 3
		else null
	end as risk_category,
	t4.name as encounter_type_name,
	concat(t5.given_name,' ',t5.middle_name,' ',t5.family_name) as person_name, 
	t6.value as phone_number,
	group_concat(t7.identifier separator ', ') as identifiers,
	t1.uuid as patient_uuid

from (select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 89) p0c,
	etl.flat_hiv_summary t1
	join etl.derived_encounter t2 using (encounter_id)
	join etl.flat_obs t3 using (encounter_id)
	join amrs.encounter_type t4 on t3.encounter_type = t4.encounter_type_id 
	join amrs.person_name t5 on t1.person_id = t5.person_id and t5.voided=0
	left outer join amrs.person_attribute t6 on t1.person_id = t6.person_id and t6.person_attribute_type_id=10 and t6.voided=0
	left outer join amrs.patient_identifier t7 on t1.person_id = t7.patient_id

where 
	encounter_type != 99999
	and next_encounter_datetime is null 
	and death_date is null
	and not obs regexp "!!1596=|!!1946=1065!!" 
	and transfer_out is null
	and if(rtc_date,rtc_date,date_add(t1.encounter_datetime, interval 90 day)) < curdate()
	and (if(encounter_type=21,(not obs regexp "!!9802=" or obs regexp "!!9802=(6101|1286|9080)!!"),1))
group by t1.person_id
);

