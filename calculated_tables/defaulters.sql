drop table if exists flat_defaulters;
create table flat_defaulters (person_id int, primary key (person_id), index loc_person (location_id,person_id), index encounter_id (encounter_id))
(select 
	t1.person_id, 
	t1.encounter_id,
	t1.location_id,
	t2.uuid as location_uuid,
	t1.uuid as patient_uuid,
	t1.encounter_type,
	date(encounter_datetime) as encounter_datetime, et.name, 
	date(rtc_date) as rtc_date,person_name, phone_number,identifier,arv_start_date,
	@days_since_rtc := timestampdiff(day,if(rtc_date,rtc_date,date_add(encounter_datetime,interval 90 day)),curdate()) as days_since_rtc,
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
from (select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 89) p0c,
reporting_JD.flat_retention_data t1
join amrs.encounter_type et on et.encounter_type_id = t1.encounter_type
join amrs.location t2 using (location_id)
where 
	next_appt_date is null 
	and dead=0
	and reason_exited_care is null
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care=1286)
	and if(rtc_date,rtc_date,date_add(encounter_datetime, interval 90 day)) < curdate()
	and (if(encounter_type=21,ampath_status is null or ampath_status in (6101,1286,9080),1))
);


drop table if exists flat_defaulters;
create table flat_defaulters (person_id int, location_id int, primary key (person_id), index loc_person (location_id,person_id));
explain
(select 
	t1.person_id, 
	t1.encounter_id,
	t1.location_id,
	date(t1.encounter_datetime) as encounter_datetime, 
	date(rtc_date) as rtc_date,
	arv_start_date,
	@days_since_rtc := timestampdiff(day,if(rtc_date,rtc_date,date_add(t1.encounter_datetime,interval 90 day)),curdate()) as days_since_rtc,
	case 
		when encounter_type=21 and obs regexp "9082=9080" then 0
		when encounter_type=21 and obs regexp "9082=6101" and @days_since_rtc >= @start_range_high_risk then 1
		when rtc_date is null and timestampdiff(day,t1.encounter_datetime,curdate()) < 90 then 5
		when (@days_since_rtc > 90 or (rtc_date is null and timestampdiff(day,t1.encounter_datetime,curdate()) >= 90)) then 4
		when @days_since_rtc between @start_range_high_risk and 90 and (timestampdiff(day,t1.encounter_datetime,rtc_date) <= 14 or timestampdiff(day,arv_start_date,t1.encounter_datetime) <= 90) then 1
		when timestampdiff(day,arv_start_date,t1.encounter_datetime) > 90 and @days_since_rtc between @start_range and @end_range then 2
		when @days_since_rtc between @start_range and @end_range then 3
		else null
	end as risk_category
from (select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 89) p0c,
reporting_JD.flat_hiv_summary t1
join reporting_JD.derived_encounter t2 using (encounter_id)
join reporting_JD.flat_obs t3 using (encounter_id)
join amrs.encounter t4 using (encounter_id)

where 
	encounter_type != 99999
	and next_encounter_datetime is null 
	and death_date is null
	and not obs regexp "1596=|1946=1065" 
	and transfer_out is null
	and if(rtc_date,rtc_date,date_add(t1.encounter_datetime, interval 90 day)) < curdate()
	and (if(encounter_type=21,(not obs regexp "9802=" or obs regexp "9802=(6101|1286|9080)"),1))
);

drop view defaulter_list;
create view defaulter_list as
(select 
	t1.person_id, 
	t3.name as encounter_type_name,
	concat(t4.given_name,' ',t4.middle_name,' ',t4.family_name) as person_name, 
	t5.value as phone_number,
	group_concat(t6.identifier separator ', ') as identifiers,
	t7.uuid as patient_uuid
	from
		reporting_JD.flat_defaulters t1 
		join amrs.encounter t2 using (encounter_id)
		join amrs.encounter_type t3 on t2.encounter_type = t3.encounter_type_id 
		join amrs.person_name t4 on t1.person_id = t4.person_id
		left outer join amrs.person_attribute t5 on t1.person_id = t5.person_id and t5.person_attribute_type_id=10 and t5.voided=0
		join amrs.patient_identifier t6 on t2.patient_id = t6.patient_id
		join amrs.person t7 on t1.person_id = t7.person_id
	group by t1.person_id
	limit 10000,10000
);

