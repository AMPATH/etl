drop table if exists flat_defaulters;
create table flat_defaulters (person_id int, primary key (person_id), index loc_person (location_id,person_id))
(select 
	t1.person_id, 
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




describe amrs.person_name
drop table if exists flat_defaulters;
create table flat_defaulters (person_id int, primary key (person_id), index loc_person (location_id,person_id));
create temporary table foo
(select 
	t1.person_id, 
	t1.location_id,
	t4.uuid as location_uuid,
	t1.uuid as patient_uuid,
	t5.encounter_type,
	date(t1.encounter_datetime) as encounter_datetime, 
	et.name, 
	date(rtc_date) as rtc_date,
	concat(t6.given_name,' ',t6.middle_name,' ',t6.family_name) as person_name, 
#	phone_number,
#	identifier,
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
join amrs.location t4 using (location_id)
join amrs.encounter t5 using (encounter_id)
join amrs.encounter_type et on et.encounter_type_id = t5.encounter_type
join amrs.person_name t6 on t6.person_id = t1.person_id

where 
	encounter_type != '99999'
	and next_encounter_datetime is null 
	and death_date is null
	and not obs regexp "1596=" 
	and not obs regexp "1946=1065"
	and transfer_out is null
	and if(rtc_date,rtc_date,date_add(t1.encounter_datetime, interval 90 day)) < curdate()
	and (if(encounter_type=21,(not obs regexp "9802=" or obs regexp "9802=(6101|1286|9080)"),1))
);

create index next_enc on derived_encounter (next_encounter_datetime);

