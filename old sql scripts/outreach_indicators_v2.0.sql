# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26)
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)


# indicators stratified by provider
select
	provider_id,
	concat(family_name,', ',given_name) as name,
	count(*) as '# of forms',
	count(distinct date(encounter_datetime)) as '# days with a form',
	count(*)/count(distinct date(encounter_datetime)) 'forms / day',

	sum(if(patient_contacted_by_phone in (1065,1560,9065,9066),1,null)) as '# of calls',
	sum(if(patient_contacted_by_phone in (1065,1560,9065,9066),1,null)) / count(distinct if(patient_contacted_by_phone in (1065,1560,9065,9066),encounter_datetime,null)) as 'calls / day',

	sum(if(field_attempt_method != 1555,1,null)) as '# of field follow-ups',
	sum(if(field_attempt_method != 1555,1,null)) / count(distinct if(field_attempt_method !=1555,encounter_datetime,null)) as 'field follow-ups / day',

	sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as '# patients followed-up',
	sum(if(date_found is not null,1,0)) as '# found',
	sum(if(date_found is not null,1,0)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null))  as 'perc found',
	sum(if(ampath_status_outreach = 9080 and next_hiv_clinic_date is null,1,0)) as '# being traced',


	sum(if(ampath_status_outreach != 9080 and patient_contacted_by_phone=1065,1,null)) as '# reached by phone',
	sum(if(ampath_status_outreach != 9080 and patient_contacted_by_phone=1065,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as 'perc reached by phone',
	sum(if(ampath_status_outreach != 9080 and patient_contacted_by_phone=1065,1,null)) / sum(if(date_found is not null and (ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null)),1,null)) as 'perc of found, reached by phone',


	sum(if(ampath_status_outreach != 9080 and patient_found_in_field=1065,1,null)) as '# found in field',
	sum(if(ampath_status_outreach != 9080 and patient_found_in_field=1065,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as 'perc found in field',
	sum(if(ampath_status_outreach != 9080 and patient_found_in_field=1065,1,null)) / sum(if(date_found is not null and (ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null)),1,null)) as 'perc of found, found in field',

	
	sum(if(ampath_status_outreach not in (159,9036,9080),1,0)) as '# followed up, not dead, HIV+',	
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21,1,0)) as '# who returned',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21,1,0)) / sum(if(ampath_status_outreach not in (159,9036,9080),1,0)) as 'RTC rate, overall',

	sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,null)) as '# due to return',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,0)) as '# who returned,due to return',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,0)) / sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,null)) as 'RTC rate, due to return',

	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_contacted_by_phone=1065,1,0)) as '# returning, reached by phone',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_contacted_by_phone=1065,1,0)) / sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_contacted_by_phone=1065,1,null)) as 'RTC rate, reached by phone',

	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_found_in_field=1065,1,0)) as '# returning, found in field',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_found_in_field=1065,1,0)) / sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_found_in_field=1065,1,null)) as 'RTC rate, found in field',

	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=9079,1,0)) / sum(if(ampath_status_outreach=9079,1,0)) as 'perc returning, untraceable',
	

		
	sum(if(ampath_status_outreach=6101,1,null)) as '# planning to return',
	sum(if(ampath_status_outreach=6101,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc planning to return',
	sum(if(ampath_status_outreach = 159,1,0)) as '# deaths',
	sum(if(ampath_status_outreach=159,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc dead',
	sum(if(ampath_status_outreach=9083,1,0)) as '# self-disengaged',
	sum(if(ampath_status_outreach=9083,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc self-disengaged',

	sum(if(ampath_status_outreach = 9036,1,0)) as '# hiv neg',
	sum(if(ampath_status_outreach=9036,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc hiv neg',

	sum(if(ampath_status_outreach=9079,1,0)) as '# untraceable',
	sum(if(ampath_status_outreach=9079,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc untraceable',

	sum(if(ampath_status_outreach=9068,1,0)) as '# transfer non-amrs',
	sum(if(ampath_status_outreach=9068,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc transfer non-amrs',

	sum(if(ampath_status_outreach=1286,1,0)) as '# transfer amrs',
	sum(if(ampath_status_outreach=1286,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc transfer amrs',

	sum(if(ampath_status_outreach=1287,1,0)) as '# transfer non-ampath',
	sum(if(ampath_status_outreach=1287,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc transfer non-ampath',

	sum(if(ampath_status_outreach=9080 and next_hiv_clinic_date is null,1,0)) as '# being traced',
	sum(if(ampath_status_outreach=9080 and next_hiv_clinic_date is null,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc being traced'
	
	from (select @start_date := '2014-01-01') p0, (select @end_date := '2015-01-01') p1, flat_outreach_data t1
	join amrs.person_name t2 on t1.provider_id = t2.person_id
	where form_id=457 and ampath_status_outreach and encounter_datetime between @start_date and @end_date
	group by t1.provider_id;

# indicators stratified by clinic
select
	location_id,
	name as 'clinic',
	count(*) as '# of forms',
	count(distinct date(encounter_datetime)) as '# days with a form',
	count(*)/count(distinct date(encounter_datetime)) 'forms / day',

	sum(if(patient_contacted_by_phone in (1065,1560,9065,9066),1,null)) as '# of calls',
	sum(if(patient_contacted_by_phone in (1065,1560,9065,9066),1,null)) / count(distinct if(patient_contacted_by_phone in (1065,1560,9065,9066),encounter_datetime,null)) as 'calls / day',

	sum(if(field_attempt_method != 1555,1,null)) as '# of field follow-ups',
	sum(if(field_attempt_method != 1555,1,null)) / count(distinct if(field_attempt_method !=1555,encounter_datetime,null)) as 'field follow-ups / day',

	sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as '# patients followed-up',
	sum(if(date_found is not null,1,0)) as '# found',
	sum(if(date_found is not null,1,0)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null))  as 'perc found',
	sum(if(ampath_status_outreach = 9080 and next_hiv_clinic_date is null,1,0)) as '# being traced',


	sum(if(ampath_status_outreach != 9080 and patient_contacted_by_phone=1065,1,null)) as '# reached by phone',
	sum(if(ampath_status_outreach != 9080 and patient_contacted_by_phone=1065,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as 'perc reached by phone',
	sum(if(ampath_status_outreach != 9080 and patient_contacted_by_phone=1065,1,null)) / sum(if(date_found is not null and (ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null)),1,null)) as 'perc of found, reached by phone',


	sum(if(ampath_status_outreach != 9080 and patient_found_in_field=1065,1,null)) as '# found in field',
	sum(if(ampath_status_outreach != 9080 and patient_found_in_field=1065,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as 'perc found in field',
	sum(if(ampath_status_outreach != 9080 and patient_found_in_field=1065,1,null)) / sum(if(date_found is not null and (ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null)),1,null)) as 'perc of found, found in field',

	sum(if(reason_missed_appt regexp '[[:<:]]1574[[:>:]]',1,0)) / sum(if(reason_missed_appt is not null,1,0)) as 'perc claiming did not miss',
	
	sum(if(ampath_status_outreach not in (159,9036,9080),1,0)) as '# followed up, not dead, HIV+',	
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21,1,0)) as '# who returned',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21,1,0)) / sum(if(ampath_status_outreach not in (159,9036,9080),1,0)) as 'RTC rate, overall',

	sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,null)) as '# due to return',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,0)) as '# who returned,due to return',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,0)) / sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,null)) as 'RTC rate, due to return',

	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_contacted_by_phone=1065,1,0)) as '# returning, reached by phone',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_contacted_by_phone=1065,1,0)) / sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_contacted_by_phone=1065,1,null)) as 'RTC rate, reached by phone',

	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_found_in_field=1065,1,0)) as '# returning, found in field',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_found_in_field=1065,1,0)) / sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach and patient_found_in_field=1065,1,null)) as 'RTC rate, found in field',

	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=9079,1,0)) / sum(if(ampath_status_outreach=9079,1,0)) as 'RTC rate, untraceable',
	

		
	sum(if(ampath_status_outreach=6101,1,null)) as '# planning to return',
	sum(if(ampath_status_outreach=6101,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc planning to return',
	sum(if(ampath_status_outreach = 159,1,0)) as '# deaths',
	sum(if(ampath_status_outreach=159,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc dead',
	sum(if(ampath_status_outreach=9083,1,0)) as '# self-disengaged',
	sum(if(ampath_status_outreach=9083,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc self-disengaged',

	sum(if(ampath_status_outreach = 9036,1,0)) as '# hiv neg',
	sum(if(ampath_status_outreach=9036,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc hiv neg',

	sum(if(ampath_status_outreach=9079,1,0)) as '# untraceable',
	sum(if(ampath_status_outreach=9079,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc untraceable',

	sum(if(ampath_status_outreach=9068,1,0)) as '# transfer non-amrs',
	sum(if(ampath_status_outreach=9068,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc transfer non-amrs',

	sum(if(ampath_status_outreach=1286,1,0)) as '# transfer amrs',
	sum(if(ampath_status_outreach=1286,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc transfer amrs',

	sum(if(ampath_status_outreach=1287,1,0)) as '# transfer non-ampath',
	sum(if(ampath_status_outreach=1287,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc transfer non-ampath',

	sum(if(ampath_status_outreach=9080 and next_hiv_clinic_date is null,1,0)) as '# being traced',
	sum(if(ampath_status_outreach=9080 and next_hiv_clinic_date is null,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,0)) as 'perc being traced'
	
	from (select @start_date := '2014-01-01') p0, (select @end_date := '2015-01-01') p1,
	flat_outreach_data t1
	join amrs.location t2 using (location_id)
	where form_id=457 and ampath_status_outreach and encounter_datetime between @start_date and @end_date
	group by t1.location_id
	with rollup;

# number of new LTFU per month stratified by clinic

select @total_ltfu:=0;
select @cur_location_id:=0;

select *, 
	case
		when @cur_location_id != location_id then @total_ltfu := 0
		else @total_ltfu := @total_ltfu + monthly_ltfu
	end as total_ltfu_to_date,
	@cur_location_id := location_id
from
	(select year(encounter_datetime) as year, 
		month(encounter_datetime) as month, 
		f.location_id, 
		name as clinic,
		count(*) as monthly_ltfu,
		count(if(next_appt_date is not null and next_encounter_type=21,1,null)) as seen_by_outreach
	from flat_retention_data f
		join amrs.location l using (location_id)
	where next_hiv_clinic_date is null
		and timestampdiff(day,rtc_date,curdate()) > 90
		and location_id in (1,13,14,15)
	group by clinic,year, month
	order by clinic, year, month
) t1
order by clinic, year, month;

# Key Indicators for assessing outreach worker performance over past week
select
	date(encounter_datetime) as date,

	count(*) as '# of forms',
	sum(if(patient_contacted_by_phone in (1065,1560,9065,9066),1,null)) as '# of calls',
	sum(if(field_attempt_method != 1555,1,null)) as '# of field follow-ups',
	sum(if(date_found is not null,1,0)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_appt_date is null),1,null))  as '% found',
	sum(if(ampath_status_outreach = 9080 and next_appt_date is null,1,0)) as '# being traced'
		
	from (select @provider_id := 138305) p0a,
		flat_outreach_data t1
		join amrs.person_name t2 on t1.provider_id = t2.person_id
	where form_id=457 
		and ampath_status_outreach
		and provider_id=@provider_id
		and timestampdiff(day,encounter_datetime,curdate()) <= 30
	group by date
	order by date;

# Quick overview of worker performance by date, stratified by clinic
select provider_id, family_name, given_name,
	sum(if(timestampdiff(day,date(encounter_datetime),curdate())=7,1,0)) as '-7',
	sum(if(timestampdiff(day,date(encounter_datetime),curdate())=6,1,0)) as '-6',
	sum(if(timestampdiff(day,date(encounter_datetime),curdate())=5,1,0)) as '-5',
	sum(if(timestampdiff(day,date(encounter_datetime),curdate())=4,1,0)) as '-4',
	sum(if(timestampdiff(day,date(encounter_datetime),curdate())=3,1,0)) as '-3',
	sum(if(timestampdiff(day,date(encounter_datetime),curdate())=2,1,0)) as '-2',
	sum(if(timestampdiff(day,date(encounter_datetime),curdate())=1,1,0)) as '-1',
	sum(if(timestampdiff(day,date(encounter_datetime),curdate())=0,1,0)) as '0',
	count(*) as total
	from flat_outreach_data t1
		join amrs.person_name t2 on t1.provider_id = t2.person_id
	where form_id=457 
		and timestampdiff(day,encounter_datetime,curdate()) <= 7
	group by provider_id
	order by provider_id;

select t1.creator, family_name, given_name,
	sum(if(timestampdiff(day,date(t1.date_created),curdate())=7,1,0)) as '-7',
	sum(if(timestampdiff(day,date(t1.date_created),curdate())=6,1,0)) as '-6',
	sum(if(timestampdiff(day,date(t1.date_created),curdate())=5,1,0)) as '-5',
	sum(if(timestampdiff(day,date(t1.date_created),curdate())=4,1,0)) as '-4',
	sum(if(timestampdiff(day,date(t1.date_created),curdate())=3,1,0)) as '-3',
	sum(if(timestampdiff(day,date(t1.date_created),curdate())=2,1,0)) as '-2',
	sum(if(timestampdiff(day,date(t1.date_created),curdate())=1,1,0)) as '-1',
	sum(if(timestampdiff(day,date(t1.date_created),curdate())=0,1,0)) as '0',
	count(*) as total
	from amrs.encounter t1
		join amrs.person_name t2 on t1.creator = t2.person_id
	where form_id=457 
		and timestampdiff(day,t1.date_created,curdate()) <= 7
	group by t1.creator
	order by t1.creator;


# of LTFU by month, system wide
select @total_ltfu_all_enc:=0;
select @total_ltfu_hiv_enc:=0;

select *, 
	@total_ltfu_all_enc := @total_ltfu_all_enc + monthly_ltfu_all_enc as ltfu_total_all_enc,
	@total_ltfu_hiv_enc := @total_ltfu_hiv_enc + monthly_ltfu_hiv_enc as ltfu_total_hiv_enc
from
	(select year(if(rtc_date,rtc_date,adddate(encounter_datetime, interval 60 day))) as year, 
		month(if(rtc_date,rtc_date,adddate(encounter_datetime, interval 60 day))) as month,
		count(
			case
				when next_appt_date is null and encounter_type=21 then if(ampath_status is null or ampath_status in (6101,6102,9079,1286,9080),1,null)
				when next_appt_date is null then 1
				else null
			end) as monthly_ltfu_all_enc,
		count(if(encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47),1,null)) as monthly_ltfu_hiv_enc,
		count(if(rtc_date is null and encounter_type!=21,1,null)) as no_rtc_date
	from flat_retention_data f
	where next_hiv_clinic_date is null
		and timestampdiff(day,if(rtc_date,rtc_date,adddate(encounter_datetime, interval 60 day)),curdate()) > 90
		and hiv_start_date
		and location_id in (1,13,14,15)
		and encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47,21)
		and (transfer_care is null or transfer_care!=1287)
		and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
		and person_id not in (select person_id from flat_outreach_data where next_appt_date is null and ampath_status in (159,9036,9068,1287))
	group by year, month
	order by year, month
) t1
order by year, month;


	select person_id, encounter_datetime, rtc_date, next_appt_date, next_encounter_type, next_hiv_clinic_date, encounter_type,
			case
				when next_appt_date is null and encounter_type=21 then if(ampath_status is null or ampath_status in (6101,6102,9079,1286,9080),1,null)
				when next_appt_date is null then 1
				else null
			end as monthly_ltfu_all_enc,
			if(encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47),1,null) as monthly_ltfu_hiv_enc

	from flat_retention_data f
	where next_hiv_clinic_date is null
		and timestampdiff(day,if(rtc_date,rtc_date,adddate(encounter_datetime, interval 60 day)),curdate()) > 90
		and hiv_start_date
		and location_id in (1,13,14,15)
		and encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47,21)
		and (transfer_care is null or transfer_care!=1287)
		and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
		and rtc_date >= '2014-01-01';



# INDICATOR REPORT FOR OUTREACH TEAM

select 
	count(if(timestampdiff(day,next_hiv_clinic_date,rtc_date) between -7 and 7 and encounter_type != 21,1,null)) as num_within_scheduled_rtc_date,
	count(if(rtc_date,1,null)) as num_scheduled,			
	count(if((timestampdiff(day,next_hiv_clinic_date,rtc_date) > 7
				and (next_hiv_clinic_date is not null and next_encounter_type != 21)),1,null)) as num_returning_before_follow_up,
	count(if(encounter_type=21 and ampath_status_outreach != 9080,1,null)) as num_followed_up,
	count(if(patient_referred_for_field_follow_up in (1551,6834) or reason_field_follow_up_not_attempted in (1551,6834),1,null)) as num_too_far,
	count(if(patient_referred_for_field_follow_up in (1561) or reason_field_follow_up_not_attempted in (1561),1,null)) as num_no_locator,
	count(if(patient_referred_for_field_follow_up in (6834) or reason_field_follow_up_not_attempted in (6834),1,null)) as num_out_of_catchment,
	count(if(
				((timestampdiff(day,encounter_datetime,rtc_date) < 14 or timestampdiff(day,arv_start_date,encounter_datetime) <= 90) 
				and (timestampdiff(day,rtc_date,if(next_hiv_clinic_date,next_hiv_clinic_date,@end_date)) > 7))
				or
				(timestampdiff(day,rtc_date,if(next_hiv_clinic_date,next_hiv_clinic_date,@end_date)) >= 30)
				
			,1,null)) as num_defaulters
	from (select @start_date := '2014-05-01') p1, (select @end_date := '2014-05-31') p2,
		flat_retention_data t1
		left outer join flat_outreach_defined t2 using (encounter_id)
	where 
		((encounter_type != 21 and rtc_date between @start_date and @end_date)
			or (encounter_type=21 and encounter_datetime between @start_date and @end_date))
		and location_id in (1)
		and (next_hiv_clinic_date is null or next_hiv_clinic_date >= @start_date);


select t1.person_id, encounter_datetime, rtc_date, encounter_type, next_hiv_clinic_date, next_appt_date, next_encounter_type
	from (select @start_date := '2014-04-01') p1, (select @end_date := '2014-04-30') p2,
		flat_retention_data t1
		left outer join flat_outreach_defined t2 using (encounter_id)
	where 
		((encounter_type != 21 and rtc_date between '2014-04-01' and '2014-04-30')
			or (encounter_type=21 and encounter_datetime between @start_date and @end_date))
		and location_id in (1)
		and (next_hiv_clinic_date is null or next_hiv_clinic_date >= @start_date);




# KEY INDICATORS SYSTEM WIDE

select year(encounter_datetime) as year, 
	month(encounter_datetime) as month,
	count(*) as '# of forms',
	count(*)/count(distinct date(encounter_datetime)) 'forms / day',
	sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as '# patients followed-up',
	sum(if(ampath_status_outreach = 9080 and next_hiv_clinic_date is null,1,0)) as '# being traced',
	sum(if(date_found is not null,1,0)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null))  as 'perc found',
	sum(if(ampath_status_outreach != 9080 and patient_contacted_by_phone=1065,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as 'perc reached by phone',	
	sum(if(ampath_status_outreach not in (159,9036,9080),1,0)) as '# followed up, not dead, HIV+',	
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21,1,0)) / sum(if(ampath_status_outreach not in (159,9036,9080),1,0)) as 'RTC rate, overall',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,0)) / sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,null)) as 'RTC rate, due to return'
	from (select @start_date := '2014-01-01') p0, (select @end_date := '2015-01-01') p1,
	flat_outreach_data t1
	where form_id=457 and ampath_status_outreach and encounter_datetime between @start_date and @end_date	
	group by year, month
	order by year, month;


#KEY INDICATORS BY CLINIC AND MONTH
select concat(ucase(left(name,1)),lcase(substring(name,2))) as clinic,
	year(encounter_datetime) as year, 
	month(encounter_datetime) as month,
	count(*) as '# of forms',
	count(*)/count(distinct date(encounter_datetime)) 'forms / day',
	sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as '# patients followed-up',
	sum(if(ampath_status_outreach = 9080 and next_hiv_clinic_date is null,1,0)) as '# being traced',
	sum(if(date_found is not null,1,0)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null))  as 'perc found',
	sum(if(ampath_status_outreach != 9080 and patient_contacted_by_phone=1065,1,null)) / sum(if(ampath_status_outreach != 9080 or (ampath_status_outreach=9080 and next_hiv_clinic_date is null),1,null)) as 'perc reached by phone',	
	sum(if(ampath_status_outreach not in (159,9036,9080),1,0)) as '# followed up, not dead, HIV+',	
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21,1,0)) / sum(if(ampath_status_outreach not in (159,9036,9080),1,0)) as 'RTC rate, overall',
	sum(if(next_hiv_clinic_date is not null and next_encounter_type != 21 and ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,0)) / sum(if(ampath_status_outreach=6101 and curdate() > rtc_date_outreach,1,null)) as 'RTC rate, due to return'
	from (select @start_date := '2014-01-01') p0, (select @end_date := '2015-01-01') p1,
	flat_outreach_data t1
	join amrs.location l using (location_id)
	where form_id=457 and ampath_status_outreach and encounter_datetime between @start_date and @end_date	
	group by location_id,year, month
	order by name, year, month;


select concat(ucase(left(name,1)),lcase(substring(name,2))) as clinic,
count(if(next_appt_date is null 
	and dead=0
	and reason_exited_care is null
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care=1286)
	and ((timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) 
		between if(timestampdiff(day,encounter_datetime,rtc_date) < 14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,@start_range_high_risk,@start_range) and @end_range)
		or (encounter_type=21 and ampath_status=9080) 
		or (if(rtc_date,rtc_date,encounter_datetime) > '2014-02-01' and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) >= 90)
        )
	and (if(encounter_type=21,ampath_status is null or ampath_status in (6101,1286,9080),1))
	,1,null)) as requiring_follow_up,
count(if(next_appt_date is null 
	and dead=0
	and reason_exited_care is null
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care=1286)
	and ((timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) 
		between if(timestampdiff(day,encounter_datetime,rtc_date) < 14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,@start_range_high_risk,@start_range)+14 and @end_range)
		or (encounter_type=21 and ampath_status=9080) 
		or (if(rtc_date,rtc_date,encounter_datetime) > '2014-02-01' and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),curdate()) >= 90)
        )
	and (if(encounter_type=21,ampath_status is null or ampath_status in (6101,1286,9080),1))
	,1,null)) as on_list_gt_2_week

from 
(select @start_range_high_risk := 8) p0a,(select @start_range := 30) p0b, (select @end_range := 89) p0c,
reporting_JD.flat_retention_data t1
join amrs.location l using (location_id) #on l.location_id = if(encounter_type=21 and ampath_status=1286,health_center,t1.location_id)
where t1.location_id in (65,64,55,83,4,19,26,27,90,60,7,24,25,98,102,57,81,56,58,69,17,86,107,62,23,11,91,74,76,72,77,71,2,9,1,13,14,15,78,130,100,70,54,20,109,103,82,75,105,73,104,94,12,88,3,31,8,28)
and if(rtc_date,rtc_date,encounter_datetime) > '2014-02-01'
group by t1.location_id
order by name;


# REASONS FOR MISSED APPT
select reason_missed_appt, count(*) as count
	from flat_outreach_data,
		(select @start_date := '2014-01-01') p1,
		(select @end_date := '2020-01-01') p2,
		(select @all_locations := 1) p3
	where 
		if(@all_locations,1,location_id in (1)) 
		and form_id=457
		and reason_missed_appt is not null
	group by reason_missed_appt
	

;
select reason_missed_appt, count(*)
	from flat_outreach_data,
		(select @start_date := '2014-01-01') p1,
		(select @end_date := '2020-01-01') p2,
		(select @all_locations := 1) p3
	where 
		if(@all_locations,1,location_id in (1)) 
		and form_id=457
		and reason_missed_appt is not null
	group by reason_missed_appt;


select  count(*) as total
	from flat_outreach_data,
		(select @start_date := '2014-01-01') p1,
		(select @end_date := '2020-01-01') p2,
		(select @all_locations := 1) p3
	where 
		if(@all_locations,1,location_id in (1)) 
		and form_id=457
		and reason_missed_appt is not null
	



select concept_id, name
	from amrs.concept_name
	where concept_id in (1574) and locale='en' and locale_preferred=1 and voided=0;

