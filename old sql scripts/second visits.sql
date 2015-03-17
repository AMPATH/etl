select count(*) from flat_retention_data 
	where encounter_type=21 and prev_encounter_type=21 and prev


select count(*) from flat_retention_data where ampath_status = 6101
select count(*) from flat_retention_data where ampath_status = 6101 and next_hiv_clinic_date is not null

select count(*) from flat_retention_data where ampath_status != 9080 and next_encounter_type=21
select count(*) from flat_retention_data where ampath_status != 9080 and next_encounter_type=21 and next_hiv_clinic_date is null;
select count(*) from flat_retention_data where ampath_status != 9080 and next_encounter_type=21 and next_hiv_clinic_date is not null;