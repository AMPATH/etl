# Query to count number of encounters stratified by provider and encounter type
# parameters: date range, location (though not included here)
select 
	provider_id,
	encounter_type,
	count(*)
	from amrs.encounter_provider t1
	join amrs.encounter t2 using (encounter_id)
	where 
		encounter_datetime between "2015-09-01" and "2015-09-30"
		and t1.voided=0 and t2.voided=0

	group by provider_id, encounter_type
	order by provider_id, encounter_type;


# query to count number of encounters by type stratified by date
# parameters: provider_id and date range
select 
	date(encounter_datetime) as date, 
	encounter_type,
	count(*) as total
	from amrs.encounter_provider t1
	join amrs.encounter t2 using (encounter_id)
	where 
		encounter_datetime between "2015-09-01" and date_add("2015-09-01",interval 7 day)
		and provider_id=234 # just an example, this should be a parameter
		and t1.voided=0 and t2.voided=0

	group by date, encounter_type;


# query to count number of forms entereted stratified by creator and encounter_type
# parameters: date range, location (though not included here)
select 
	creator,
	encounter_type,
	count(*)
	from amrs.encounter t1
	where 
		date_created between "2015-09-01" and "2015-09-30"
		and voided=0
	group by creator, encounter_type
	order by creator, encounter_type;


# query to count number of encounters stratified by form_id and location (though not included)
# parameters: date range and location (not shown. location can be both a stratified and a parameter)
select
	form_id,
	name,
	count(*) as t
	from amrs.encounter t1
	join amrs.form t2 using (form_id)
	where 
		encounter_datetime between "2015-09-01" and "2015-09-30"
		and voided=0
	group by form_id;