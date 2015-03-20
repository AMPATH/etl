# average number of patients and encounters per location per month
select name, avg(num_patients) as avg_patients, avg(num_encounters) as avg_enc
from
(select year(encounter_datetime) as year, month(encounter_datetime) as month, location_id, 
	count(distinct patient_id) as num_patients,
	count(*) as num_encounters
	from amrs.encounter
	where encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47)
	and encounter_datetime >= "2014-01-01"
	and voided=0
	group by year, month, location_id
) t1
join amrs.location using (location_id)
group by location_id;


###############################################################################################################
###############################################################################################################
###############################################################################################################3