# HIV encounter_type_ids : (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) 
#1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21 
#1,2,3,4,10,13,14,15,17,19,22,23,26,43,47 (reporting team)

# HIV pmtct encounter_type_ids : (10,11,12,44,46,47)
# HIV adult visit : (1,2,14,17,19)
# HIV peds visit : (3,4,15,26)
# non-lab encounters : (1,2,3,4,10,11,12,16,17,19,25,27,28,29,32,33,34,37,43,47,49,40,54,55,56,57,58,59,60,61,62,63,64,65,66,67,68,69,70,71,73,75,76,78,79,80,83)
# lab encounters : (5,6,7,8,9,45,87)

select name, 
	count(*) as num_encounters,
	count(if(encounter_type in (1,2,3,4,10,14,15,17,19,22,23,26,47,21),1,null)) as num_hiv_encounters,
	count(if(encounter_type in (1,2,14,17,19),1,null)) as num_adult_hiv,
	count(if(encounter_type in (3,4,15,26),1,null)) as num_peds_hiv,
	count(if(encounter_type in (10,47),1,null)) as num_pmtct,
	count(if(encounter_type in (21),1,null)) as num_outreach
from
	amrs.encounter t1
	join amrs.location t2 using (location_id)
where encounter_datetime >= "2014-01-01" and voided=0
group by name
order by num_encounters desc;