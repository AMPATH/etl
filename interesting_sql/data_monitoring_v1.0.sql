(select 
	year(e.encounter_datetime) as year,
	month(e.encounter_datetime) as month,
	location_id,
	name,
	count(*) as all_encounters,
	count(if(encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21),1,null)) as hiv_encounters, 
	count(if(encounter_type=1,1,null)) as adult_init,
	count(if(encounter_type=2,1,null)) as adult_return,
	count(if(encounter_type=3,1,null)) as peds_initial,
	count(if(encounter_type=4,1,null)) as peds_return,
	count(if(encounter_type=10,1,null)) as pmtct_anc,
	count(if(encounter_type=13,1,null)) as baseline_investigation,
	count(if(encounter_type=14,1,null)) as adult_non_clinical_med,
	count(if(encounter_type=15,1,null)) as peds_non_clinical_med,
	count(if(encounter_type=17,1,null)) as ecs_stable,
	count(if(encounter_type=19,1,null)) as ecs_high_risk,
	count(if(encounter_type=21,1,null)) as outreach,
	count(if(encounter_type=22,1,null)) as adherence_follow_up,
	count(if(encounter_type=23,1,null)) as adherence_initial,
	count(if(encounter_type=26,1,null)) as peds_express_care,
	count(if(encounter_type=43,1,null)) as family_planning,
	count(if(encounter_type=47,1,null)) as pmtct_postnatal
from amrs.encounter e
	join amrs.location l using (location_id)
where e.date_created >= (DATE_FORMAT(NOW() ,'%Y-%m-01') - interval 1 year) #Better to include voided as this represents work done
	and day(e.encounter_datetime) < day(curdate())
	and e.encounter_datetime between (DATE_FORMAT(NOW() ,'%Y-%m-01') - interval 1 year) and curdate()
group by year,month,name
order by name,year desc,month desc);


(select 
	year(e.date_created) as year,
	month(e.date_created) as month,
	location_id,
	name,
	count(*) as all_encounters,
	count(if(encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21),1,null)) as hiv_encounters, 
	count(if(encounter_type=1,1,null)) as adult_init,
	count(if(encounter_type=2,1,null)) as adult_return,
	count(if(encounter_type=3,1,null)) as peds_initial,
	count(if(encounter_type=4,1,null)) as peds_return,
	count(if(encounter_type=10,1,null)) as pmtct_anc,
	count(if(encounter_type=13,1,null)) as baseline_investigation,
	count(if(encounter_type=14,1,null)) as adult_non_clinical_med,
	count(if(encounter_type=15,1,null)) as peds_non_clinical_med,
	count(if(encounter_type=17,1,null)) as ecs_stable,
	count(if(encounter_type=19,1,null)) as ecs_high_risk,
	count(if(encounter_type=21,1,null)) as outreach,
	count(if(encounter_type=22,1,null)) as adherence_follow_up,
	count(if(encounter_type=23,1,null)) as adherence_initial,
	count(if(encounter_type=26,1,null)) as peds_express_care,
	count(if(encounter_type=43,1,null)) as family_planning,
	count(if(encounter_type=47,1,null)) as pmtct_postnatal
from amrs.encounter e
	join amrs.location l using (location_id)
where e.date_created >= (DATE_FORMAT(NOW() ,'%Y-%m-01') - interval 1 year) #Better to include voided as this represents work done
	and day(e.encounter_datetime) < day(curdate())
group by year,month,name
order by name,year desc,month desc);



(select 
	year(encounter_datetime) as year,
	month(encounter_datetime) as month,
	location_id,
	name,
	count(*) as total,
	count(if(last_day(encounter_datetime) = last_day(t1.date_created),1,null)) as total_by_end_of_month,
	count(if(last_day(encounter_datetime) = last_day(t1.date_created),1,null))/count(*) as perc_by_end_of_month,
	count(if(t1.date_created < date_add(last_day(encounter_datetime), interval 3 day),1,null)) as total_by_third,
	count(if(t1.date_created < date_add(last_day(encounter_datetime), interval 3 day),1,null))/count(*) as perc_by_third,
	count(if(t1.date_created < date_add(last_day(encounter_datetime), interval 3 day),1,null))/count(*) >= 0.95 as perc_by_third_gte_95,
	count(if(t1.date_created < date_add(last_day(encounter_datetime), interval 10 day),1,null)) as total_by_tenth,
	count(if(t1.date_created < date_add(last_day(encounter_datetime), interval 10 day),1,null))/count(*) as perc_by_tenth,
	count(if(t1.date_created < date_add(last_day(encounter_datetime), interval 10 day),1,null))/count(*) >= 0.95 as perc_by_tenth_gte_95
					

	from amrs.encounter t1
		join amrs.location t2 using (location_id)
	where encounter_datetime >= "2014-01-01" and encounter_datetime <= (date_sub(last_day(curdate()),interval 2 month))
	group by year, month, name
	order by name, year desc, month desc);			
