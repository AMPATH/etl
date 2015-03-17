select reason_missed_appt, count(*)
from flat_outreach_data

where form_id=457
group by reason_missed_appt

select name, year(t1.date_created) as year, month(t1.date_created) as month,count(*)
from amrs.encounter t1 
join amrs.location t2 using (location_id)
where t1.creator=156737
group by name, year, month
order by year, month, name;

select year(t1.date_created) as year, month(t1.date_created) as month, t1.creator, given_name, family_name,
count(*) as total, 
count(*) / count(distinct date(t1.date_created)) as forms_per_day,
count(*) / 20 as forms_per_day_20,
count(if(encounter_type in (2,4),1,null)) as follow_up,
round(count(if(encounter_type in (2,4),1,null)) / count(*) * 100,1) as perc_follow_up,
count(if(encounter_type in (1,3),1,null)) as initial,
round(count(if(encounter_type in (1,3),1,null)) / count(*) * 100,1) as perc_initial,
count(if(encounter_type not in (1,2,3,4),1,null)) as other,
round(count(if(encounter_type not in (1,2,3,4),1,null)) / count(*) * 100,1) as perc_other
from amrs.encounter t1
join amrs.person_name t2 on t1.creator = t2.person_id
where t1.date_created between '2014-05-01' and '2014-05-31'
group by year, month, creator
having total > 100;

select year(t1.date_created) as year, month(t1.date_created) as month, t1.encounter_type, name, count(*)
from amrs.encounter t1
join amrs.encounter_type t2 on t1.encounter_type = t2.encounter_type_id
where t1.date_created between '2014-05-01' and '2014-05-31'
group by year, month, encounter_type
order by encounter_type, year, month;