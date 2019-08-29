# This script creates a temporary table `dead' with all patients who have an encounter_type or obs indicating death
# but have a person.dead as false / 0 

drop table if exists dead;
create temporary table dead (person_id int, primary key person_id (person_id))
(select distinct person_id
	from amrs.encounter e
		join amrs.person p on e.patient_id=p.person_id
		where encounter_type=31 and p.dead=0 and e.voided=0
);

insert ignore into dead
(select 
	person_id
	from amrs.obs o
		join amrs.person p using (person_id)
	where p.dead=0 and concept_id in (1570,1734,1573) and o.voided=0
);


insert ignore into dead
(select 
	person_id
	from amrs.obs o
		join amrs.person p using (person_id)
	where p.dead=0 and concept_id in (1733,6206,9082) and value_coded=159 and o.voided=0
);


# Delete any test patients
delete t1 
from dead t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';

(select * from dead);
