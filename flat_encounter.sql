# This is the ETL table for flat_encounter. This had demographic level information
# obs concept_ids: 1246,1361,1271,1285,1357,1591,1596,1705,1733,1834,1835,1839,7015,1502,1568,5096,7016

# encounter types: 1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_encounter with flat_encounter_name
# 2. Replace concept_id in () with concept_id in (obs concept_ids)
# 3. Add column definitions 
# 4. Add obs_set column definitions
select @last_update := (select max(date_updated) from flat_log where table_name="flat_encounter");
select @last_update := "2015-03-10";
select @now := now();

create table if not exists flat_encounter
(encounter_id int,  
person_id int,
scheduled_visit boolean,
hiv_tested_prev boolean,
tests_ordered varchar(1000),
transfer_care int,
hiv_test_result_this_visit int,
outreach_reason_contact_attempted int,
outreach_reason_exited_care int,
recs_made varchar(1000),
reason_for_missed_visit varchar(1000),
reason_for_visit varchar(1000),
reasons_for_next_visit varchar(1000),
current_visit_type int,
transfer_in int,
rtc_date_express_care datetime,
outreach_date_found datetime,
rtc_date datetime,
transfer_in_detailed varchar(1000),
index encounter_id (encounter_id));

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_update and date_created <= @last_update and concept_id in (1246,1361,1271,1285,1357,1591,1596,1705,1733,1834,1835,1839,7015,1502,1568,5096,7016));

drop temporary table if exists enc;
create temporary table enc (encounter_id int, person_id int, primary key encounter_id (encounter_id), index person_id (person_id))
(select e.encounter_id, e.patient_id as person_id, e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e 
where e.voided=0
and e.date_created > @last_update
and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);


insert ignore into enc
(select e.encounter_id, e.patient_id as person_id
from amrs.encounter e
join voided_obs v using (encounter_id)
where e.date_created <= @last_update and e.voided=0 and v.encounter_id is not null and e.encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# add in encounters which have new relevant obs attached to them
insert ignore into enc
(select e.encounter_id, e.patient_id as person_id
from amrs.encounter e
join amrs.obs o 
force index for join (date_created)
using (encounter_id)
where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and concept_id in (1246,1361,1271,1285,1357,1591,1596,1705,1733,1834,1835,1839,7015,1502,1568,5096,7016)  and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# remove test patients
delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';

# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index encounter_id (encounter_id))
(select * from amrs.obs o use index (date_created) where concept_id in (1246,1361,1271,1285,1357,1591,1596,1705,1733,1834,1835,1839,7015,1502,1568,5096,7016) and o.voided=0 and date_created > @last_update);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_update and t2.encounter_id is not null and t1.concept_id in (1246,1361,1271,1285,1357,1591,1596,1705,1733,1834,1835,1839,7015,1502,1568,5096,7016));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o use index (date_created) using (encounter_id) where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and o.concept_id in (1246,1361,1271,1285,1357,1591,1596,1705,1733,1834,1835,1839,7015,1502,1568,5096,7016));


drop temporary table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select
	encounter_id,
	# flattened column definitions go here
	min(if(concept_id=1246,value_boolean,null)) as scheduled_visit,
	min(if(concept_id=1361,value_boolean,null)) as hiv_tested_prev,
	group_concat(if(concept_id=1271,value_coded,null) order by value_coded separator ' // ') as tests_ordered,
	min(if(concept_id=1285,value_coded,null)) as transfer_care,
	min(if(concept_id=1357,value_coded,null)) as hiv_test_result_this_visit,
	min(if(concept_id=1591,value_coded,null)) as outreach_reason_contact_attempted,
	min(if(concept_id=1596,value_coded,null)) as outreach_reason_exited_care,
	group_concat(if(concept_id=1705,value_coded,null) order by value_coded separator ' // ') as recs_made,
	group_concat(if(concept_id=1733,value_coded,null) order by value_coded separator ' // ') as reason_for_missed_visit,
	group_concat(if(concept_id=1834,value_coded,null) order by value_coded separator ' // ') as reason_for_visit,
	group_concat(if(concept_id=1835,value_coded,null) order by value_coded separator ' // ') as reasons_for_next_visit,
	min(if(concept_id=1839,value_coded,null)) as current_visit_type,
	min(if(concept_id=7015,value_coded,null)) as transfer_in,
	min(if(concept_id=1502,value_datetime,null)) as rtc_date_express_care,
	min(if(concept_id=1568,value_datetime,null)) as outreach_date_found,
	min(if(concept_id=5096,value_datetime,null)) as rtc_date,
	min(if(concept_id=7016,value_text,null)) as transfer_in_detailed
	from obs_subset
	where encounter_id is not null
	group by encounter_id 
);

#remove any encounters that have a voided obs. 
drop table if exists encounters_to_be_removed;
create temporary table encounters_to_be_removed (primary key encounter_id (encounter_id))
(select distinct encounter_id from voided_obs);

#remove any encounters that have been voided.
insert ignore into encounters_to_be_removed
(select encounter_id from amrs.encounter where voided=1 and date_created <= @last_update and date_voided > @last_update and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21));

# remove any encounters that will be (re)inserted
insert ignore into encounters_to_be_removed
(select encounter_id from enc);


delete t1
from flat_encounter t1
join encounters_to_be_removed t2 using (encounter_id);


insert into flat_encounter
(select *
from enc e1 
inner join n_obs n1 using (encounter_id)
order by e1.person_id,e1.encounter_id
);

## UPDATE data for derived tables

#remove any encounters that have been voided.
insert ignore into flat_new_person_data
(select distinct person_id from voided_obs);

insert ignore into flat_new_person_data
(select patient_id as person_id from amrs.encounter where voided=1 and date_created <= @last_update and date_voided > @last_update);

#remove any encounters with new obs as the entire encounter will be rebuilt and added back
insert ignore into flat_new_person_data
(select person_id from obs_subset);

insert ignore into flat_new_person_data
(select person_id from enc);

drop table voided_obs;

insert into flat_log values (now(),"flat_encounter");

