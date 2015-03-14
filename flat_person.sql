# This is the ETL table for flat_person
# obs concept_ids: 1573,1734,1570

# encounter types: 1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_person with flat_person_name
# 2. Replace concept_id in (1573,1734,1570) with concept_id in (obs concept_ids)
# 3. Add column definitions 
# 4. Add obs_set column definitions

create table if not exists flat_person
(encounter_id int,  
person_id int,
encounter_type int,
enc_date_created datetime,
#COLUMN DEFINITIONS GO HERE ##########################################################################################

#obs_date_created should be last obs column
obs_date_created datetime,
index encounter_id (encounter_id), 
index enc_date_created (enc_date_created));

select @last_enc_date_created := max(enc_date_created) from flat_person;
select @last_enc_date_created := if(@last_enc_date_created,@last_enc_date_created,'1900-01-01');
select @last_enc_date_created := '2015-03-01';


select @last_obs_date_created := max(obs_date_created) from flat_person;
select @last_obs_date_created := if(@last_obs_date_created,@last_obs_date_created,'1900-01-01');
select @last_obs_date_created := '2015-03-01';


drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_obs_date_created and date_created <= @last_obs_date_created and concept_id in (1573,1734,1570));

drop temporary table if exists enc;
create temporary table enc (encounter_id int, person_id int, encounter_type int, enc_date_created datetime, primary key encounter_id (encounter_id), index person_id (person_id))
(select e.encounter_id, e.patient_id as person_id, e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e 
where e.voided=0
and e.date_created > @last_enc_date_created
and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);


insert ignore into enc
(select e.encounter_id, e.patient_id as person_id, e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join voided_obs v using (encounter_id)
where e.date_created <= @last_enc_date_created and e.voided=0 and v.encounter_id is not null and e.encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# add in encounters which have new relevant obs attached to them
insert ignore into enc
(select e.encounter_id, e.patient_id as person_id, e.encounter_type, e.date_created as enc_date_created
from amrs.encounter e
join amrs.obs o 
force index for join (date_created)
using (encounter_id)
where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and concept_id in (1573,1734,1570)  and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# remove test patients
delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';

# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index encounter_id (encounter_id))
(select * from amrs.obs o use index (date_created) where concept_id in (1573,1734,1570) and o.voided=0 and date_created > @last_obs_date_created);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_obs_date_created and t2.encounter_id is not null and t1.concept_id in (1573,1734,1570));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o use index (date_created) using (encounter_id) where o.date_created > @last_obs_date_created and o.voided=0 and e.date_created <= @last_enc_date_created and e.voided=0 and o.concept_id in (1573,1734,1570));


drop temporary table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select
	encounter_id,
	# flattened column definitions go here##############################################################################################3
	max(date_created) as obs_date_created
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
(select encounter_id from amrs.encounter where voided=1 and date_created <= @last_enc_date_created and date_voided > @last_enc_date_created and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21));

# remove any encounters that will be (re)inserted
insert ignore into encounters_to_be_removed
(select encounter_id from enc);


delete t1
from flat_person t1
join encounters_to_be_removed t2 using (encounter_id);


#will inner join to avoid having any encounters which have no obs. 
insert into flat_person
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
(select patient_id as person_id from amrs.encounter where voided=1 and date_created <= @last_enc_date_created and date_voided > @last_enc_date_created);

#remove any encounters with new obs as the entire encounter will be rebuilt and added back
insert ignore into flat_new_person_data
(select person_id from obs_subset);

insert ignore into flat_new_person_data
(select person_id from enc);

drop table voided_obs;
