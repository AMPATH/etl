# This is the ETL table for flat_maternity (data specific to antenatal, postnatal, pmtct, etc.)
# obs concept_ids: 1856,5272,1363,1846,1992,2055,2198,5630,1836,5596,5599,1279,1855,5992

# encounter types: 1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_table with flat_maternity
# 2. Replace concept_id in () with concept_id in (1856,5272,1363,1846,1992,2055,2198,5630,1836,5596,5599,1279,1855,5992)
# 3. Add column definitions 
# 4. Add obs_set column definitions

# first check if in flat_log
select @last_update := (select max(date_updated) from flat_log where table_name="flat_maternity");

# then use the max_date_created from amrs.encounter
select @last_update :=
	if(@last_update is null, 
		(select max(date_created) from amrs.encounter e join flat_maternity using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');

select @now := now();

# drop table if exists flat_maternity;
#delete from flat_log where table_name="flat_maternity";
#select @last_update := "2015-01-01";

create table if not exists flat_maternity
(encounter_id int,  
person_id int,
fetal_movement boolean,
is_pregnant boolean,
arvs_given_during_labor varchar(1000),
last_preg_outcome int,
pmtct_on_arvs_coded int,
anc_is_enrolled int,
mother_on_arvs int,
delivery_method int,
lmp datetime,
expected_delivery_date datetime,
actual_delivery_date datetime,
num_weeks_preg double,
fundal_height double,
preg_current_month double,
delivered_since_last_visit int,
index encounter_id (encounter_id));


drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_update and date_created <= @last_update and concept_id in (1856,5272,1363,1846,1992,2055,2198,5630,1836,5596,5599,1279,1855,5992));

drop temporary table if exists enc;
create temporary table enc (encounter_id int, person_id int, primary key encounter_id (encounter_id), index person_id (person_id))
(select e.encounter_id, e.patient_id as person_id
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
force index for join (encounter_date_created)
using (encounter_id)
where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and concept_id in (1856,5272,1363,1846,1992,2055,2198,5630,1836,5596,5599,1279,1855,5992)  and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# remove test patients
delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';

# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index encounter_id (encounter_id))
(select * from amrs.obs o use index (encounter_date_created) where concept_id in (1856,5272,1363,1846,1992,2055,2198,5630,1836,5596,5599,1279,1855,5992) and o.voided=0 and date_created > @last_update);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_update and t2.encounter_id is not null and t1.concept_id in (1856,5272,1363,1846,1992,2055,2198,5630,1836,5596,5599,1279,1855,5992));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o use index (encounter_date_created) using (encounter_id) where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and o.concept_id in (1856,5272,1363,1846,1992,2055,2198,5630,1836,5596,5599,1279,1855,5992));


drop temporary table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select
	encounter_id,
	min(if(concept_id=1856,value_boolean,null)) as fetal_movement,
	min(if(concept_id=5272,value_boolean,null)) as is_pregnant,
	group_concat(if(concept_id=1363,value_coded,null) order by value_coded separator ' // ') as arvs_given_during_labor,
	min(if(concept_id=1846,value_coded,null)) as last_preg_outcome,
	min(if(concept_id=1992,value_coded,null)) as pmtct_on_arvs_coded,
	min(if(concept_id=2055,value_coded,null)) as anc_is_enrolled,
	min(if(concept_id=2198,value_coded,null)) as mother_on_arvs,
	min(if(concept_id=5630,value_coded,null)) as delivery_method,
	min(if(concept_id=1836,value_datetime,null)) as lmp,
	min(if(concept_id=5596,value_datetime,null)) as expected_delivery_date,
	min(if(concept_id=5599,value_datetime,null)) as actual_delivery_date,
	min(if(concept_id=1279,value_numeric,null)) as num_weeks_preg,
	min(if(concept_id=1855,value_numeric,null)) as fundal_height,
	min(if(concept_id=5992,value_numeric,null)) as preg_current_month,
	min(if(concept_id=1146,value_coded,null)) as delivered_since_last_visit
	from obs_subset
	where encounter_id is not null
		 and voided=0
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
from flat_maternity t1
join encounters_to_be_removed t2 using (encounter_id);


insert into flat_maternity
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

insert into flat_log values (@now,"flat_maternity");
