# This is the ETL table for flat_int_data
# obs concept_ids: 654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856

# encounter types: 1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_int_data with flat_int_data_name
# 2. Replace concept_id in () with concept_id in (obs concept_ids)
# 3. Add column definitions 
# 4. Add obs_set column definitions
# drop table if exists flat_int_data;

select @last_update := (select max(date_updated) from flat_log where table_name="flat_int_data");
select @last_update := "2015-03-10";
select @now := now();

create table if not exists flat_int_data
(encounter_id int,  
person_id int,
int_alt double,
int_ast double,
int_cd4_count double,
int_cd4_percent double,
int_chest_xray int,
int_creatinine double,
int_hemoglobin double,
int_hiv_dna_pcr int,
int_hiv_long_elisa int,
int_hiv_rapid_test int,
int_hiv_vl_qual int,
int_hiv_western_blot int,
int_sputum_afb int,
int_syphylis_tpha_qual int,
int_syphylis_tpha_titer int,
int_tv_pcr int,
int_urine_pregnancy_test int,
int_vdrl int,
int_viral_load double,
index encounter_id (encounter_id)
);



drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_update and date_created <= @last_update and concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856));

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
force index for join (date_created)
using (encounter_id)
where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856)  and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# remove test patients
delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';


# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index encounter_id (encounter_id))
(select * from amrs.obs o use index (date_created) where concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856) and o.voided=0 and date_created > @last_update);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_update and t2.encounter_id is not null and t1.concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o use index (date_created) using (encounter_id) where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and o.concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856));


drop temporary table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select
	encounter_id,
	min(if(concept_id=654,value_numeric,null)) as alt,
	min(if(concept_id=653,value_numeric,null)) as ast,
	min(if(concept_id=5497,value_numeric,null)) as cd4_count,
	min(if(concept_id=730,value_numeric,null)) as cd4_percent,
	min(if(concept_id=12,value_coded,null)) as chest_xray,
	min(if(concept_id=790,value_numeric,null)) as creatinine,
	min(if(concept_id=21,value_numeric,null)) as hemoglobin,
	min(if(concept_id=1030,value_coded,null)) as hiv_dna_pcr,
	min(if(concept_id=1042,value_coded,null)) as hiv_long_elisa,
	min(if(concept_id=1040,value_coded,null)) as hiv_rapid_test,
	min(if(concept_id=1305,value_coded,null)) as hiv_vl_qual,
	min(if(concept_id=1047,value_coded,null)) as hiv_western_blot,
	min(if(concept_id=307,value_coded,null)) as sputum_afb,
	min(if(concept_id=1032,value_coded,null)) as syphylis_tpha_qual,
	min(if(concept_id=1031,value_coded,null)) as syphylis_tpha_titer,
	min(if(concept_id=1039,value_coded,null)) as tv_pcr,
	min(if(concept_id=45,value_coded,null)) as urine_pregnancy_test,
	min(if(concept_id=299,value_coded,null)) as vdrl,
	min(if(concept_id=856,value_numeric,null)) as viral_load
	from obs_subset
	where encounter_id is not null and voided=0
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
from flat_int_data t1
join encounters_to_be_removed t2 using (encounter_id);


#will inner join to avoid having any encounters which have no obs. 
insert into flat_int_data
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

insert into flat_log values (now(),"flat_int_data");
