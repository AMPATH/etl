# This is the ETL table for flat_data
# obs concept_ids: 654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856


# encounter types: 1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_table with flat_table_name
# 2. Replace concept_id in () with concept_id in (obs concept_ids)
# 3. Add column definitions 
# 4. Add obs_set column definitions

select @last_update := (select max(date_updated) from flat_log where table_name="flat_data");
select @last_update := "2015-01-01";
select @now := now();


# drop table if exists flat_data;
create table if not exists flat_data
(person_id int,
obs_datetime datetime,
encounter_id int,  
alt double,
ast double,
cd4_count double,
cd4_percent double,
chest_xray int,
creatinine double,
hemoglobin double,
hiv_dna_pcr int,
hiv_long_elisa int,
hiv_rapid_test int,
hiv_vl_qual int,
hiv_western_blot int,
sputum_afb int,
syphylis_tpha_qual int,
syphylis_tpha_titer int,
tv_pcr int,
urine_pregnancy_test int,
vdrl int,
viral_load double,
index (encounter_id),
index (obs_datetime)
);

drop table if exists voided_obs;
create table voided_obs (index obs_datetime(obs_datetime), index obs_id (obs_id))
(select person_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs use index (date_voided)
where voided=1 and date_voided > @last_update and date_created <= @last_update 
	and encounter_id is null
	and concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856));


drop temporary table if exists enc;
create temporary table enc (encounter_id int, person_id int, primary key encounter_id (encounter_id), index person_id (person_id), index obs_datetime(obs_datetime))
(select o.obs_id as encounter_id, o.person_id, obs_datetime
from amrs.obs o use index (date_created)
where o.voided=0
and o.date_created > @last_update
and concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856)
and encounter_id is null
group by obs_datetime
);

# remove test patients
delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';


# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index person_date (person_id,obs_datetime))
(select * 
	from amrs.obs o use index (date_created) 
	where concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856) 
		and o.voided=0 and date_created > @last_update
);


insert ignore into obs_subset
(select t1.* 
	from amrs.obs t1 
	join voided_obs t2 using (obs_datetime) 
	where t1.voided=0 
		and t1.encounter_id is null 
		and t1.date_created <= @last_update 
		and t1.concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856)
);


drop temporary table if exists n_obs;
create temporary table n_obs (index obs_datetime (obs_datetime))
(select
	person_id, 
	obs_datetime, 
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
	group by person_id,obs_datetime 
);


#remove any encounters that have a voided obs. 
drop table if exists encounters_to_be_removed;
create temporary table encounters_to_be_removed (primary key obs_datetime (obs_datetime))
(select distinct obs_datetime from voided_obs);

#remove any encounters that have been voided.
insert ignore into encounters_to_be_removed
(select obs_datetime 
	from amrs.obs 
	where voided=1 
		and date_created <= @last_update 
		and date_voided > @last_update
		and concept_id in (654,653,5497,730,12,790,21,1030,1042,1040,1305,1047,307,1032,1031,1039,45,299,856)
);

# remove any encounters that will be (re)inserted
insert ignore into encounters_to_be_removed
(select obs_datetime from enc);


delete t1
from flat_data t1
join encounters_to_be_removed t2 using (obs_datetime);


#will inner join to avoid having any encounters which have no obs. 
insert into flat_data
(select *
from enc e1 
inner join n_obs n1 using (person_id, obs_datetime)
order by e1.person_id,e1.encounter_id
);

## UPDATE data for derived tables

#remove any encounters that have been voided.
insert ignore into flat_new_person_data
(select distinct person_id from voided_obs);

#remove any encounters with new obs as the entire encounter will be rebuilt and added back
insert ignore into flat_new_person_data
(select person_id from obs_subset);

insert ignore into flat_new_person_data
(select person_id from enc);

drop table voided_obs;

insert into flat_log values (now(),"flat_data");
