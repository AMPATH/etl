# This is the ETL table for flat_tb
# obs concept_ids: 2021,2022,1110,1111,1264,1265,1266,1268,1269,1270,1506,2028,5965,6077,6174,6206,1113
# encounter types: 1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_tb with flat_tb_name
# 2. Replace concept_id in () with concept_id in (obs concept_ids)
# 3. Add column definitions 
# 4. Add obs_set column definitions
select @last_update := (select max(date_updated) from flat_log where table_name="flat_tb");
select @last_update := "2015-01-01";
select @now := now();

create table if not exists flat_tb
(encounter_id int,  
person_id int,
tb_dx_since_last_visit boolean,
tb_dx_this_visit boolean,
tb_prophy_current int,
tb_tx_current varchar(1000),
tb_prophy_started int,
tb_prophy_plan int,
tb_reason_prophy_stopped int,
tb_tx_plan int,
tb_tx_reason_stopped int,
tb_tx_started varchar(1000),
tb_skin_test int,
reason_for_tb_dx int,
on_tb_tx int,
tb_tx_adherence int,
tb_symptom varchar(1000),
tb_tx_outcome int,
tb_tx_start_date datetime,
on_tb_tx int,
index encounter_id (encounter_id), 
index enc_date_created (enc_date_created));

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where voided=1 and date_voided > @last_update and date_created <= @last_update and concept_id in (2021,2022,1110,1111,1264,1265,1266,1268,1269,1270,1506,2028,5965,6077,6174,6206,1113));

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
where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and concept_id in (2021,2022,1110,1111,1264,1265,1266,1268,1269,1270,1506,2028,5965,6077,6174,6206,1113)  and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
);

# remove test patients
delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';

# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index encounter_id (encounter_id))
(select * from amrs.obs o use index (date_created) where concept_id in (2021,2022,1110,1111,1264,1265,1266,1268,1269,1270,1506,2028,5965,6077,6174,6206,1113) and o.voided=0 and date_created > @last_update);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_update and t2.encounter_id is not null and t1.concept_id in (2021,2022,1110,1111,1264,1265,1266,1268,1269,1270,1506,2028,5965,6077,6174,6206,1113));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o use index (date_created) using (encounter_id) where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and o.concept_id in (2021,2022,1110,1111,1264,1265,1266,1268,1269,1270,1506,2028,5965,6077,6174,6206,1113));


drop temporary table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select
	encounter_id,
	min(if(concept_id=2021,value_boolean,null)) as tb_dx_since_last_visit,
	min(if(concept_id=2022,value_boolean,null)) as tb_dx_this_visit,
	min(if(concept_id=1110,value_coded,null)) as tb_prophy_current,
	group_concat(if(concept_id=1111 and value_coded not in (1065,1107,1267,1065,1595),value_coded,null) order by value_coded separator ' // ') as tb_tx_current,
	min(if(concept_id=1264,value_coded,null)) as tb_prophy_started,
	min(if(concept_id=1265,value_coded,null)) as tb_prophy_plan,
	min(if(concept_id=1266,value_coded,null)) as tb_reason_prophy_stopped,
	min(if(concept_id=1268,value_coded,null)) as tb_tx_plan,
	min(if(concept_id=1269,value_coded,null)) as tb_tx_reason_stopped,
	group_concat(if(concept_id=1270,value_coded,null) order by value_coded separator ' // ') as tb_tx_started,
	min(if(concept_id=1506,value_coded,null)) as tb_skin_test,
	min(if(concept_id=2028,value_coded,null)) as reason_for_tb_dx,
	min(if(concept_id=5965,value_coded,null)) as on_tb_tx,
	min(if(concept_id=6077,value_coded,null)) as tb_tx_adherence,
	group_concat(if(concept_id=6174,value_coded,null) order by value_coded separator ' // ') as tb_symptom,
	min(if(concept_id=6206,value_coded,null)) as tb_tx_outcome,
	min(if(concept_id=1113,value_datetime,null)) as tb_tx_start_date,
	min(if(concept_id=1111 and value_coded in (1065,1107,1267,1065,1595),value_coded,null)) as tb_tx_current_plan
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
from flat_tb t1
join encounters_to_be_removed t2 using (encounter_id);


#will inner join to avoid having any encounters which have no obs. 
insert into flat_tb
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
insert into flat_log values (now(),"flat_tb");
