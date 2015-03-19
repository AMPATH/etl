# This is the ETL table for flat_arvs
# obs concept_ids: 1192,1087,1088,1156,1164,1250,1251,1252,1255,1490,1505,1717,1999,2031,2033,2154,2155,2157,1187,1387,966,1086,147,1176,1181,1499,1719
# encounter types: 1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21

select @last_update := (select max(date_updated) from flat_log where table_name="flat_arvs");
select @last_update := if(@last_update,@last_update,'1900-01-01');

select @now := now();

# drop table if exists flat_arvs;
#select @last_update := "2015-01-01";
create table if not exists flat_arvs
(encounter_id int,  
person_id int,
arv_use_ever boolean,
arvs_previous varchar(1000),
arvs_current varchar(1000),
arv_adh_past_month int,
arv_adh_past_week int,
arv_started varchar(1000),
reason_arv_started int,
reason_arvs_stopped int,
arv_plan int,
arvs_adh_past_seven_days int,
eligible_for_arvs_reason_not_started int,
arv_status int,
arv_has_changed int,
new_arv_side_effect varchar(1000),
has_arv_side_effect int,
arvs_per_patient varchar(1000),
arvs_reason_for_taking int,
arvs_taken_in_past varchar(1000),
newborn_arv_prophylaxis varchar(1000),
peds_arvs_given_in_newborn_period int,
pmtct_cur_arvs varchar(1000),
pmtct_previous_arvs varchar(1000),
pmtct_arvs_while_preg varchar(1000),
pmtct_hx_arvs varchar(1000),
pmtct_hx_arvs_dose int,
arvs_start_date datetime,
arv_past_week_days_missed double,
index encounter_id (encounter_id));

drop table if exists voided_obs;
create table voided_obs (index encounter_id (encounter_id), index obs_id (obs_id))
(select person_id, encounter_id, obs_id, obs_datetime, date_voided, concept_id, date_created
from amrs.obs where 
voided=1 and date_voided > @last_update and date_created <= @last_update and concept_id in (1192,1087,1088,1156,1164,1250,1251,1252,1255,1490,1505,1717,1999,2031,2033,2154,2155,2157,1187,1387,966,1086,147,1176,1181,1499,1719));

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
(select e.patient_id as person_id, e.encounter_id
from amrs.encounter e
join amrs.obs o 
force index for join (date_created)
using (encounter_id) 
where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,43,47,21) 
and concept_id in (1192,1087,1088,1156,1164,1250,1251,1252,1255,1490,1505,1717,1999,2031,2033,2154,2155,2157,1187,1387,966,1086,147,1176,1181,1499,1719)
);

# remove test patients
delete t1 
from enc t1 
join amrs.person_attribute t2 using (person_id) 
where t2.person_attribute_type_id=28 and value='true';

# create a dataset of the new obs. 
drop table if exists obs_subset;
create temporary table obs_subset (primary key obs_id (obs_id), index encounter_id (encounter_id))
(select * from amrs.obs o use index (date_created) where 
concept_id in (1192,1087,1088,1156,1164,1250,1251,1252,1255,1490,1505,1717,1999,2031,2033,2154,2155,2157,1187,1387,966,1086,147,1176,1181,1499,1719) and o.voided=0 and date_created > @last_update);

# add obs of encounters with voided obs
insert ignore into obs_subset
(select t1.* from amrs.obs t1 join voided_obs t2 using (encounter_id) where t1.voided=0 and t1.date_created <= @last_update and t2.encounter_id is not null and t1.concept_id in (1192,1087,1088,1156,1164,1250,1251,1252,1255,1490,1505,1717,1999,2031,2033,2154,2155,2157,1187,1387,966,1086,147,1176,1181,1499,1719));

# add obs for encounters which have new obs
insert ignore into obs_subset
(select o.* from amrs.encounter e join amrs.obs o use index (date_created) using (encounter_id) where o.date_created > @last_update and o.voided=0 and e.date_created <= @last_update and e.voided=0 and o.concept_id in (1192,1087,1088,1156,1164,1250,1251,1252,1255,1490,1505,1717,1999,2031,2033,2154,2155,2157,1187,1387,966,1086,147,1176,1181,1499,1719));


drop temporary table if exists n_obs;
create temporary table n_obs (index encounter_id (encounter_id))
(select
	encounter_id,
	min(if(concept_id=1192,value_boolean,null)) as arv_use_ever,
	group_concat(if(concept_id=1087,value_coded,null) order by value_coded separator ' // ') as arvs_previous,
	group_concat(if(concept_id=1088,value_coded,null) order by value_coded separator ' // ') as arvs_current,
	min(if(concept_id=1156,value_coded,null)) as arv_adh_past_month,
	min(if(concept_id=1164,value_coded,null)) as arv_adh_past_week,
	group_concat(if(concept_id=1250,value_coded,null) order by value_coded separator ' // ') as arv_started,
	min(if(concept_id=1251,value_coded,null)) as reason_arv_started,
	min(if(concept_id=1252,value_coded,null)) as reason_arvs_stopped,
	min(if(concept_id=1255,value_coded,null)) as arv_plan,
	min(if(concept_id=1490,value_coded,null)) as arvs_adh_past_seven_days,
	min(if(concept_id=1505,value_coded,null)) as eligible_for_arvs_reason_not_started,
	min(if(concept_id=1717,value_coded,null)) as arv_status,
	min(if(concept_id=1999,value_coded,null)) as arv_has_changed,
	group_concat(if(concept_id=2031,value_coded,null) order by value_coded separator ' // ') as new_arv_side_effect,
	min(if(concept_id=2033,value_coded,null)) as has_arv_side_effect,
	group_concat(if(concept_id=2154,value_coded,null) order by value_coded separator ' // ') as arvs_per_patient,
	min(if(concept_id=2155,value_coded,null)) as arvs_reason_for_taking,
	group_concat(if(concept_id=2157,value_coded,null) order by value_coded separator ' // ') as arvs_taken_in_past,
	group_concat(if(concept_id=1187,value_coded,null) order by value_coded separator ' // ') as newborn_arv_prophylaxis,
	min(if(concept_id=1387,value_coded,null)) as peds_arvs_given_in_newborn_period,
	group_concat(if(concept_id=966,value_coded,null) order by value_coded separator ' // ') as pmtct_cur_arvs,
	group_concat(if(concept_id=1086,value_coded,null) order by value_coded separator ' // ') as pmtct_previous_arvs,
	group_concat(if(concept_id=1147,value_coded,null) order by value_coded separator ' // ') as pmtct_arvs_while_preg,
	group_concat(if(concept_id=1176,value_coded,null) order by value_coded separator ' // ') as pmtct_hx_arvs,
	min(if(concept_id=1181,value_coded,null)) as pmtct_hx_arvs_dose,
	min(if(concept_id=1499,value_datetime,null)) as arvs_start_date,
	min(if(concept_id=1719,value_numeric,null)) as arv_past_week_days_missed
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
from flat_arvs t1
join encounters_to_be_removed t2 using (encounter_id);


insert into flat_arvs
(select *
from enc e1 
inner join n_obs n1 using (encounter_id)
order by e1.person_id
);

## UPDATE data for derived tables

#remove any encounters that have been voided.
insert ignore into flat_new_person_data
(select distinct person_id from voided_obs);

insert ignore into flat_new_person_data
(select patient_id from amrs.encounter where voided=1 and date_created <= @last_update and date_voided > @last_update);

#remove any encounters with new obs as the entire encounter will be rebuilt and added back
insert ignore into flat_new_person_data
(select person_id from obs_subset);

insert ignore into flat_new_person_data
(select person_id from enc);

drop table voided_obs;

insert into flat_log values (@now,"flat_arvs");

