# This is the ETL table for flat_orders
# orders concept_ids:

# encounter types: 1,2,3,4,5,6,7,8,9,10,13,14,15,17,19,22,23,26,43,47,21
# 1. Replace flat_orders with flat_orders_name
# 2. Replace concept_id in () with concept_id in (orders concept_ids)
# 3. Add column definitions
# 4. Add orders_set column definitions

select @table_version := "flat_orders_v1.1";
select @start := now();

set session group_concat_max_len=100000;
select @last_date_created := (select max(date_created) from amrs.orders);


select @boundary := "!!";

#delete from flat_log where table_name="flat_orders";
#drop table if exists flat_orders;
create table if not exists flat_orders
(person_id int,
encounter_id int,
order_id int,
encounter_datetime datetime,
encounter_type int,
date_activated datetime,
orders text,
order_datetimes text,
max_date_created datetime,
index encounter_id (encounter_id),
index person_enc_id (person_id,encounter_id),
index date_created (max_date_created),
primary key (encounter_id)
);

# this breaks when replication is down
select @last_update := (select max(date_updated) from flat_log where table_name=@table_version);

# then use the max_date_created from amrs.encounter. This takes about 10 seconds and is better to avoid.
select @last_update :=
	if(@last_update is null,
		(select max(date_created) from amrs.encounter e join flat_orders using (encounter_id)),
		@last_update);

#otherwise set to a date before any encounters had been created (i.g. we will get all encounters)
select @last_update := if(@last_update,@last_update,'1900-01-01');

#select @last_update := "2015-10-20";

drop table if exists voided_orders;
create table voided_orders (index encounter_id (encounter_id), index orders_id (order_id))
(select patient_id, encounter_id, order_id, date_activated, date_voided, concept_id, date_created
from amrs.orders where voided=1 and date_voided > @last_update and date_created <= @last_update);




# delete any rows that have voided orders with encounter_id
delete t1
from flat_orders t1
join voided_orders t2 using (encounter_id);


replace into flat_orders
(select
	o.patient_id,
	o.encounter_id,
    o.order_id,
	e.encounter_datetime,
	e.encounter_type,
	e.location_id,
	group_concat(o.concept_id order by o.concept_id separator ' ## ') as orders,
	group_concat(concat(@boundary,o.concept_id,'=',date(o.date_activated),@boundary) order by o.concept_id separator ' ## ') as order_datetimes,
	max(o.date_created) as max_date_created
	from voided_orders v
		join amrs.orders o using (encounter_id)
		join amrs.encounter e using (encounter_id)
	where
		o.encounter_id >= 1 and o.voided=0
	group by o.encounter_id
);


# Insert newly created orders with encounter_ids
replace into flat_orders
(select
	o.patient_id,
	o.encounter_id,
    o.order_id,
	e.encounter_datetime,
	e.encounter_type,
	e.location_id,
	group_concat(concept_id order by o.concept_id separator ' ## ') as orders,
	group_concat(concat(@boundary,o.concept_id,'=',date(o.date_activated),@boundary) order by o.concept_id separator ' ## ') as order_datetimes,
	max(o.date_created) as max_date_created

	from amrs.orders o
		join amrs.encounter e using (encounter_id)
	where o.encounter_id > 0
		and o.voided=0 and o.date_created > @last_update
	group by o.encounter_id
);




# remove voided patients
delete t1
from flat_orders t1
join amrs.person t2 using (person_id)
where t2.voided=1;

drop table voided_orders;

select @end := now();
insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
select concat(@table_version," : Time to complete: ",timestampdiff(minute, @start, @end)," minutes");