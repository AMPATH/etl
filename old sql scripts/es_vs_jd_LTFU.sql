drop table if exists foo;
create table foo (person_id int, index (person_id));
load data local infile 'LTFU.csv' into table foo;

drop table if exists foo2;
create table foo2 (person_id int, index (person_id))
(select person_id,encounter_datetime
from
(select @date_1 := '2014-03-31') p0d,
reporting_JD.flat_retention_data t1
where 
	(encounter_datetime <= @date_1 and (next_hiv_clinic_date is null or next_hiv_clinic_date > @date_1))
	and if(dead,death_date < @date_1,1)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
#	and arv_start_date <= @date_1
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),@date_1) >= 90
	and encounter_type != 21
	and location_id in (65,64,55,83,4,19,26,27,90,60,7,24,25,98,102,57,81,56,58,69,17,86,107,62,23,11,91,74,76,72,77,71,2,9,1,13,14,15,78,130,100,70,54,20,109,103,82,75,105,73,104,94,12,88,3,31,8,28)	
	and enc_date_created <= '2014-04-08'
);

# find patients JD is missing
select * from
(select t1.person_id as p1,t2.person_id as p2
	from foo t1
	left outer join foo2 t2 using (person_id)
) t3
where p2 is null;

# find patients ES is missing
select * from
(select t1.person_id as p1,t2.person_id as p2
	from foo2 t1
	left outer join foo t2 using (person_id)
	where encounter_datetime >= '2013-10-01'
) t3
where p2 is null;

select @date_1 := '2014-03-31';
select person_id,
	encounter_datetime, 
	next_hiv_clinic_date,
	rtc_date,
	timestampdiff(day,rtc_date,@date_1) as days,
	next_appt_date,
	hiv_start_date,
	dead,death_date, 
	discontinue_is_hiv_neg, 
	transfer_care,
	arv_start_date,
	encounter_type,
	location_id 
from flat_retention_data where person_id=300580
;