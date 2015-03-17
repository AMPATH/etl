select count(*) from flat_lab_data;          #6048747
select count(*) from flat_lab_data_complete; #6048747

select sum(cd4_1) from flat_lab_data;          #2209267939
select sum(cd4_1) from flat_lab_data_complete; #2209267939

select sum(cd4_2) from flat_lab_data;          #1663790492
select sum(cd4_2) from flat_lab_data_complete; #1663790492

select sum(vl1) from flat_lab_data;          #37646402299
select sum(vl1) from flat_lab_data_complete; #37646402299



drop table if exists foo;
create temporary table foo (index encounter_id (encounter_id))
(select t1.encounter_id, t1.person_id
	from flat_lab_data t1
	join flat_lab_data_complete t2 using (encounter_id)	
);


drop table if exists foo1;
create temporary table foo1 (index encounter_id (person_id, encounter_datetime))
(select t1.encounter_id, t1.person_id, t1.encounter_datetime, t2.encounter_id as t2_enc_id, t2.person_id as t2_id, t2.encounter_datetime as t2_date
from flat_lab_data t1
left outer join foo t2 using (encounter_id)
);

select count(*) from foo1 where enc_id is null


select count(*) from
(select person_id, lab_tested_hiv_positive, lab_vl, lab_cd4_count, lab_cd4_percent, count(*) as num1 from ltfu_1 where encounter_type=999999 group by person_id, lab_tested_hiv_positive, lab_vl, lab_cd4_count, lab_cd4_percent having count(*) > 1) t1
join
(select person_id, lab_tested_hiv_positive, lab_vl, lab_cd4_count, lab_cd4_percent, count(*) as num2 from reporting.ltfu_1  where encounter_type=999999 group by person_id, lab_tested_hiv_positive, lab_vl, lab_cd4_count, lab_cd4_percent having count(*) > 1) t2
using (person_id)
where num1 != num2


select * from ltfu_1 limit 1

drop table if exists foo;
create temporary table foo (index encounter_id (encounter_id))
(select t1.encounter_id
	from flat_lab_data t1
	join flat_lab_data_complete t2 using (encounter_id)
);


select *
from
(select t1.person_id a, t1.encounter_datetime, t1.encounter_id b, t1.cd4_1 c, t2.person_id d, t2.encounter_id e, t2.cd4_1 f
	from flat_lab_data_3 t1
	join flat_lab_data_complete t2 using (encounter_id)
) t3
where (c != f or (c is null and f is not null) or (c is not null and f is null));

set @person_id = 48547;
select obs_id, person_id, encounter_id, obs_datetime, value_numeric, date_created, date_voided from amrs.obs where person_id=@person_id and concept_id=5497 order by obs_datetime;
select obs_id, person_id, encounter_id, obs_datetime, concept_id, value_numeric, date_created, date_voided from amrs.obs where person_id=@person_id and obs_id=188410290;
select * from flat_lab_data_complete where person_id=@person_id order by person_id, encounter_datetime;
select * from flat_lab_data_3 where person_id=@person_id order by person_id, encounter_datetime;
select obs_id, person_id, encounter_id, obs_datetime,concept_id, value_numeric, date_created, date_voided from amrs.obs where encounter_id=120023 


(select 'local',person_id, encounter_id, encounter_datetime, enc_cd4_count, lab_cd4_count from flat_lab_data where person_id = @person_id order by person_id, encounter_datetime)
union
(select 'original',person_id, encounter_id, encounter_datetime, enc_cd4_count, lab_cd4_count from flat_lab_data_complete where person_id = @person_id order by person_id, encounter_datetime);



select * from amrs.encounter where encounter_id=120023
select obs_id,concept_id,date_created,voided from amrs.obs where encounter_id=120023