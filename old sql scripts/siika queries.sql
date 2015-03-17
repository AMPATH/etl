

drop table if exists on_arvs;
create temporary table on_arvs (index person_id (person_id))
(select distinct person_id from flat_retention_data where arv_start_date);

select count(*) as num_vls, 
	count(distinct t1.person_id) num_pts, 
	count(distinct if(lab_vl >= 1000,t1.person_id,null)) as num_unsuppressed,
	count(distinct if(vl1 >= 1000 and vl2 >= 1000 and timestampdiff(day,vl2_date,vl1_date) >= 90,t1.person_id,null)) as persistent
#	count(distinct if(vl1 >= 1000 and vl2 >= 1000 and timestampdiff(day,vl1_date,vl2_date) >= 90 and prev_arv_line=1 and cur_arv_line=2,t1.person_id,null)) as arv_change

	from flat_lab_data t1
	join on_arvs t3 on t1.person_id=t3.person_id
	where lab_vl >= 0
30639	21681	8366	2235
;
select 
	count(distinct t1.person_id) num_pts, 
	count(distinct if(vl1 >= 1000,t1.person_id,null)) as num_unsuppressed,
	count(distinct if(vl1 >= 1000 and vl2 >= 1000 and vl1 != vl2 and timestampdiff(day,vl2_date,vl1_date) >= 90,t1.person_id,null)) as persistent,
	count(distinct if(vl1 >= 1000 and vl2 >= 1000 and vl1 != vl2 and timestampdiff(day,vl2_date,vl1_date) >= 90 and prev_arv_line=1 and cur_arv_line=2,t1.person_id,null)) as arv_change
	
	from flat_lab_data t1
	join flat_retention_data t2 using (encounter_id)
	where vl1 >=0 and arv_start_date <= '2013-10-01' and t1.encounter_datetime >= '2013-10-01';

select t1.person_id,t1.encounter_datetime,arv_meds,arv_plan,arv_meds_plan,prev_arv_line,cur_arv_line, vl2,vl2_date,vl1,vl1_date, timestampdiff(day,vl2_date,vl1_date) as days
	from flat_lab_data t1
	join flat_retention_data t2 using (encounter_id)
	where vl1 >= 1000 and vl2 >= 1000 and timestampdiff(day,vl2_date,vl1_date) between 90 and 180 and prev_arv_line=1 and cur_arv_line=2 and vl1 != vl2
	order by encounter_datetime desc;



select t1.person_id,t1.encounter_datetime,arv_meds,arv_plan,arv_meds_plan,prev_arv_line,cur_arv_line, vl2,vl2_date,vl1,vl1_date, timestampdiff(day,vl2_date,vl1_date) as days
	from flat_lab_data t1
	join flat_retention_data t2 using (encounter_id)
	where t1.person_id=50676 #196814

select count(distinct t1.person_id) #*,timestampdiff(year,birth_date,curdate()) as age
	from flat_lab_data t1
	join flat_retention_data t2 using (encounter_id)
	where prev_arv_line=2 and cur_arv_line=1
	and encounter_datetime >= '2013-10-01';
