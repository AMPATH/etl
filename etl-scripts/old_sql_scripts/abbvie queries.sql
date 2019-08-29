explain
select year(encounter_datetime) as year, month(encounter_datetime) as month, count(*), count(distinct provider_id),
	count(*)/count(distinct provider_id)
	from flat_outreach_defined t1
	join flat_retention_defined t2 using (encounter_id)
	join flat_retention_derived t3 using (encounter_id)
	where encounter_datetime >= "2013-01-01"
		and if(ampath_status_outreach=9080,next_appt_date is null,1)
	group by year, month
	order by year, month;

# Number of LTFU at a given time point, using clinic as last appt
select t1.location_id,name,count(*) as num_ltfu,count(if(arv_start_date <= @date_1,1,null)) as on_arvs
from
(select @date_1 := last_day('2014-06-30')) p0d,
reporting_JD.flat_retention_data t1
left outer join flat_outreach_defined t2 using (encounter_id)
join amrs.location using (location_id)
where 
	(encounter_datetime <= @date_1 and (next_hiv_clinic is null or next_appt_date > @date_1))
	and if(dead,t1.death_date < @date_1,1)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),@date_1) >= 90
	and location_id in (1,2,3,4,7,8,9,11,12,13,14,15,17,19,20,23,24,25,26,27,28,31,50,54,55,64,65,69,70,71,72,73,78,82,83,100,130,135)	
	and if(encounter_type=21,ampath_status_outreach in (6102,9079,1286,9080),1)
group by t1.location_id
order by t1.location_id;

create index enc_datetime on flat_retention_defined (encounter_datetime);



select t1.location_id,name,count(distinct t1.person_id) as num_ltfu
from
(select @date_1 := last_day('2014-09-30')) p0d,
reporting_JD.flat_retention_data t1
left outer join flat_outreach_defined t2 using (encounter_id)
join amrs.location t3 on t1.location_id=t3.location_id
where 
	(encounter_datetime <= @date_1 and (next_hiv_clinic_date is null or next_hiv_clinic_date > @date_1))
	and if(dead,t1.death_date < @date_1,1)
	and hiv_start_date
	and (discontinue_is_hiv_neg is null or discontinue_is_hiv_neg != 1065)
	and (transfer_care is null or transfer_care!=1286)
	and arv_start_date <= @date_1
	and if(encounter_type=21,ampath_status_outreach in (6102,9079,1286,9080),1)
	and timestampdiff(day,if(rtc_date,rtc_date,encounter_datetime),@date_1) >= 90
	and t1.location_id in (1,2,3,4,7,8,9,11,12,13,14,15,17,19,20,23,24,25,26,27,28,31,50,54,55,64,65,69,70,71,72,73,78,82,83,100,130,135)
group by t1.location_id
order by t1.location_id;
