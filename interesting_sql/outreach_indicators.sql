select year(encounter_datetime) as year,
	month(encounter_datetime) as month,
	count(*),
	count(if(ampath_status_outreach,ampath_status_outreach in (9079,9080,9083,6101,1286,9068),1)) as mod_num,	
	count(if(if(ampath_status_outreach,ampath_status_outreach in (9079,9080,9083,6101,1286,9068),1) and datediff(next_appt_date,rtc_date_outreach) < 30,1,null)) as num_rtc_30,
	count(if(if(ampath_status_outreach,ampath_status_outreach in (9079,9080,9083,6101,1286,9068),1) and datediff(next_appt_date,rtc_date_outreach) < 30,1,null))/count(if(ampath_status_outreach,ampath_status_outreach in (9079,9080,9083,6101,1286,9068),1)) as returned_in_30_days,
	count(if(if(ampath_status_outreach,ampath_status_outreach in (9079,9080,9083,6101,1286,9068),1) and datediff(next_appt_date,rtc_date_outreach) < 60,1,null)) as num_rtc_60,
	count(if(if(ampath_status_outreach,ampath_status_outreach in (9079,9080,9083,6101,1286,9068),1) and datediff(next_appt_date,rtc_date_outreach) < 60,1,null))/count(if(ampath_status_outreach,ampath_status_outreach in (9079,9080,9083,6101,1286,9068),1)) as returned_in_60_days

from flat_outreach_data
	where encounter_datetime >= "2013-04-01"
		and if(ampath_status_outreach=9080,next_appt_date is null,1)
group by year, month
order by year, month;

select 
	year(encounter_datetime) as year,
	month(encounter_datetime) as month,
	count(if(datediff(next_appt_date,rtc_date) > 30 and next_encounter_type=21,1,null)) as num_getting_follow_up,
	count(if(datediff(next_appt_date,rtc_date) > 30 and next_encounter_type=21,1,null))/count(if(datediff(next_appt_date,rtc_date) > 30,1,null)) as perc_getting_follow_up
	from flat_retention_data
		where encounter_datetime >= "2013-04-01"
		and if(ampath_status=9080,next_appt_date is null,1)
group by year, month
order by year, month;

	

;