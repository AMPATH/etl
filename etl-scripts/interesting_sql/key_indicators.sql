#create view hiv_summary_indicators as
(select 
	t1.location_id,
	t1.location_uuid,
	name as clinic,
	count(*) total,
	count(if(death_date,1,null)) as num_deaths,
	count(transfer_out) as transfer_out,
	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 30 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and transfer_out is null
				and not obs regexp "1596=1594"
				and cur_arv_line is not null
		,1,null)) num_missing_on_arvs,
	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 30 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and transfer_out is null
				and not obs regexp "1596=1594"
				and cur_arv_line is not null
		,1,null))/count(*) perc_missing_on_arvs,

	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and transfer_out is null
				and not obs regexp "1596=1594"
		,1,null)) num_lost,
	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and transfer_out is null
				and not obs regexp "1596=1594"
		,1,null))/count(*) perc_lost,
	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and transfer_out is null
				and not obs regexp "1596=1594"
				and cur_arv_line is not null
		,1,null)) num_lost_on_arvs,
	count(if(cur_arv_line is null,1,null)) as not_on_arvs,
	count(if(cur_arv_line,1,null))/count(*) as perc_on_arvs,
	count(if(cur_arv_line=1,1,null)) as on_first_line,
	count(if(cur_arv_line=2,1,null)) as on_second_line,
	count(if(edd > curdate() and cur_arv_line > 0,1,null)) as preg_on_arvs,
	count(if(edd > curdate() and cur_arv_line is null,1,null)) as preg_not_on_arvs,
	count(if((cd4_1 < 500 or edd is not null) and cur_arv_line is null,1,null)) as art_eligible,
	count(if(cur_arv_line is null and datediff(curdate(),cd4_1_date) > 365,1,null)) as need_cd4,
	count(if(vl_1 >= 0 and datediff(curdate(),vl_1_date) <= 365,1,null)) as num_vls_past_year,
	count(if(vl_1 < 1000 and datediff(curdate(),vl_1_date) <= 365,1,null))/count(if(vl_1 >= 0 and datediff(curdate(),vl_1_date) <= 365,1,null)) as perc_suppressed_past_year,
	count(if(cur_arv_line > 0 and datediff(curdate(),vl_1_date) <= 365,1,null)) as vl_in_past_year,
	count(if(cur_arv_line > 0 and (datediff(curdate(),vl_1_date) > 365 or vl_1_date is null),1,null)) as need_yearly_vl,
	count(if(cur_arv_line > 0 and datediff(curdate(),vl_1_date) > 180 and vl_1 >= 1000,1,null)) as non_suppressed_need_vl

from 
	flat_hiv_summary t1 use index (location_rtc)
	join amrs.location using (location_id)
	join derived_encounter using (encounter_id)
	join etl.flat_obs using (encounter_id)
	where next_encounter_datetime is null		
		and datediff(curdate(),t1.encounter_datetime) <= 365 
group by t1.location_id
with rollup
);


