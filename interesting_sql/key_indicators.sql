select @today := curdate();

drop view key_indicators;
create view key_indicators as
explain
(select 
	t1.location_id,name,
	count(*) total,
	count(transfer_out) as transfer_out,
	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 30 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and not transfer_out
				and not obs regexp "1596=1594"
				and cur_arv_line is not null
		,1,null)) num_missing_on_arvs,
	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 30 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and not transfer_out
				and not obs regexp "1596=1594"
				and cur_arv_line is not null
		,1,null))/count(*) perc_missing_on_arvs,

	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and not transfer_out
				and not obs regexp "1596=1594"
		,1,null)) num_lost,
	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and not transfer_out
				and not obs regexp "1596=1594"
		,1,null))/count(*) perc_lost,
	count(if(datediff(curdate(),if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (not obs regexp "9082=" or obs regexp "9082=(9080|9083|6101|1286)")
				and not transfer_out
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
	count(if(vl_1 >= 0,1,null)) as num_vls,
	count(if(vl_1 < 1000,1,null))/count(if(vl_1 >= 0,1,null)) as perc_suppressed,
	count(if(cur_arv_line > 0 and datediff(curdate(),vl_1_date) <= 365,1,null)) as vl_in_past_year,
	count(if(cur_arv_line > 0 and (datediff(curdate(),vl_1_date) > 365 or vl_1_date is null),1,null)) as need_yearly_vl,
	count(if(cur_arv_line > 0 and datediff(curdate(),vl_1_date) > 180 and vl_1 >= 1000,1,null)) as non_suppressed_need_vl,
	count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as num_two_vls,
	count(if(vl_1 >= 1000 and vl_2 >= 1000,1,null))/count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as consec_non_suppressed,
	count(if(vl_1 >= 1000 and vl_2 < 1000,1,null))/count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as non_suppressed_to_suppressed,
	count(if(vl_1 < 1000 and vl_2 < 1000,1,null))/count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as two_suppressed,
	count(if(vl_1 < 1000 and vl_2 >= 1000,1,null))/count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as suppressed_to_non_suppressed

from 
	flat_hiv_summary t1 use index (location_rtc)
	join amrs.location using (location_id)
	join derived_encounter using (encounter_id)
	join amrs.encounter using (encounter_id)
	join reporting_JD.flat_obs using (encounter_id)
	where next_encounter_datetime is null		
		and t1.encounter_datetime >= "2014-01-01" 
		and t1.death_date is null
		and location_uuid = '08feae7c-1352-11df-a1f1-0026b9348838'
);

select * from key_indicators;

/*
select * 
from 
	flat_moh_indicators t1
	join derived_encounter using (encounter_id)
	where next_encounter_datetime is null
		and encounter_datetime >= "2014-01-01" and death_date is null
		and edd >= curdate() and cur_arv_line is null;
*/