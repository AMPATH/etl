select @today := curdate();
select 
	location_id,name,
	count(*) total,
	count(if(transfer_care in (1287,9068) or outreach_reason_exited_care=1594 or patient_care_status in (1287,9068),1,null)) as transfer_out,
	count(if(datediff(@today,if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 30 
				and (patient_care_status is null or patient_care_status in (9080,9083,6101,1286))
				and (transfer_care is null or transfer_care=1286)
				and (outreach_reason_exited_care is null or outreach_reason_exited_care != 1594)
				and cur_arv_line is not null
		,1,null)) num_missing_on_arvs,
	count(if(datediff(@today,if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 30 
				and (patient_care_status is null or patient_care_status in (9080,9083,6101,1286))
				and (transfer_care is null or transfer_care=1286)
				and (outreach_reason_exited_care is null or outreach_reason_exited_care != 1594)
				and cur_arv_line is not null
		,1,null))/count(*) perc_missing_on_arvs,

	count(if(datediff(@today,if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (patient_care_status is null or patient_care_status in (9080,9083,6101,1286))
				and (transfer_care is null or transfer_care=1286)
				and (outreach_reason_exited_care is null or outreach_reason_exited_care != 1594)
		,1,null)) num_lost,
	count(if(datediff(@today,if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (patient_care_status is null or patient_care_status in (9080,9083,6101,1286))
				and (transfer_care is null or transfer_care=1286)
				and (outreach_reason_exited_care is null or outreach_reason_exited_care != 1594)
		,1,null))/count(*) perc_lost,
	count(if(datediff(@today,if(t1.rtc_date,t1.rtc_date,date_add(t1.encounter_datetime, interval 90 day))) >= 90 
				and (patient_care_status is null or patient_care_status in (9080,9083,6101,1286))
				and (transfer_care is null or transfer_care=1286)
				and (outreach_reason_exited_care is null or outreach_reason_exited_care != 1594)
				and cur_arv_line is not null
		,1,null)) num_lost_on_arvs,
	count(if(cur_arv_line is null,1,null)) as not_on_arvs,
	count(if(cur_arv_line,1,null))/count(*) as perc_on_arvs,
	count(if(cur_arv_line=1,1,null)) as on_first_line,
	count(if(cur_arv_line=2,1,null)) as on_second_line,
	count(if(cur_arv_line is null and datediff(@today,cd4_1_date) > 365,1,null)) as need_cd4,
	count(if(edd > @today and cur_arv_line > 0,1,null)) as preg_on_arvs,
	count(if(edd > @today and cur_arv_line is null,1,null)) as preg_not_on_arvs,
	count(if((cd4_1 < 500 or edd is not null) and cur_arv_line is null,1,null)) as art_eligible,
	count(if(cur_arv_line is null and datediff(@today,cd4_1_date) > 365,1,null)) as need_cd4,
	count(if(vl_1 >= 0,1,null)) as num_vls,
	count(if(vl_1 < 1000,1,null))/count(if(vl_1 >= 0,1,null)) as perc_suppressed,
	count(if(cur_arv_line > 0 and datediff(@today,vl_1_date) <= 365,1,null)) as vl_in_past_year,
	count(if(cur_arv_line > 0 and (datediff(@today,vl_1_date) > 365 or vl_1_date is null),1,null)) as need_yearly_vl,
	count(if(cur_arv_line > 0 and datediff(@today,vl_1_date) > 180 and vl_1 >= 1000,1,null)) as non_suppressed_need_vl,
	count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as num_two_vls,
	count(if(vl_1 >= 1000 and vl_2 >= 1000,1,null))/count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as consec_non_suppressed,
	count(if(vl_1 >= 1000 and vl_2 < 1000,1,null))/count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as non_suppressed_to_suppressed,
	count(if(vl_1 < 1000 and vl_2 < 1000,1,null))/count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as two_suppressed,
	count(if(vl_1 < 1000 and vl_2 >= 1000,1,null))/count(if(vl_1 >= 0 and vl_2 >= 0,1,null)) as suppressed_to_non_suppressed

from 
	flat_moh_indicators t1
	join derived_encounter using (encounter_id)
	join amrs.encounter using (encounter_id)
	join amrs.location using (location_id)
	left outer join flat_encounter using (encounter_id)
	left outer join flat_handp using (encounter_id)
	where next_encounter_datetime is null		
		and t1.encounter_datetime >= "2014-01-01" 
		and t1.death_date is null
	group by location_id with rollup;


/*
select * 
from 
	flat_moh_indicators t1
	join derived_encounter using (encounter_id)
	where next_encounter_datetime is null
		and encounter_datetime >= "2014-01-01" and death_date is null
		and edd >= @today and cur_arv_line is null;
*/