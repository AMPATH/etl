select @today := curdate();
select 
	count(*) total,
	count(if(datediff(@today,rtc_date) >= 90,1,null)) num_lost,
	count(if(cur_arv_line is null,1,null)) as not_on_arvs,
	count(if(cur_arv_line=1,1,null)) as on_first_line,
	count(if(cur_arv_line=2,1,null)) as on_second_line,
	count(if(cur_arv_line is null and datediff(@today,cd4_1_date) > 365,1,null)) as need_cd4,
	count(if(edd > @today and cur_arv_line > 0,1,null)) as preg_on_arvs,
	count(if(edd > @today and cur_arv_line is null,1,null)) as preg_not_on_arvs,
	count(if((cd4_1 < 500 or edd is not null) and cur_arv_line is null,1,null)) as art_eligible,
	count(if(cur_arv_line is null and datediff(@today,cd4_1_date) > 365,1,null)) as need_cd4,
	count(if(vl_1 >= 1000,1,null)) as num_not_suppressed,
	count(if(vl_1 < 1000,1,null)) as num_suppressed,
	count(if(cur_arv_line > 0 and datediff(@today,vl_1_date) <= 365,1,null)) as vl_in_past_year,
	count(if(cur_arv_line > 0 and datediff(@today,vl_1_date) > 365,1,null)) as need_yearly_vl,
	count(if(cur_arv_line > 0 and datediff(@today,vl_1_date) > 180 and vl_1 >= 1000,1,null)) as non_suppressed_need_vl
from 
	flat_moh_indicators t1
	join derived_encounter using (encounter_id)
	where next_encounter_datetime is null
		and encounter_datetime >= "2014-01-01" and death_date is null;

select * 
from 
	flat_moh_indicators t1
	join derived_encounter using (encounter_id)
	where next_encounter_datetime is null
		and encounter_datetime >= "2014-01-01" and death_date is null
		and edd >= @today and cur_arv_line is null;
