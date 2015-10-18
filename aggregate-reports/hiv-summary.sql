# # of patients
# # / % of patients not on arvs (denominator: all patients who went to clinic)
# # / % of patients on arvs (denominator: all patients who went to clinic)
# # / % on first line (denominator: all patients on arvs)
# # / % on second line (denominator: all patients on arvs)
# # of patients with viral loads in past 12 months (denominator: all patients on arvs)
# # / % with viral loads < 1000 (denominator: all patients on arvs)
# # / % with viral loads > 1000 (denominator: all patients on arvs)
# # of viral load orders (denominator: # with test ordered in filtered time period)
# # with viral load order but no result (denominator: # with test ordered in filtered time period)
# # requiring viral loads (no viral load in past 1 year)
# # who needed viral load
# % getting viral load ordered who needed viral load
# # of pregnant patients
# % of pregnant patients on arvs (denominator: # of pregnant patients)



select
	name as location,
	location_uuid,
	count(*) as total_encounters,
	count(distinct person_id) as num_patients,
	count(distinct if(cur_arv_line is not null,person_id,null)) as on_arvs,
	count(distinct if(cur_arv_line=1,person_id,null)) as on_arvs_first_line,
	count(distinct if(cur_arv_line=2,person_id,null)) as on_arvs_second_line,
	count(distinct if(cur_arv_line>2,person_id,null)) as on_arvs_third_line,
	count(distinct if(timestampdiff(week,vl_1_date,encounter_datetime) <= 52,person_id,null)) as vl_done_past_year,
	count(distinct if(cur_arv_line is not null and (vl_1_date is null or timestampdiff(week,vl_1_date,encounter_datetime) >= 52),person_id,null)) as no_vl_in_past_year,

	count(distinct if(timestampdiff(week,vl_1_date,encounter_datetime) <= 52 and vl_1 <= 1000,person_id,null)) as vl_done_past_year_lte_1000,
	count(distinct if(timestampdiff(week,vl_1_date,encounter_datetime) <= 52 and vl_1 > 1000,person_id,null)) as vl_done_past_year_gt_1000,

	count(distinct if(vl_resulted >= 0,person_id,null)) as vl_done_this_encounter,
	count(distinct if(vl_resulted <= 1000,person_id,null)) as vl_done_this_encounter_lte_1000,
	count(distinct if(vl_resulted > 1000,person_id,null)) as vl_done_this_encounter_gt_1000,
	count(distinct if(vl_order_date=encounter_datetime,person_id,null)) as vl_ordered,
	count(distinct if(timestampdiff(week,vl_order_date,encounter_datetime) >= 0 and vl_1_date < vl_order_date,person_id,null)) as pending_vl_order,
	count(distinct if(timestampdiff(week,vl_order_date,encounter_datetime) >= 4 and vl_1_date < vl_order_date,person_id,null)) as pending_vl_order_no_result_after_4_weeks,

	count(distinct if(timestampdiff(week,arv_start_date,encounter_datetime) <= 26,person_id,null)) as on_arvs_lte_26_weeks,
	count(distinct if(timestampdiff(week,arv_start_date,encounter_datetime) <= 52,person_id,null)) as on_arvs_lte_52_weeks,
	count(distinct if(timestampdiff(week,arv_start_date,encounter_datetime) <= 52 and timestampdiff(week,vl_1_date,encounter_datetime) <= 52,person_id,null)) as on_arvs_lte_52_weeks_and_have_vl,
	count(distinct if(timestampdiff(week,arv_start_date,encounter_datetime) <= 52 and timestampdiff(week,vl_1_date,encounter_datetime) <= 52 and vl_1 <= 1000,person_id,null)) as on_arvs_lte_52_weeks_and_have_vl_lte_1000,

	count(distinct if(edd > encounter_datetime,person_id,null)) as num_pregnant,
	count(distinct if(edd > encounter_datetime,person_id,null) and cur_arv_line is not null) as num_pregnant_and_on_arvs

	from flat_hiv_summary t1
		join amrs.location t2 on t1.location_uuid = t2.uuid
	where 
		encounter_datetime between "2015-07-01" and "2015-09-30"
#		and location_uuid = "08feb14c-1352-11df-a1f1-0026b9348838"
#		and death_date is null
#		and out_of_care is null
#		and transfer_out is null
	group by location_uuid
;

/*


[
{
	name:is_pregnant,
	label: "# of pregnant patient in a given time period",
	expression: "etl.flat_hiv_summary.edd > etl.flat_hiv_summary.encounter_datetime",
	resource: "etl.hiv_summary"
},
]
	

getCounts(indicators,dateParameters,locations) {
     s = "select ";
	tables = getTables(indicators) //need to give each table a alias which will then be used in the expression
	for(i in indicators) {
		alias = tables[i.table].alias;
        s+= "count(distinct " + alias + ".person_id if(" + i.expression + ",person_id,null)) as " + i.name)
	    tables.push(s.table)
	}
	s += createTableJoin(tables),
	s += "where encounter_datetime betwee " + dateParameters.startDate + " and " dateParameters.endDate"
	s += "group by location_uuid"
	

}
*/
		
#	select encounter_datetime, person_id, vl_order_date, vl_1_date
