# PATIENTS QUALIFYING FOR OUTREACH
select 
	year(encounter_datetime) as year,
	month(encounter_datetime) as month,
	count(*) as 'num encounters',

#	sum(if(timestampdiff(day,encounter_datetime, rtc_date)<=14,1,0)) as 'num 2 week RTC date',
#	sum(if(timestampdiff(day,arv_start_date,encounter_datetime) < 90,1,0)) as 'num arvs started recently',
	sum(if(timestampdiff(day,encounter_datetime, rtc_date)<=14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,1,0)) as 'num high risk encounters',
	count(distinct if(timestampdiff(day,encounter_datetime, rtc_date)<=14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,person_id,null)) as 'num high risk patients',	

	sum(if(timestampdiff(day,encounter_datetime, rtc_date)<=14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,1,0)) 
	/ 
	count(*) as '% high risk encounters',


	sum(if( (timestampdiff(day,encounter_datetime, rtc_date)<=14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90) and (next_appt_date > rtc_date or next_appt_date is null),1,0)) 
	/ 
	sum(if(timestampdiff(day,encounter_datetime, rtc_date)<=14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90,1,0)) as '% late high risk encounters',

	count(distinct if((
		timestampdiff(day,encounter_datetime,rtc_date)<=14 or timestampdiff(day,arv_start_date,encounter_datetime) < 90) 
						and (next_appt_date > rtc_date or next_appt_date is null
		),person_id,null)) as 'num late high risk patients',

	sum(if(
				(timestampdiff(day,rtc_date,next_appt_date) between 30 and 89 or (next_appt_date is null and timestampdiff(day,rtc_date,curdate()) between 30 and 89)) 
				and (next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or next_encounter_type is null)
				and (timestampdiff(day,arv_start_date,encounter_datetime) > 90)
			,1,0)) as num_medium_risk,

	sum(if(
				(timestampdiff(day,rtc_date,next_appt_date) between 30 and 89 or (next_appt_date is null and timestampdiff(day,rtc_date,curdate()) between 30 and 89)) 
				and (next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or next_encounter_type is null)
				and arv_start_date is null
			,1,0)) as num_low_risk,

	sum(if(
				(timestampdiff(day,rtc_date,next_appt_date) >= 90 or (next_appt_date is null and timestampdiff(day,rtc_date,curdate()) >= 90)) 
				and (next_encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) or next_encounter_type is null)
			,1,0)) as num_ltfu

	from amrs.lost_to_follow_up
	where encounter_type in (1,2,3,4,10,11,12,14,15,17,19,26,44,46,47) and location_id=13
	group by year, month;


select * from amrs.location;


# of outreach forms
select 
	count(*)
	from amrs.encounter e 
	where encounter_type=21; 

# of patients for which outreach is attempted, percent reached by phone =65%
select 
	count(*), 
	sum(if(value_coded=1555,1,0)) as reached_by_phone,  
	sum(if(value_coded=1555,1,0)) / count(*) as percent_reached_by_phone
	from amrs.obs o
	join amrs.encounter e using (encounter_id)
	where concept_id = 1569 and encounter_type=21;


#  of patients qualifying for outreach, % attempted = ~15%
select 
	count(*)
	from amrs.obs o
	join amrs.encounter e using (encounter_id)
	where concept_id= 1725 and encounter_type=21; #18085


# of patients where outreach contacted, % found = 75%
select 
	count(*),
	sum(if(value_coded=1065,1,0)) / count(*)
	from amrs.obs o
	join amrs.encounter e using (encounter_id)
	where concept_id= 1559 and encounter_type=21;


# % of time field follow-up is successful

