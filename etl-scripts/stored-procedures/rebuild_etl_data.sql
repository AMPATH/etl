DELIMITER $$
CREATE  PROCEDURE `rebuild_etl_data`()
BEGIN
			select @fake_visit_id := 10000000;
			select @boundary := "!!";
			select @sep := " ## ";


			select @person_ids_count := (select count(*) from rebuild_etl_data_queue);

			while @person_ids_count > 0 do

					drop table if exists rebuild_etl_data_queue_0;
					create temporary table rebuild_etl_data_queue_0 (select * from rebuild_etl_data_queue limit 500); 


					delete t1 
						from flat_obs t1 
						join rebuild_etl_data_queue_0 t2 using (person_id);

					replace into flat_obs
					(select
						o.person_id,
						case
							when e.visit_id is not null then e.visit_id else @fake_visit_id :=@fake_visit_id + 1
						end as visit_id,
						o.encounter_id,
						encounter_datetime,
						encounter_type,
						e.location_id,
						group_concat(
							case
								when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
								when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
								when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
								when value_complex is not null then concat(@boundary,o.concept_id,'=',value_complex,@boundary)
								when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
								when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
								when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
							end
							order by concept_id,value_coded
							separator ' ## '
						) as obs,

						group_concat(
							case
								when value_coded is not null or value_numeric is not null or value_datetime is not null or value_boolean is not null or value_text is not null or value_drug is not null or value_modifier is not null
								then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
							end
							order by o.concept_id,value_coded
							separator ' ## '
						) as obs_datetimes,
						max(o.date_created) as max_date_created

						from amrs.obs o
					
							join amrs.encounter e using (encounter_id)
							join rebuild_etl_data_queue_0 t2 using (person_id)
						where o.voided=0
						
						group by o.encounter_id
					);

					


					
					replace into flat_obs
					(select
						o.person_id,
						@fake_visit_id :=@fake_visit_id + 1 as visit_id,
						min(o.obs_id) + 100000000 as encounter_id,
						o.obs_datetime,
						99999 as encounter_type,
						null as location_id,
						group_concat(
							case
								when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
								when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
								when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
								when value_complex is not null then concat(@boundary,o.concept_id,'=',value_complex,@boundary)
								when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
								when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
								when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
							end
							order by concept_id,value_coded
							separator ' ## '
						) as obs,

						group_concat(
							case
								when value_coded is not null or value_numeric is not null or value_datetime is not null or value_boolean is not null or value_text is not null or value_drug is not null or value_modifier is not null
								then concat(@boundary,o.concept_id,'=',date(o.obs_datetime),@boundary)
							end
							order by o.concept_id,value_coded
							separator ' ## '
						) as obs_datetimes,
						max(o.date_created) as max_date_created

						from amrs.obs o 
							join rebuild_etl_data_queue_0 t2 using (person_id)
						where
							o.encounter_id is null
							and voided=0
						group by person_id, o.obs_datetime
					);

					
					delete t1
						from flat_lab_obs t1
						join rebuild_etl_data_queue_0 t2 using (person_id);



					replace into flat_lab_obs
					(select
						o.person_id,
						min(o.obs_id) + 100000000 as encounter_id,
						date(o.obs_datetime),
						99999 as encounter_type,
						null as location_id,
						group_concat(
							case
								when value_coded is not null then concat(@boundary,o.concept_id,'=',value_coded,@boundary)
								when value_numeric is not null then concat(@boundary,o.concept_id,'=',value_numeric,@boundary)
								when value_datetime is not null then concat(@boundary,o.concept_id,'=',date(value_datetime),@boundary)
								when value_complex is not null then concat(@boundary,o.concept_id,'=',value_complex,@boundary)
								when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
								when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
								when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
							end
							order by concept_id,value_coded
							separator ' ## '
						) as obs,
						max(o.date_created) as max_date_created,
						group_concat(concat(@boundary,o.concept_id,'=',o.value_coded,"=",if(encounter_id is not null,encounter_id,""),@boundary)) as encounter_ids,
						group_concat(concat(@boundary,o.concept_id,'=',o.obs_id,@boundary)) as obs_ids

						from amrs.obs o 
							join rebuild_etl_data_queue_0 t2 using (person_id)
						where voided=0
					
							and concept_id in (856,5497,730,21,653,790,12,1030,1040,1271,9239,9020,9508, 6126, 887, 6252, 1537, 857)
							and if(concept_id=1271 and value_coded=1107,false,true) 
						group by person_id, date(o.obs_datetime)
					);

					

					
					
					delete t1
						from flat_orders t1
						join rebuild_etl_data_queue_0 t2 using (person_id);
					

					
					replace into flat_orders
					(select
						o.patient_id,
						o.encounter_id,
						o.order_id,
						e.encounter_datetime,
						e.encounter_type,
						e.location_id,
						group_concat(concept_id order by o.concept_id separator ' ## ') as orders,
						group_concat(concat(@boundary,o.concept_id,'=',date(o.date_activated),@boundary) order by o.concept_id separator ' ## ') as order_datetimes,
						max(o.date_created) as max_date_created

						from amrs.orders o
							join amrs.encounter e using (encounter_id)
							join rebuild_etl_data_queue_0 t2 on o.patient_id = t2.person_id
						where o.encounter_id > 0
							and o.voided=0 
							
						group by o.encounter_id
					);


					
					delete t1
						from flat_hiv_summary t1
						join rebuild_etl_data_queue_0 t2 using (person_id);

					replace into flat_hiv_summary_queue (select * from rebuild_etl_data_queue_0); 

					
					



					
					delete t1 
						from flat_labs_and_imaging t1
						join rebuild_etl_data_queue_0 using (person_id);

					select @unknown_encounter_type := 99999;

					drop table if exists new_data_person_ids_0;
					create temporary table new_data_person_ids_0(person_id int, primary key (person_id));
					insert into new_data_person_ids_0 (select * from rebuild_etl_data_queue_0);


					drop table if exists flat_labs_and_imaging_0;
					create temporary table flat_labs_and_imaging_0(index encounter_id (encounter_id), index person_test (person_id,test_datetime))
					(select * from
					((select t1.person_id,
						t1.encounter_id,
						t1.test_datetime,
						t1.encounter_type,
						t1.location_id,
						t1.obs
						,t2.orders
						from flat_lab_obs t1
							join new_data_person_ids_0 t0 using (person_id)
							left join flat_orders t2 on date(t1.test_datetime) = date(t2.encounter_datetime) and t1.person_id = t2.person_id)
							UNION ALL
					( select t1.person_id,
						t1.encounter_id,
						t1.encounter_datetime as test_datetime,
						t2.encounter_type,
						t2.location_id,
						t2.obs,
						t1.orders
						from flat_orders t1
							left join flat_lab_obs t2 on date(t2.test_datetime) = date(t1.encounter_datetime) and t1.person_id = t2.person_id
							where 
								t1.person_id=@person_id
								and t2.person_id is null

					) order by person_id, test_datetime) as derived );


					select @prev_id := null;
					select @cur_id := null;
					select @cur_location := null;
					select @vl := null;
					select @cd4_count := null;
					Select @cd4_percent := null;
					select @hemoglobin := null;
					select @ast := null;
					select @creatinine := null;
					select @chest_xray := null;

					drop temporary table if exists flat_labs_and_imaging_1;
					create temporary table flat_labs_and_imaging_1 (index encounter_id (encounter_id))
					(select
						@prev_id := @cur_id as prev_id,
						@cur_id := t1.person_id as cur_id,
						t1.person_id,
						p.uuid,
						t1.encounter_id,
						t1.test_datetime,
						t1.encounter_type,

						case
							when location_id then @cur_location := location_id
							when @prev_id = @cur_id then @cur_location
							else null
						end as location_id,

						
						
						
						
						
						
						
						
						
						
						
						
						
						
						
						
						

						if(obs regexp "!!1030=",cast(replace(replace((substring_index(substring(obs,locate("1030=",obs)),@sep,1)),"1030=",""),"!!","") as unsigned),null) as hiv_dna_pcr,
						if(obs regexp "!!1040=",cast(replace(replace((substring_index(substring(obs,locate("1040=",obs)),@sep,1)),"1040=",""),"!!","") as unsigned),null) as hiv_rapid_test,

						case
							when obs regexp "!!856=" then cast(replace(replace((substring_index(substring(obs,locate("856=",obs)),@sep,1)),"856=",""),"!!","") as unsigned)
						end as hiv_viral_load,
						if(obs regexp "!!5497=",cast(replace(replace((substring_index(substring(obs,locate("5497=",obs)),@sep,1)),"5497=",""),"!!","") as unsigned),null) as cd4_count,
						if(obs regexp "!!730=",cast(replace(replace((substring_index(substring(obs,locate("730=",obs)),@sep,1)),"730=",""),"!!","") as decimal(3,1)),null) as cd4_percent,
						if(obs regexp "!!21=",cast(replace(replace((substring_index(substring(obs,locate("21=",obs)),@sep,1)),"21=",""),"!!","") as decimal(4,1)),null) as hemoglobin,
						if(obs regexp "!!653=",cast(replace(replace((substring_index(substring(obs,locate("653=",obs)),@sep,1)),"653=",""),"!!","") as unsigned),null) as ast,
						if(obs regexp "!!790=",cast(replace(replace((substring_index(substring(obs,locate("790=",obs)),@sep,1)),"790=",""),"!!","") as decimal(4,1)),null) as creatinine,
						if(obs regexp "!!12=" and not obs regexp "!!12=1107",cast(replace(replace((substring_index(substring(obs,locate("12=",obs)),@sep,1)),"12=",""),"!!","") as unsigned),null) as chest_xray,
						if(obs regexp "!!6126=",cast(replace(replace((substring_index(substring(obs,locate("6126=",obs)),@sep,1)),"6126=",""),"!!","") as decimal(4,1)),null) as hba1c,
						if(obs regexp "!!887=",cast(replace(replace((substring_index(substring(obs,locate("887=",obs)),@sep,1)),"887=",""),"!!","") as decimal(4,1)),null) as rbs,
						if(obs regexp "!!6252=",cast(replace(replace((substring_index(substring(obs,locate("6252=",obs)),@sep,1)),"6252=",""),"!!","") as decimal(4,1)),null) as fbs,
						if(obs regexp "!!1537=",cast(replace(replace((substring_index(substring(obs,locate("1537=",obs)),@sep,1)),"1537=",""),"!!","") as unsigned),null) as ecg,
						if(obs regexp "!!857=",cast(replace(replace((substring_index(substring(obs,locate("857=",obs)),@sep,1)),"857=",""),"!!","") as decimal(4,1)),null) as urea,
						if(obs regexp "!!9239=",obs,null) as has_errors,
						if(obs regexp "!!9239=856!!",1,null) as vl_error,
						if(obs regexp "!!9239=5497!!",1,null) as cd4_error,
						if(obs regexp "!!9239=1030",1,null) as hiv_dna_pcr_error,
						CASE
							WHEN
								(obs REGEXP '!!1271='
									AND NOT obs REGEXP '!!1271=1107')
							THEN
								CONCAT(REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!1271=', obs)),
														@sep,
														ROUND((LENGTH(obs) - LENGTH(REPLACE(obs, '1271=', ''))) / LENGTH('!!1271=')))),
												'!!1271=',
												''),
											'!!',
											''),
										' ## ',
										IFNULL(orders, ''))
							ELSE orders
						END AS tests_ordered

					from flat_labs_and_imaging_0 t1
						join amrs.person p using (person_id)
					);

					replace into flat_labs_and_imaging
					(select
						person_id,
						t1.uuid,
						encounter_id,
						test_datetime,
						encounter_type,
						hiv_dna_pcr,
						hiv_rapid_test,
						hiv_viral_load,
						cd4_count,
						cd4_percent,
						hemoglobin,
						ast,
						creatinine,
						chest_xray,
						hba1c,
						rbs,
						fbs,
						ecg,
						urea,
						has_errors,
						vl_error,
						cd4_error,
						hiv_dna_pcr_error,
						tests_ordered
					from flat_labs_and_imaging_1 t1
					);


					



					
					delete t1
						from flat_vitals t1 
						join rebuild_etl_data_queue_0 t2 using (person_id);

					drop table if exists new_data_person_ids;
					create temporary table new_data_person_ids(person_id int, primary key (person_id));
					insert into new_data_person_ids (select * from rebuild_etl_data_queue_0);

					drop table if exists flat_vitals_0;
					create temporary table flat_vitals_0(encounter_id int, primary key (encounter_id), index person_enc_date (person_id,encounter_datetime))
					(select
						t1.person_id,
						t1.encounter_id,
						t1.encounter_datetime,
						t1.encounter_type,
						t1.location_id,
						t1.obs,
						t1.obs_datetimes
						from flat_obs t1
							join new_data_person_ids t0 using (person_id)
						where encounter_type in (1,2,3,4,10,13,14,15,17,19,22,23,26,32,33,43,47,110,112,113,114,115)
						order by person_id, encounter_datetime
					);
					
					select @prev_id := null;
					select @cur_id := null;
					select @cur_location := null;
					select @systolic_bp := null;
					select @diastolic_bp := null;
					SELECT @pulse := null;
					select @temp := null;
					select @oxygen_sat := null;
					select @weight := null;
					select @height := null;

					drop temporary table if exists flat_vitals_1;
					create temporary table flat_vitals_1 (index encounter_id (encounter_id))
					(select
						@prev_id := @cur_id as prev_id,
						@cur_id := t1.person_id as cur_id,
						t1.person_id,
						p.uuid,
						t1.encounter_id,
						t1.encounter_datetime,
						case
							when location_id then @cur_location := location_id
							when @prev_id = @cur_id then @cur_location
							else null
						end as location_id,

						
						
						
						
						
						
						

						if(obs regexp "!!5089=",cast(replace(replace((substring_index(substring(obs,locate("!!5089=",obs)),@sep,1)),"!!5089=",""),"!!","") as decimal(4,1)),null) as weight,
						if(obs regexp "!!5090=",cast(replace(replace((substring_index(substring(obs,locate("!!5090=",obs)),@sep,1)),"!!5090=",""),"!!","") as decimal(4,1)),null) as height,
						if(obs regexp "!!5088=",cast(replace(replace((substring_index(substring(obs,locate("!!5088=",obs)),@sep,1)),"!!5088=",""),"!!","") as decimal(4,1)),null) as temp,
						if(obs regexp "!!5092=",cast(replace(replace((substring_index(substring(obs,locate("!!5092=",obs)),@sep,1)),"!!5092=",""),"!!","") as unsigned),null) as oxygen_sat,
						if(obs regexp "!!5085=",cast(replace(replace((substring_index(substring(obs,locate("!!5085=",obs)),@sep,1)),"!!5085=",""),"!!","") as unsigned),null) as systolic_bp,
						if(obs regexp "!!5086=",cast(replace(replace((substring_index(substring(obs,locate("!!5086=",obs)),@sep,1)),"!!5086=",""),"!!","") as unsigned),null) as diastolic_bp,
						if(obs regexp "!!5087=",cast(replace(replace((substring_index(substring(obs,locate("!!5087=",obs)),@sep,1)),"!!5087=",""),"!!","") as unsigned),null) as pulse

					from flat_vitals_0 t1
						join amrs.person p using (person_id)
					);



					replace into flat_vitals
					(select
						person_id,
						uuid,
						encounter_id,
						encounter_datetime,
						location_id,
						weight,
						height,
						temp,
						oxygen_sat,
						systolic_bp,
						diastolic_bp,
						pulse
					from flat_vitals_1);

					

					
					delete t1
						from flat_pep_summary t1
						join rebuild_etl_data_queue_0 t2 using(person_id);

					drop table if exists new_data_person_ids_0;
					create temporary table new_data_person_ids_0(person_id int, primary key (person_id));
					insert into new_data_person_ids_0 (select * from rebuild_etl_data_queue_0);


					drop table if exists flat_pep_summary_0a;
					create temporary table flat_pep_summary_0a
					(select
						t1.person_id,
						t1.visit_id,
						t1.encounter_id,
						t1.encounter_datetime,
						t1.encounter_type,
						t1.location_id,
						t1.obs,
						t1.obs_datetimes
						from flat_obs t1
							join new_data_person_ids_0 t0 using (person_id)
						where t1.encounter_type in (56,57)
					);

					drop table if exists flat_pep_summary_0;
					create temporary table flat_pep_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
					(select * from flat_pep_summary_0a
					order by person_id, date(encounter_datetime)
					);


					SELECT @prev_id:=NULL;
					SELECT @cur_id:=NULL;
					SELECT @enrollment_date:=NULL;
					SELECT @cur_location:=NULL;
					SELECT @cur_rtc_date:=NULL;
					SELECT @prev_rtc_date:=NULL;

					SELECT @hiv_exposed_occupational:=NULL;
					SELECT @pep_start_date:=NULL;

					drop temporary table if exists flat_pep_summary_1;
					create temporary table flat_pep_summary_1 (index encounter_id (encounter_id))
					(select
						@prev_id := @cur_id as prev_id,
						@cur_id := t1.person_id as cur_id,
						t1.person_id,
						p.uuid as uuid,
						t1.visit_id,
						t1.encounter_id,
						t1.encounter_datetime,
						t1.encounter_type,

						case
							when @prev_id != @cur_id then @enrollment_date := encounter_datetime
							when  @enrollment_date is null then @enrollment_date := encounter_datetime
							else @enrollment_date
						end as enrollment_date,

						case
							when location_id then @cur_location := location_id
							when @prev_id = @cur_id then @cur_location
							else null
						end as location_id,

						case
							when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
							else @prev_rtc_date := null
						end as prev_rtc_date,

						
						case
							when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
							when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
							else @cur_rtc_date := null
						end as cur_rtc_date,

						case
							when  obs regexp "!!1972=(5619|5507|1496|6280|8710|8713|8714)" then @hiv_exposed_occupational := 1
							when !(obs regexp "!!1972=(5619|5507|1496|6280|8710|8713|8714)") then @hiv_exposed_occupational := 0
							when @prev_id = @cur_id then @hiv_exposed_occupational
							else @hiv_exposed_occupational := null
						end as hiv_exposed_occupational,
						case
							when encounter_type = 56 and (obs regexp "!!1705=1149" or obs regexp  "!!7123=") then @pep_start_date := encounter_datetime
							when @prev_id = @cur_id then @pep_start_date
							else @pep_start_date := null
						end as pep_start_date

					from flat_pep_summary_0 t1
						join amrs.person p using (person_id)
					);

					replace into flat_pep_summary
					(select
						person_id,
						t1.uuid,
						t1.visit_id,
						encounter_id,
						encounter_datetime,
						encounter_type,
						location_id,
						t2.uuid as location_uuid,
						enrollment_date,
						prev_rtc_date,
						cur_rtc_date,
						hiv_exposed_occupational,
						pep_start_date
						from flat_pep_summary_1 t1
							join amrs.location t2 using (location_id));
			
            
						
						delete t1 
							from rebuild_etl_data_queue t1 
							join rebuild_etl_data_queue_0 t2 using (person_id);
							
							select @person_ids_count := (select count(*) from rebuild_etl_data_queue);

			 end while;
                    
		END$$
DELIMITER ;
