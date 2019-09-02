DELIMITER $$
CREATE PROCEDURE `rebuild_etl_data_v1_1`()
BEGIN
			set @fake_visit_id = 10000000;			
			SET GLOBAL group_concat_max_len=500000;

			set @boundary = "!!";
			set @sep = " ## ";


			select count(*) into @person_ids_count from rebuild_etl_data_queue;

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
#								when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
								when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
								when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
								when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
							end
							order by concept_id,value_coded
							separator ' ## '
						) as obs,

						group_concat(
							case
								when value_coded is not null or value_numeric is not null or value_datetime is not null or value_text is not null or value_drug is not null or value_modifier is not null
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
#								when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
								when value_text is not null then concat(@boundary,o.concept_id,'=',value_text,@boundary)
								when value_drug is not null then concat(@boundary,o.concept_id,'=',value_drug,@boundary)
								when value_modifier is not null then concat(@boundary,o.concept_id,'=',value_modifier,@boundary)
							end
							order by concept_id,value_coded
							separator ' ## '
						) as obs,

						group_concat(
							case
								when value_coded is not null or value_numeric is not null or value_datetime is not null or value_text is not null or value_drug is not null or value_modifier is not null
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
#								when value_boolean is not null then concat(@boundary,o.concept_id,'=',value_boolean,@boundary)
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
					
								and concept_id in (856,5497,730,21,653,790,12,1030,1040,1271,9239,9020,9508, 6126, 887, 6252, 1537, 857,
                                   679,21,851,1018,1017,1016,729,678,1330,6134,790,1132,1133,1134,655,1297,6123,
                                   653,654,717,848,785,1014,10249,10250,10251,9010,9011,9699,9012,9812)							
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

					replace into flat_hiv_summary_sync_queue (select * from rebuild_etl_data_queue_0);

					delete t1
						from hiv_monthly_report_dataset_v1_2 t1
						join rebuild_etl_data_queue_0 t2 using (person_id);

					replace into hiv_monthly_report_dataset_sync_queue (select * from rebuild_etl_data_queue_0);					

					
					delete t1 
						from flat_labs_and_imaging t1
						join rebuild_etl_data_queue_0 using (person_id);
                        
					replace into flat_labs_and_imaging_sync_queue (select * from rebuild_etl_data_queue_0);


					
					delete t1
						from flat_vitals t1 
						join rebuild_etl_data_queue_0 t2 using (person_id);
                        
					replace into flat_vitals_sync_queue (select * from rebuild_etl_data_queue_0);


					delete t1
						from flat_pep_summary t1
						join rebuild_etl_data_queue_0 t2 using(person_id);



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
							join rebuild_etl_data_queue_0 t0 using (person_id)
						where t1.encounter_type in (56,57)
					);

					drop table if exists flat_pep_summary_0;
					create temporary table flat_pep_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
					(select * from flat_pep_summary_0a
					order by person_id, date(encounter_datetime)
					);


					SET @prev_id=NULL;
					SET @cur_id=NULL;
					SET @enrollment_date=NULL;
					SET @cur_location=NULL;
					SET @cur_rtc_date=NULL;
					SET @prev_rtc_date=NULL;

					SET @hiv_exposed_occupational=NULL;
					SET @pep_start_date=NULL;

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
							from flat_cdm t1 
							join rebuild_etl_data_queue_0 t2 using (person_id);
							
						replace into flat_cdm_sync_queue (select * from rebuild_etl_data_queue_0);



						delete t1
							from flat_cervical_cancer_screening t1 
							join rebuild_etl_data_queue_0 t2 using (person_id);
							
						replace into flat_cervical_cancer_screening_sync_queue (select * from rebuild_etl_data_queue_0);
            

						delete t1
							from flat_breast_cancer_screening t1 
							join rebuild_etl_data_queue_0 t2 using (person_id);
							
						replace into flat_breast_cancer_screening_sync_queue (select * from rebuild_etl_data_queue_0);


						
						delete t1 
							from rebuild_etl_data_queue t1 
							join rebuild_etl_data_queue_0 t2 using (person_id);
							
							select count(*) into @person_ids_count from rebuild_etl_data_queue;
                            
			 end while;
                    
		END$$
DELIMITER ;
