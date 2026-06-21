DELIMITER $$
CREATE PROCEDURE `generate_flat_otz_summary`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
					set @primary_table := "flat_otz_summary";
                    set @total_rows_written = 0;
                    set @query_type = query_type;
                    
                    set @start = now();
                    set @table_version := "flat_otz_summary_v1.0";

                    set session sort_buffer_size=512000000;

                    -- set @last_date_created := (select max(max_date_created) from etl.flat_obs);
                    
SELECT 'Initializing variables successfull ...';

CREATE TABLE IF NOT EXISTS flat_otz_summary (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    gender TEXT,
    birth_date DATE,
    encounter_id INT,
    encounter_datetime DATETIME,
    next_encounter_datetime DATETIME,
    prev_encounter_datetime DATETIME,
    encounter_type INT,
    date_enrolled_to_otz DATE,
    age_at_otz_enrollment INT, 
    previously_enrolled_to_otz tinyint,
    original_art_start_date DATE,
    original_art_regimen VARCHAR(500),
    vl_result_at_otz_enrollment int,
    vl_result_date_at_otz_enrollment DATE,
    art_regimen_at_otz_enrollment VARCHAR(500),
    art_regimen_start_date_at_otz_enrollment DATETIME,
    art_regimen_line_at_otz_enrollment int,
    first_regimen_switch VARCHAR(500),
    first_regimen_switch_date DATE,
    first_regimen_switch_reason VARCHAR(500),
    second_regimen_switch VARCHAR(500),
    second_regimen_switch_date DATE,
    second_regimen_switch_reason VARCHAR(500),
    third_regimen_switch VARCHAR(500),
    third_regimen_switch_date DATE,
    third_regimen_switch_reason VARCHAR(500),
    fourth_regimen_switch VARCHAR(500),
    fourth_regimen_switch_date DATE,
    fourth_regimen_switch_reason VARCHAR(500),
    vl_result_post_otz_enrollment int,
    vl__result_date_post_otz_enrollment DATE,
    otz_orientation tinyint,
    otz_treatment_literacy tinyint,
    otz_participation tinyint,
    otz_peer_mentorship tinyint,
    otz_leadership tinyint,
    otz_positive_health_dignity_prevention tinyint,
    otz_future_decison_making tinyint,
    otz_transition_adult_care tinyint,
    discontinue_otz_reason int,
    discontinue_otz_date DATETIME,
    clinical_remarks VARCHAR(500),
    is_clinical_encounter INT,
    location_id INT,
    enrollment_location_id INT,
    prev_arv_meds VARCHAR(500),
    cur_arv_meds VARCHAR(500),
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX location_id_rtc_date (location_id),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
);

SELECT 'created table successfully ...';

create table if not exists flat_otz_summary_build_queue(
person_id int not null
);

  if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_otz_summary_temp_",queue_number);
                            set @queue_table = concat("flat_otz_summary_build_queue_",queue_number);                                                                    

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_otz_summary_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            
                            SET @dyn_sql=CONCAT('delete t1 from flat_otz_summary_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                    end if;
                    
                    SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
                    
                    SELECT @person_ids_count AS 'num patients to sync';

                    SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
                    
                    while @person_ids_count > 0 do
                    select 'inside while loop';
                    drop  table if exists flat_otz_summary_build_queue__0;
					
					SET @dyn_sql=CONCAT('create table flat_otz_summary_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
				
                    
                    SELECT 'creating  flat_otz_summary_obs from flat_obs...';
                        drop temporary table if exists flat_otz_summary_obs;
                        create temporary table flat_otz_summary_obs
                        (select
                            t1.person_id,
                            t1.encounter_id,
                            t1.encounter_datetime,
                            t1.encounter_type,
                            t1.location_id,
                            l.name as `clinic`,
                            p.birthdate,
                            p.gender,
                            t1.obs,
                            t1.obs_datetimes
                            from etl.flat_obs t1
                                join flat_otz_summary_build_queue__0 t0 using (person_id)                                
                                join amrs.location l using (location_id)
                                join amrs.person p on p.person_id = t1.person_id
								where t1.encounter_type in (284,288,285,283)
								
                        );
                        
                        set @prev_id = -1;
                        set @cur_id = -1;
                        set @enrollment_location_id = null;
                        set @enrollment_date = null;
                        set @previously_enrolled_to_otz := null;
                        
                        SELECT 'creating  flat_otz_enrollment ...';
                        drop temporary table if exists flat_otz_enrollment;
                        create temporary table flat_otz_enrollment(
                        select o.*,
                        case 
                        when obs regexp "!!10793=1066" then @enrollment_location_id := o.location_id
                        when obs regexp "!!10793=1065" then @enrollment_location_id := null
                        else @enrollment_location_id
                        end as enrollment_location_id,
                        
                         case 
                        when obs regexp "!!10793=1066" then @enrollment_date := o.encounter_datetime
                        when obs regexp "!!10793=1065" and obs regexp "!!10747=" then @enrollment_date := etl.GetValues(o.obs, 10747)
                        else @enrollment_date
                        end as enrollment_date,
                        
                        case
                        when obs regexp "!!10793=1066" then @previously_enrolled_to_otz := 0
                        when obs regexp "!!10793=1065" then @previously_enrolled_to_otz := 1
                        else @previously_enrolled_to_otz
                        end as previously_enrolled_to_otz
                        
                        from flat_otz_summary_obs o 
                        );
                        
						set @prev_id = -1;
                        set @cur_id = -1;
                        set @prev_encounter_datetime = null;
                        set @cur_encounter_datetime = null;
                        set @otz_orientation := null;
                        set @otz_treatment_literacy := null;
                        set @otz_participation := null;
                        set @otz_peer_mentorship := null;
                        set @otz_leadership := null;
                        set @otz_health_dignity_prevention := null;
                        set @otz_decision_making_and_planning := null;
                        set @otz_transition_to_adult_care := null;
                        set @clinical_remarks := null;
                        set @discontinue_otz_reason := null;
                        set @discontinue_otz_date := null;
						
                        
                        SELECT 'creating  flat_otz_modules ...';
                        
                        drop temporary table if exists flat_otz_modules;
                        create temporary table flat_otz_modules (
                        select e.*,
                        @prev_id := @cur_id as prev_id,
                            @cur_id := e.person_id as cur_id,

                            case
                                when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                                else @prev_encounter_datetime := null
                            end as next_encounter_datetime,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,
                        case 
                        when obs regexp "!!11032=1066" then @otz_orientation := 0
                        when obs regexp "!!11032=1065" then @otz_orientation := 1
                        else @otz_orientation
                        end as otz_orientation,
                        
                        case
                        when obs regexp "!!11037=1066" then @otz_treatment_literacy := 0
                        when obs regexp "!!11037=1065" then @otz_treatment_literacy := 1
                        else @otz_treatment_literacy
                        end as otz_treatment_literacy,
                        
						case
                        when obs regexp "!!11033=1066" then @otz_participation := 0
                        when obs regexp "!!11033=1065" then @otz_participation := 1
                        else @otz_participation
                        end as otz_participation,
                        
                        case
                        when obs regexp "!!12300=1066" then @otz_peer_mentorship := 0
                        when obs regexp "!!12300=1065" then @otz_peer_mentorship := 1
                        else @otz_peer_mentorship
                        end as otz_peer_mentorship,
                        
                        case
                        when obs regexp "!!11034=1066" then @otz_leadership := 0
                        when obs regexp "!!11034=1065" then @otz_leadership := 1
                        else @otz_leadership
                        end as otz_leadership,
                        
                        case
                        when obs regexp "!!12272=1066" then @otz_health_dignity_prevention := 0
                        when obs regexp "!!12272=1065" then @otz_health_dignity_prevention := 1
                        else @otz_health_dignity_prevention
                        end as otz_health_dignity_prevention,
                        
                        case
                        when obs regexp "!!11035=1066" then @otz_decision_making_and_planning := 0
                        when obs regexp "!!11035=1065" then @otz_decision_making_and_planning := 1
                        else @otz_decision_making_and_planning
                        end as otz_decision_making_and_planning,
                        
						case
                        when obs regexp "!!9302=1066" then @otz_transition_to_adult_care := 0
                        when obs regexp "!!9302=1065" then @otz_transition_to_adult_care := 1
                        else @otz_transition_to_adult_care
                        end as otz_transition_to_adult_care,
                        
                        case 
                        when obs regexp "!!9467=" then @clinical_remarks := etl.GetValues(e.obs, 9467)
                        else @clinical_remarks
                        end as clinical_remarks,
                        
                        case 
                        when obs regexp "!!1596=" then @discontinue_otz_reason := etl.GetValues(e.obs, 1596) 
						else @discontinue_otz_reason
						end as discontinue_otz_reason,
						                   
                        case 
                        when obs regexp "!!1596=" then @discontinue_otz_date := e.encounter_datetime
                        else @discontinue_otz_date
                        end as discontinue_otz_date,
                        
                        
                        now() as date_created
                        
                        from flat_otz_enrollment e order by e.person_id, e.encounter_datetime desc
                        );
                        
                        SELECT 'creating  flat_otz_original_regimen ...';
                        drop temporary table if exists flat_otz_original_regimen;
                        create temporary table  flat_otz_original_regimen (
                        SELECT 
								m.*,
								hs.arv_first_regimen AS original_art_regimen,
								hs.arv_first_regimen_start_date AS original_art_start_date
							FROM
								flat_otz_modules m
									LEFT JOIN
								etl.flat_hiv_summary_v15b hs ON (hs.person_id = m.person_id
									and hs.next_encounter_datetime_hiv is null )
                        );
                        
                        SELECT 'creating  flat_otz_status_at_enrollment ...';
                         drop temporary table if exists flat_otz_status_at_enrollment;
                        create temporary table  flat_otz_status_at_enrollment (
                        select o.*,
                           s.vl_1 as vl_result_at_otz_enrollment,
                           s.vl_1_date as vl_result_date_at_otz_enrollment,
                           s.cur_arv_meds as art_regimen_at_otz_enrollment,
						   s.arv_start_date AS art_regimen_start_date_at_otz_enrollment,
						   s.cur_arv_line as art_regimen_line_at_otz_enrollment
                        
                        from flat_otz_original_regimen o
                        left join
						etl.flat_hiv_summary_v15b s ON (o.person_id = s.person_id
						AND (s.next_encounter_datetime_hiv IS NULL || date(s.next_encounter_datetime_hiv) > date(o.enrollment_date))
						and date(s.encounter_datetime) <= date(o.enrollment_date) )
                        );
                        
                        set @first_switch := null;
						set @first_switch_date := null;
                        set @first_switch_reason := null;
						set @second_switch := null;
						set @second_switch_date := null;
                        set @second_switch_reason := null;
						set @third_switch := null;
						set @third_switch_date := null;
                        set @third_switch_reason := null;
						set @fourth_switch := null;
						set @fourth_switch_date := null;
                        set @fourth_switch_reason := null;
                        set @first__regimen_switch_reason := null;
                        set @second_regimen_switch_reason := null;
                        set @third_regimen_switch_reason := null;
                        set @fourth_regimen_switch_reason := null;
                        
                         SELECT 'creating  flat_otz_regimen_post_enrollment ...';
                         drop temporary table if exists flat_otz_regimen_post_enrollment;
                         create temporary table  flat_otz_regimen_post_enrollment (
                         select b.* from (select o.*,
                         CASE
								WHEN
									@fourth_switch IS NULL and @third_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@fourth_regimen_switch_reason := etl.GetValues(o.obs, 1252)
								ELSE @fourth_regimen_switch_reason
							END AS fourth_regimen_switch_reason,
                           CASE
								WHEN
									@fourth_switch IS NULL and @third_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@fourth_switch_date:=f.encounter_datetime
								ELSE @fourth_switch_date
							END AS fourth_switch_date,
							 CASE
								WHEN
									@fourth_switch IS NULL and @third_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@fourth_switch:=f.cur_arv_meds
								ELSE @fourth_switch
							END AS fourth_switch,
                            CASE
								WHEN
									@third_switch IS NULL and @second_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@third_regimen_switch_reason := etl.GetValues(o.obs, 1252)
								ELSE @third_regimen_switch_reason
							END AS third_regimen_switch_reason,
							CASE
								WHEN
									@third_switch IS NULL and @second_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@third_switch_date:=f.encounter_datetime
								ELSE @third_switch_date
							END AS third_switch_date,
							 CASE
								WHEN
									@third_switch IS NULL and @second_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@third_switch:=f.cur_arv_meds
								ELSE @third_switch
							END AS third_switch,
                            CASE
								WHEN
									@second_switch IS NULL and @first_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@second_regimen_switch_reason:= etl.GetValues(o.obs, 1252)
								ELSE @second_regimen_switch_reason
							END AS second_regimen_switch_reason,
							CASE
								WHEN
									@second_switch IS NULL and @first_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@second_switch_date:=f.encounter_datetime
								ELSE @second_switch_date
							END AS second_switch_date,
							
							CASE
								WHEN
									@second_switch IS NULL and @first_switch IS not NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@second_switch:=f.cur_arv_meds
								ELSE @second_switch
							END AS second_switch,
                            CASE
								WHEN
									@first_switch IS NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@first_regimen_switch_reason := etl.GetValues(o.obs, 1252)
								ELSE @first_regimen_switch_reason
							END AS first_regimen_switch_reason,
							case
							 WHEN
									@first_switch IS NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@first_switch_date:=f.encounter_datetime
								ELSE @first_switch_date
							end as first_switch_date,
							CASE
								WHEN
									@first_switch IS NULL
										AND f.cur_arv_meds <> f.prev_arv_meds
								THEN
									@first_switch:=f.cur_arv_meds
								ELSE @first_switch
							END AS first_switch
                         
                         from flat_otz_status_at_enrollment o
                          LEFT JOIN
							etl.flat_hiv_summary_v15b f ON (f.person_id = o.person_id
							AND f.cur_arv_meds <> f.prev_arv_meds
							AND o.enrollment_date  < f.encounter_datetime)
							order by o.person_id, f.encounter_id desc) b group by b.person_id, b.encounter_id
                         );
                         
                        
                        SELECT 'creating  flat_otz_summary ...';
                         SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
                        '(select
                        null as date_created,
						t1.person_id,
						t1.gender,
						t1.birthdate as birth_date ,
						t1.encounter_id,
						t1.encounter_datetime,
                        next_encounter_datetime,
						null as prev_encounter_datetime,
						t1.encounter_type,
						t1.enrollment_date as date_enrolled_to_otz,
						null as age_at_otz_enrollment, 
						t1.previously_enrolled_to_otz,
						t1.original_art_start_date,
						t1.original_art_regimen,
						t1.vl_result_at_otz_enrollment,
						t1.vl_result_date_at_otz_enrollment,
						t1.art_regimen_at_otz_enrollment,
                        t1.art_regimen_start_date_at_otz_enrollment,
						t1.art_regimen_line_at_otz_enrollment,
						t1.first_switch as first_regimen_switch,
						t1.first_switch_date as first_regimen_switch_date,
						t1.first_regimen_switch_reason,
						t1.second_switch as second_regimen_switch,
						t1.second_switch_date as second_regimen_switch_date,
						t1.second_regimen_switch_reason,
						t1.third_switch as third_regimen_switch,
						t1.third_switch_date as third_regimen_switch_date,
						t1.third_regimen_switch_reason,
						t1.fourth_switch as fourth_regimen_switch,
						t1.fourth_switch_date as fourth_regimen_switch_date,
						t1.fourth_regimen_switch_reason,
						null as vl_result_post_otz_enrollment,
						null as vl__result_date_post_otz_enrollment,
						t1.otz_orientation,
						t1.otz_treatment_literacy,
						t1.otz_participation,
						t1.otz_peer_mentorship,
						t1.otz_leadership,
						t1.otz_health_dignity_prevention as otz_positive_health_dignity_prevention,
						t1.otz_decision_making_and_planning as otz_future_decison_making,
						t1.otz_transition_to_adult_care as otz_transition_adult_care,
						t1.discontinue_otz_reason,
						t1.discontinue_otz_date,
						t1.clinical_remarks,
						null as is_clinical_encounter,
						t1.location_id,
						null as enrollment_location_id,
						null as prev_arv_meds,
						null as cur_arv_meds
                        
                        from flat_otz_regimen_post_enrollment t1
                        )');

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_otz_summary_build_queue__0 t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    end while;
                    
                    SET @dyn_sql=CONCAT('replace into ', @primary_table,
                            '(select * from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                    
                    SET @dyn_sql=CONCAT('drop table if exists ',@queue_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;
                    
                    SET @dyn_sql=CONCAT('drop table if exists ',@write_table,';'); 
					PREPARE s1 from @dyn_sql; 
					EXECUTE s1; 
					DEALLOCATE PREPARE s1;

            END$$
DELIMITER ;