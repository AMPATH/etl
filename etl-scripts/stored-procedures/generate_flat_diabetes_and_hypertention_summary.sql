DELIMITER $$
CREATE  PROCEDURE `generate_flat_diabetes_and_hypertention_summary`(IN query_type varchar(50), IN queue_number int,
                                                               IN queue_size int, IN cycle_size int)
BEGIN
                    set @primary_table := "flat_diabetes_and_hypertention_summary";
                    set @query_type = query_type;
                    set @queue_table = "";
                    set @total_rows_written = 0;
                    set @sep = " ## ";
                    
                    set @start = now();
                    set @table_version = "flat_diabetes_and_hypertention_summary_v1.0";
                    set @last_date_created = (select max(max_date_created) from etl.flat_obs);
drop table if exists  flat_diabetes_and_hypertention_summary;
CREATE TABLE IF NOT EXISTS flat_diabetes_and_hypertention_summary (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    visit_id INT,
    location_id INT,
    encounter_id INT,
    encounter_type INT,
    is_clinical_encounter SMALLINT,
    encounter_datetime DATETIME,
    next_encounter_datetime DATETIME,
    next_clinical_encounter_datetime DATETIME,
    ncd_visit_type SMALLINT,
    has_diabetes SMALLINT,
    diabetes_diagnosis_date DATE,
    diabetes_status INT,
    diabetes_mellitus_type INT,
    diagnosis_date DATE,
    has_htn INT,
    htn_diagnosis_date DATE,
    htn_type INT,
    htn_stage INT,
    is_co_morbid SMALLINT,
    co_morbid_diagnosis_date DATE,
    co_morbid_type INT,
    stroke_diagnosis SMALLINT,
    ischemic_heart_disease_diagnosis SMALLINT,
    heart_failure_diagnosis SMALLINT,
    has_neuropathies SMALLINT,
    screened_for_diabetic_foot SMALLINT,
    diabetic_foot_screening_date DATETIME,
    has_diabetic_foot SMALLINT,
    amputation_due_to_diabetic_foot SMALLINT,
    screened_for_tb SMALLINT,
    tb_screening_date DATETIME,
    tb_screening_status INT,
    covered_by_shif SMALLINT,
    on_insulin SMALLINT,
    insulin_type INT,
    insulin_start_date DATETIME,
    insulin_end_date DATETIME,
    on_ogla_meds SMALLINT,
    hba1c decimal,
    hba1c_date DATE,
    on_exercise SMALLINT,
    on_diet SMALLINT,
    on_antihypertensives SMALLINT,
    systolic INT,
    diastolic INT,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created),
    INDEX location_id (location_id)
);
                    
                    if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_diabetes_and_hypertention_summary_temp_",queue_number);
                            set @queue_table = concat("flat_diabetes_and_hypertention_summary_build_queue_",queue_number);  

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_diabetes_and_hypertention_summary_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            
                            SET @dyn_sql=CONCAT('delete t1 from flat_diabetes_and_hypertention_summary_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                    end if;
                    
					SET @person_ids_count = 0;
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
                    
					SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
                    
                     set @total_time=0;
                    set @cycle_number = 0;
                    

                    while @person_ids_count > 0 do

                        set @loop_start_time = now();
                        
                        
						drop temporary table if exists flat_diabetes_and_hypertention_summary_build_queue__0;
                        

                        
                        SET @dyn_sql=CONCAT('create temporary table flat_diabetes_and_hypertention_summary_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;  
                        
						drop table if exists flat_diabetes_and_hypertention_summary_obs;
                        
CREATE  TABLE IF NOT EXISTS flat_diabetes_and_hypertention_summary_obs (
    person_id int,
    visit_id int,
    encounter_id int,
    encounter_datetime datetime,
    encounter_type int,
    location_id int,
    obs varchar(700),
    obs_datetimes datetime,
    orders int,
    lab_obs varchar(300),
    test_datetime datetime,
    index person_id(person_id)
);
                        
insert into flat_diabetes_and_hypertention_summary_obs (SELECT t1.person_id,
    t1.visit_id,
    t1.encounter_id,
    t1.encounter_datetime,
    t1.encounter_type,
    t1.location_id,
    t1.obs,
    t1.obs_datetimes,
    null,
    null,
    null
    FROM
    etl.flat_obs t1
        JOIN
    flat_diabetes_and_hypertention_summary_build_queue__0 t0 ON (t1.person_id = t0.person_id)
WHERE
    t1.encounter_type IN (324 , 325, 326));
    
    SELECT CONCAT('Adding labs information');
    


insert into flat_diabetes_and_hypertention_summary_obs
                        (select
                            t1.person_id,
                            null as visit_id,
                            t1.encounter_id,
                            t1.test_datetime as encounter_datetime,
                            t1.encounter_type,
                            null,
                            null,
                            null,
                            null,
                            t1.obs as lab_obs,
                            t1.test_datetime
                            from etl.flat_lab_obs t1
							join flat_diabetes_and_hypertention_summary_build_queue__0 t0 using (person_id)
                            where t1.obs REGEXP '!!6126='
);


    
SELECT CONCAT('Adding drug order information');

insert into flat_diabetes_and_hypertention_summary_obs
                        (select
                            t1.person_id,
                            null,
                            t1.encounter_id,
                            t1.encounter_datetime,
                            t1.encounter_type,
                            e.location_id,
                            null,
                            null,
                            t1.orders as orders,
                            null,
                            null
                            from etl.flat_orders t1
                            join amrs.encounter e on (e.encounter_id = t1.encounter_id)
                                join flat_diabetes_and_hypertention_summary_build_queue__0 t0 using (person_id)
);





                        
drop temporary table if exists flat_diabetes_and_hypertention_summary_1;
CREATE temporary TABLE flat_diabetes_and_hypertention_summary_1 (
    INDEX encounter_id (encounter_id),
    INDEX person_id (person_id)
) (SELECT t1.person_id,
    t1.visit_id,
    t1.location_id,
    t1.encounter_id,
    t1.encounter_type,
    CASE
     WHEN t1.encounter_type in (324,325,326) then 1
     ELSE NULL
    END AS is_clinical_encounter,
    t1.encounter_datetime,
    CASE
        WHEN t1.obs REGEXP '!!1839=' THEN etl.GetValues(t1.obs, 1839)
        ELSE NULL
    END AS ncd_visit_type,
    CASE
        WHEN t1.obs REGEXP '!!11098=' THEN etl.GetValues(t1.obs, 11098)
        WHEN t1.obs REGEXP '!!9728=' THEN etl.GetValues(t1.obs,9728)
        ELSE NULL
    END AS diagnosis_date,
    CASE
        WHEN stroke.diagnosis_coded IS NOT NULL THEN 1
        ELSE 0
    END AS stroke_diagnosis,
    CASE
        WHEN ischemic.diagnosis_coded IS NOT NULL THEN 1
        ELSE 0
    END AS ischemic_heart_disease_diagnosis,
    CASE
        WHEN hf.diagnosis_coded IS NOT NULL THEN 1
        ELSE 0
    END AS heart_failure_diagnosis,
    case
		 when neuropathies.diagnosis_coded IS NOT NULL THEN 1
		 else 0
    end as has_neuropathies,
    case
	  WHEN t1.obs REGEXP '!!12781=' THEN 1
	  ELSE NULL
    end as screened_for_diabetic_foot,
    case
     WHEN t1.obs REGEXP '!!12786=1065!!' THEN 1
     WHEN t1.obs REGEXP '!!12786=1066!!' THEN 0
	 ELSE NULL
    end as has_diabetic_foot,
    case
     WHEN t1.obs REGEXP '!!12721=7161!!' THEN 1
	 ELSE NULL
    end as amputation_due_to_diabetic_foot,
    t1.obs,
    t1.orders,
    t1.lab_obs,
    t1.test_datetime
    FROM
    flat_diabetes_and_hypertention_summary_obs t1
        LEFT JOIN
    amrs.encounter_diagnosis stroke ON (t1.encounter_id = stroke.encounter_id
        AND stroke.diagnosis_coded = 1878
        AND stroke.voided = 0)
        LEFT JOIN
    amrs.encounter_diagnosis ischemic ON (t1.encounter_id = ischemic.encounter_id
        AND ischemic.diagnosis_coded = 8077
        AND ischemic.voided = 0)
        LEFT JOIN
    amrs.encounter_diagnosis hf ON (t1.encounter_id = hf.encounter_id
        AND hf.diagnosis_coded = 13197
        AND hf.voided = 0)
        LEFT JOIN
    amrs.encounter_diagnosis neuropathies ON (t1.encounter_id = neuropathies.encounter_id
        AND neuropathies.diagnosis_coded in (6636)
        AND neuropathies.voided = 0));
        
SELECT CONCAT('Creating flat_diabetes_and_hypertention_summary_next_0');

	 set @prev_id = -1;
	 set @cur_id = -1;
	 set @prev_encounter_date = null;
	 set @cur_encounter_date = null;

	drop  table if exists etl.flat_diabetes_and_hypertention_summary_next_0;
	CREATE TABLE etl.flat_diabetes_and_hypertention_summary_next_0 (
    SELECT @prev_id:=@cur_id AS prev_id,
    @cur_id:=t.person_id AS cur_id,
    CASE
        WHEN @prev_id = @cur_id THEN @prev_encounter_date:=@cur_encounter_date
        ELSE @prev_encounter_date:=NULL
    END AS next_clinical_encounter_datetime,
    @cur_encounter_date:=t.encounter_datetime AS cur_encounter_datetime,
    t.* FROM
    flat_diabetes_and_hypertention_summary_1 t
    where t.encounter_type in (324,325,326)
ORDER BY t.encounter_datetime DESC);
    
    alter table etl.flat_diabetes_and_hypertention_summary_next_0 drop prev_id, drop cur_id;
    
    
     set @prev_id = -1;
	 set @cur_id = -1;
	 set @prev_encounter_date = null;
	 set @cur_encounter_date = null;
    
    SELECT CONCAT('Creating flat_diabetes_and_hypertention_summary_next : Add next clinical encounter datetime values');
    
    drop  table if exists etl.flat_diabetes_and_hypertention_summary_next;
	CREATE TABLE etl.flat_diabetes_and_hypertention_summary_next (
    SELECT @prev_id:=@cur_id AS prev_id,
    @cur_id:=t.person_id AS cur_id,
    CASE
        WHEN @prev_id = @cur_id THEN @prev_encounter_date:=@cur_encounter_date
        ELSE @prev_encounter_date:=NULL
    END AS next_encounter_datetime,
    @cur_encounter_date:=t.encounter_datetime AS cur_encounter_datetime,
    t.* ,
    n.next_clinical_encounter_datetime
    FROM
    flat_diabetes_and_hypertention_summary_1 t
    left join etl.flat_diabetes_and_hypertention_summary_next_0 n using (encounter_id)
ORDER BY t.encounter_datetime DESC);

 alter table etl.flat_diabetes_and_hypertention_summary_next drop prev_id, drop cur_id;
    
    # Lag values
    
     set @prev_id = -1;
	 set @cur_id = -1;
     set @has_diabetes := null;
     set @diabetes_status := null;
     set @has_htn := null;
     set @htn_diagnosis_date:=null;
     set @htn_type := null;
     set @htn_stage := null;
     set @diabetes_mellitus_type:=null;
     set @covered_by_shif:=null;
     set @screened_for_tb:=null;
     set @tb_screening_status := null;
     set @tb_screening_date := null;
     set @diabetic_foot_screening_date := null;
     set @on_insulin:=null;
     SET @insulin_type:= null;
     set @insulin_start_date:=null;
     set @on_ogla_meds:=null;
     set @hba1c:=null;
     set @hba1c_date:=null;
     set @current_location_id:=null;
     set @on_exercise:=null;
     set @on_diet:=null;
     set @is_co_morbid:=null;
     set @co_morbid_diagnosis_date:=null;
     set @co_morbid_type:=null;
     set @diabetes_diagnosis_date:=null;
    
    drop  table if exists etl.flat_diabetes_and_hypertention_summary_lag;
	CREATE TABLE etl.flat_diabetes_and_hypertention_summary_lag (SELECT @prev_id:=@cur_id AS prev_id,
    @cur_id:=t.person_id AS cur_id,
    CASE
        WHEN t.obs REGEXP '!!11679=175!!' THEN @has_diabetes:=1
        WHEN @prev_id = @cur_id THEN @has_diabetes
        ELSE @has_diabetes:=NULL
    END AS has_diabetes,
    CASE
        WHEN t.obs REGEXP '!!11679=175!!' AND t.obs REGEXP '!!11098=!!' THEN @diabetes_diagnosis_date:=etl.GetValues(t.obs, 11098)
        WHEN t.obs REGEXP '!!11679=175!!' AND t.obs NOT REGEXP '!!11098=!!'  AND t.obs REGEXP '!!7287=7281!!' THEN @diabetes_diagnosis_date:=t.encounter_datetime
        WHEN t.obs REGEXP '!!11679=175!!' AND t.obs NOT REGEXP '!!11098=!!'  AND t.obs REGEXP '!!7287=7282!!' THEN @diabetes_diagnosis_date:='1900-01-01'
        WHEN @prev_id = @cur_id THEN @diabetes_diagnosis_date
        ELSE @diabetes_diagnosis_date:=NULL
    END AS diabetes_diagnosis_date,
    CASE
        WHEN t.obs REGEXP '!!7287=' THEN @diabetes_status:=etl.GetValues(t.obs, 7287)
        WHEN @prev_id = @cur_id THEN @diabetes_status
        ELSE @diabetes_status:=NULL
    END AS diabetes_status,
    CASE
        WHEN
            t.obs REGEXP '!!9324='
        THEN
            @diabetes_mellitus_type:=REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(t.obs,
                                LOCATE('!!9324=', t.obs)),
                            @sep,
                            1)),
                    '!!9324=',
                    ''),
                '!!',
                '')
        WHEN @prev_id = @cur_id THEN @diabetes_mellitus_type
        ELSE @diabetes_mellitus_type:=NULL
    END AS diabetes_mellitus_type,
    CASE
        WHEN t.obs REGEXP '!!11679=903!!' THEN @has_htn:=1
        WHEN @prev_id = @cur_id THEN @has_htn
        ELSE @has_htn:=NULL
    END AS has_htn,
    CASE
        WHEN t.obs REGEXP '!!11679=903!!' AND t.obs REGEXP '!!11098=' THEN @htn_diagnosis_date:=etl.GetValues(t.obs, 11098)
        WHEN t.obs REGEXP '!!11679=903!!' AND t.obs REGEXP '!!9728=' THEN @htn_diagnosis_date:=etl.GetValues(t.obs,9728)
		WHEN t.obs REGEXP '!!11679=903!!' AND t.obs REGEXP '!!7288=7285!!' and t.obs NOT REGEXP '!!9728=' AND @htn_diagnosis_date is null THEN @htn_diagnosis_date:=t.encounter_datetime
        WHEN t.obs REGEXP '!!11679=903!!' AND t.obs REGEXP '!!7288=7286!!' and t.obs NOT REGEXP '!!9728=' AND @htn_diagnosis_date is null THEN @htn_diagnosis_date:='1900-01-01'
        WHEN @prev_id = @cur_id THEN @htn_diagnosis_date
        ELSE @htn_diagnosis_date:=NULL
    END AS htn_diagnosis_date,
    CASE
        WHEN t.obs REGEXP '!!7288=' THEN @htn_type:=etl.GetValues(t.obs, 7288)
        WHEN @prev_id = @cur_id THEN @htn_type
        ELSE @htn_type:=NULL
    END AS htn_type,
    CASE
        WHEN t.obs REGEXP '!!12717=' THEN @htn_stage:=etl.GetValues(t.obs, 12717)
        WHEN @prev_id = @cur_id THEN @htn_stage
        ELSE @htn_stage:=NULL
    END AS htn_stage,
    CASE
        WHEN t.obs REGEXP '!!12487=1065!!' THEN @covered_by_shif:=1
        WHEN t.obs REGEXP '!!12487=1066!!' THEN @covered_by_shif:=0
        WHEN @prev_id = @cur_id THEN @covered_by_shif
        ELSE @covered_by_shif:=NULL
    END AS covered_by_shif,
    CASE
        WHEN t.obs REGEXP '!!2359=1065!!' THEN 1
        WHEN t.obs REGEXP '!!2359=1066!!' THEN 0
        ELSE NULL
    END AS screened_for_tb,
    CASE
        WHEN t.obs REGEXP '!!8292=' THEN @tb_screening_status:=etl.GetValues(t.obs, 8292)
        WHEN @prev_id = @cur_id THEN @tb_screening_status
        ELSE @tb_screening_status:=NULL
    END AS tb_screening_status,
    CASE
        WHEN t.obs REGEXP '!!2359=1065!!' THEN @tb_screening_date:=t.encounter_datetime
        WHEN @prev_id = @cur_id THEN @tb_screening_date
        ELSE @tb_screening_date:=NULL
    END AS tb_screening_date,
    CASE
        WHEN t.obs REGEXP '!!12781=' THEN @diabetic_foot_screening_date:=t.encounter_datetime
        WHEN @prev_id = @cur_id THEN @diabetic_foot_screening_date
        ELSE @diabetic_foot_screening_date:=NULL
    END AS diabetic_foot_screening_date,
    CASE
      WHEN t.orders REGEXP '(282|9428|2254|2256)' then @on_insulin:= 1
	  WHEN @prev_id = @cur_id THEN @on_insulin
      ELSE @on_insulin:=null
    END AS on_insulin,
    CASE
      WHEN t.orders in (9428,2254) THEN @insulin_type:= t.orders
      WHEN @prev_id = @cur_id THEN @insulin_type
      else @insulin_type:=null
    END AS insulin_type,
    NULL AS insulin_start_date,
    NULL AS insulin_end_date,
    CASE
      WHEN t.orders REGEXP '(2267|2266|254|2268|15168|11349|251|2280|929|9718|2269)' THEN @on_ogla_meds:=1
	  WHEN @prev_id = @cur_id THEN @on_ogla_meds
      ELSE @on_ogla_meds:=null
    END AS on_ogla_meds,
    CASE
     WHEN t.lab_obs REGEXP '!!6126=' THEN @hba1c:= cast(replace(replace((substring_index(substring(t.lab_obs,locate("6126=",t.lab_obs)),@sep,1)),"6126=",""),"!!","") as decimal(4,1))
     WHEN @prev_id = @cur_id THEN @hba1c
     ELSE @hba1c:=null
    END AS hba1c,
    CASE
     WHEN t.lab_obs REGEXP '!!6126=' THEN @hba1c_date:=t.test_datetime
     WHEN @prev_id = @cur_id THEN @hba1c_date
     ELSE @hba1c_date:=null
    END AS hba1c_date,
    CASE
     WHEN t.obs REGEXP '!!10399=1065!!' THEN @on_exercise:=1
     WHEN t.obs REGEXP '!!10399=1066!!' THEN @on_exercise:=0
     WHEN @prev_id = @cur_id THEN @on_exercise
     ELSE @on_exercise:=null
    END as on_exercise,
    CASE
     WHEN t.obs REGEXP '!!10636=1065!!' THEN @on_diet:=1
     WHEN t.obs REGEXP '!!10636=1066!!' THEN @on_diet:=0
     WHEN @prev_id = @cur_id THEN @on_diet
     ELSE @on_diet:=null
    END as on_diet,
    0 as on_antihypertensives,
    0 as systolic,
    0 as  diastolic,
    CASE
        WHEN t.obs REGEXP '!!11679=10239!!' THEN @is_co_morbid:=1
        WHEN @prev_id = @cur_id THEN @is_co_morbid
        ELSE @is_co_morbid:=null
    END AS is_co_morbid,
    CASE
        WHEN t.obs REGEXP '!!10239=' THEN @co_morbid_type:=etl.GetValues(t.obs, 10239)
        WHEN @prev_id = @cur_id THEN @co_morbid_type
        ELSE @co_morbid_type:=NULL
    END AS co_morbid_type,
    CASE
        WHEN t.obs REGEXP '!!11679=10239!!' AND t.obs REGEXP '!!10706=' THEN @co_morbid_diagnosis_date:=etl.GetValues(t.obs,10706)
        WHEN t.obs REGEXP '!!11679=10239!!' AND t.obs NOT REGEXP '!!10706=' AND t.obs REGEXP '!!10239=1154!!' THEN @co_morbid_diagnosis_date:=t.encounter_datetime
		WHEN t.obs REGEXP '!!11679=10239!!' AND t.obs NOT REGEXP '!!10706=' AND t.obs REGEXP '!!10239=12778!!' THEN @co_morbid_diagnosis_date:='1900-01-01'
        WHEN @prev_id = @cur_id THEN @co_morbid_diagnosis_date
        ELSE @co_morbid_diagnosis_date:=NULL
    END AS co_morbid_diagnosis_date,
    t.* FROM
    flat_diabetes_and_hypertention_summary_next t
ORDER BY t.encounter_datetime);
                        
SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_diabetes_and_hypertention_summary_lag;
											
						SELECT @new_encounter_rows;                    
						set @total_rows_written = @total_rows_written + @new_encounter_rows;
						SELECT @total_rows_written;
                        
                        
                        SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
                        '(select 
                                null as date_created,
                                person_id,
								visit_id,
								location_id,
								encounter_id,
								encounter_type,
                                is_clinical_encounter,
								encounter_datetime,
                                next_encounter_datetime,
                                next_clinical_encounter_datetime,
                                ncd_visit_type,
                                has_diabetes,
                                diabetes_diagnosis_date,
                                diabetes_status,
                                diabetes_mellitus_type,
                                diagnosis_date,
                                has_htn,
                                htn_diagnosis_date,
                                htn_type,
                                htn_stage,
                                is_co_morbid,
                                co_morbid_diagnosis_date,
                                co_morbid_type,
                                stroke_diagnosis,
								ischemic_heart_disease_diagnosis,
								heart_failure_diagnosis,
                                has_neuropathies,
                                screened_for_diabetic_foot,
                                diabetic_foot_screening_date,
                                has_diabetic_foot,
                                amputation_due_to_diabetic_foot,
                                screened_for_tb,
                                tb_screening_date,
                                tb_screening_status,
                                covered_by_shif,
								on_insulin,
								insulin_type,
								insulin_start_date,
								insulin_end_date,
                                on_ogla_meds,
                                hba1c,
                                hba1c_date,
                                on_exercise,
                                on_diet,
								on_antihypertensives,
								systolic,
								diastolic
                        from flat_diabetes_and_hypertention_summary_lag);');

						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
                        
                        
					SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_diabetes_and_hypertention_summary_build_queue__0 t2 using (person_id);'); 

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    
                    
                    SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table,';'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    
                    

                    set @cycle_length = timestampdiff(second,@loop_start_time,now());
                    
                    set @total_time = @total_time + @cycle_length;
                    set @cycle_number = @cycle_number + 1;
                    
                    
                    set @remaining_time = ceil((@total_time / @cycle_number) * ceil(@person_ids_count / cycle_size) / 60);
                    

					SELECT 
    @person_ids_count AS 'persons remaining',
    @cycle_length AS 'Cycle time (s)',
    CEIL(@person_ids_count / cycle_size) AS remaining_cycles,
    @remaining_time AS 'Est time remaining (min)';
                        
				   end while;
                   
                                if(@query_type="build") then
                        SET @dyn_sql=CONCAT('drop table ',@queue_table,';'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;  
                        
                        SET @total_rows_to_write=0;
                        SET @dyn_sql=CONCAT("Select count(*) into @total_rows_to_write from ",@write_table);
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                                                
                        set @start_write = now();
SELECT 
    CONCAT(@start_write,
            ' : Writing ',
            @total_rows_to_write,
            ' to ',
            @primary_table);

                        SET @dyn_sql=CONCAT('replace into ', @primary_table,
                            '(select
								null as date_created,
                                person_id,
								visit_id,
								location_id,
								encounter_id,
								encounter_type,
                                is_clinical_encounter,
								encounter_datetime,
                                next_encounter_datetime,
                                next_clinical_encounter_datetime,
                                ncd_visit_type,
                                has_diabetes,
                                diabetes_diagnosis_date,
                                diabetes_status,
                                diabetes_mellitus_type,
                                diagnosis_date,
                                has_htn,
                                htn_diagnosis_date,
                                htn_type,
                                htn_stage,
                                is_co_morbid,
                                co_morbid_diagnosis_date,
                                co_morbid_type,
                                stroke_diagnosis,
								ischemic_heart_disease_diagnosis,
								heart_failure_diagnosis,
                                has_neuropathies,
                                screened_for_diabetic_foot,
                                diabetic_foot_screening_date,
                                has_diabetic_foot,
                                amputation_due_to_diabetic_foot,
                                screened_for_tb,
                                tb_screening_date,
                                tb_screening_status,
                                covered_by_shif,
                                on_insulin,
								insulin_type,
								insulin_start_date,
								insulin_end_date,
                                on_ogla_meds,
                                hba1c,
                                hba1c_date,
                                on_exercise,
                                on_diet,
                                on_antihypertensives,
								systolic,
								diastolic
                            from ',@write_table,');');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        set @finish_write = now();
                        set @time_to_write = timestampdiff(second,@start_write,@finish_write);
SELECT 
    CONCAT(@finish_write,
            ' : Completed writing rows. Time to write to primary table: ',
            @time_to_write,
            ' seconds ');                        
                        
                        SET @dyn_sql=CONCAT('drop table ',@write_table,';'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;  
                        
                        
                end if;
                
                                    
                set @ave_cycle_length = ceil(@total_time/@cycle_number);
				SELECT 
    CONCAT('Average Cycle Length: ',
            @ave_cycle_length,
            ' second(s)');
								
								 set @end = now();
							
								 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
								
				SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

END$$
DELIMITER ;
