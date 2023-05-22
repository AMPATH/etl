DELIMITER $$
CREATE PROCEDURE `generate_dc_monthly_report_dataset`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int , IN log boolean)
BEGIN
	set @primary_table := "dc_monthly_report_dataset";
	set @query_type = query_type;
	set @queue_table = "";
	set @total_rows_written = 0;
	
	set @start = now();
	set @table_version = "dc_monthly_report_dataset_v1";

	set @sep = " ## ";
	set @last_date_created = (select max(max_date_created) from etl.flat_obs);

	CREATE TABLE IF NOT EXISTS dc_monthly_report_dataset (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP NOT NULL,
    elastic_id BIGINT,
    end_date DATE,
    encounter_type INT,
    encounter_id INT,
    encounter_datetime DATE,
    person_id INT,
    person_uuid VARCHAR(100),
    birthdate DATE,
    age DOUBLE,
    gender VARCHAR(1),
    vl_1 VARCHAR(100),
    vl_1_date DATE,
    vl_2 VARCHAR(100),
    vl_2_date DATE,
    ipt_start_date DATE,
    ipt_completion_date DATE,
    rtc_date DATE,
    days_since_rtc_date INT,
    cur_status VARCHAR(100),
    cohort_name VARCHAR(100),
    location_uuid VARCHAR(100),
    location_id INT,
    tb_tx_start_date DATE,
    arv_first_regimen_start_date DATE,
    arv_first_regimen VARCHAR(250),
    arv_first_regimen_names VARCHAR(250),
    cur_arv_meds_names VARCHAR(250),
    cur_arv_line INT,
    program_id INT,
    program_date_completed DATE,
    weight VARCHAR(100),
    height VARCHAR(100),
    total_eligible_for_dc INT,
    eligible_not_on_dc INT,
    eligible_and_on_dc INT,
    enrolled_not_eligible INT,
    enrolled_in_dc INT,
    enrolled_in_dc_community INT,
    patients_due_for_vl INT,
    
    primary key elastic_id (elastic_id),
	index person_enc_date (person_id, encounter_datetime),
	index person_report_date (person_id, end_date),
	index endDate_location_id (end_date, location_id),
	index date_created (date_created),
	index location_id_index (location_id, end_date),
    index program_id_index (location_id, program_id, end_date)
);
    
      if (query_type = "build") then
        SELECT 'BUILDING.......................';
        set @write_table = concat("dc_monthly_report_dataset_temp",queue_number);
        set @queue_table = concat("dc_monthly_report_dataset_build_queue_",queue_number);   

        SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,'(person_id int primary key) (select * from dc_monthly_report_dataset_build_queue limit ',queue_size, ');'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;

        SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1; 
            
        SET @dyn_sql=CONCAT('delete t1 from dc_monthly_report_dataset_build_queue t1 join ',@queue_table, ' t2 using (person_id)'); 
        PREPARE s1 from @dyn_sql; 
        EXECUTE s1; 
        DEALLOCATE PREPARE s1;  
	end if;
    
    if (query_type = "sync") then
		set @write_table = concat("dc_monthly_report_dataset");
		set @queue_table = "dc_monthly_report_dataset_sync_queue";                                
		create table if not exists dc_monthly_report_dataset_sync_queue (person_id int primary key);
		
		select @last_update := (select max(date_updated) from etl.flat_log where table_name=@table_version);

		replace into dc_monthly_report_dataset_sync_queue
		(select distinct person_id from flat_hiv_summary_v15b where date_created >= @last_update);
	end if;
    
  # Remove test patients
    SET @dyn_sql=CONCAT('delete t1 FROM ',@queue_table,' t1
            join amrs.person_attribute t2 using (person_id)
            where t2.person_attribute_type_id=28 and value="true" and voided=0');
    PREPARE s1 from @dyn_sql; 
    EXECUTE s1; 
    DEALLOCATE PREPARE s1;
  
  SET @person_ids_count = 0;
  SET @dyn_sql=CONCAT('select count(*) into @person_ids_count from ',@queue_table); 
  PREPARE s1 from @dyn_sql; 
  EXECUTE s1; 
  DEALLOCATE PREPARE s1;

  SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;
  
SELECT CONCAT('Patients in queue: ', @person_ids_count);
                          
set @total_time=0;
set @cycle_number = 0;

            set @total_time=0;
			set @cycle_number = 0;
                    
			while @person_ids_count > 0 do
			
				set @loop_start_time = now();                        

				drop temporary table if exists dc_monthly_report_dataset_cycle_queue;
                create temporary table dc_monthly_report_dataset_cycle_queue (person_id int primary key);                

                SET @dyn_sql=CONCAT('insert into dc_monthly_report_dataset_cycle_queue (select * from ',@queue_table,' limit ',cycle_size,');'); 
				PREPARE s1 from @dyn_sql; 
				EXECUTE s1; 
				DEALLOCATE PREPARE s1;
								
								
				drop temporary table if exists cohort_members_stage_0;
				create temporary table cohort_members_stage_0 (
                SELECT t1.encounter_datetime,
					t2.endDate AS `c_end_date`,
					t4.cohort_type_id,
                    t4.name as `cohort_name`,
					t1.person_id AS `c_person_id` FROM
					etl.flat_hiv_summary_v15b t1
						JOIN etl.dates t2
						LEFT JOIN
					(SELECT patient_id AS `cm_patient_id`, cohort_id AS `cm_cohort_id`, voided FROM amrs.cohort_member) t3 ON (t3.cm_patient_id = t1.person_id AND t3.voided = 0)
						LEFT JOIN
					(SELECT cohort_id,cohort_type_id,name,date_created AS `cm_date_created`,voided FROM amrs.cohort) t4 ON (t3.cm_cohort_id = t4.cohort_id AND t4.voided = 0)
                    join etl.dc_monthly_report_dataset_cycle_queue t5 using (person_id)
				WHERE t1.encounter_datetime < date_add(t2.endDate, interval 1 day)
				AND (t1.next_clinical_datetime_hiv is null or t1.next_clinical_datetime_hiv >= date_add(t2.endDate, interval 1 day) )
				AND t1.is_clinical_encounter=1 
				AND t2.endDate BETWEEN '2019-01-01' AND DATE_ADD(now(), INTERVAL 2 YEAR)
				AND t4.cohort_type_id = 1);
                
                drop temporary table if exists dc_enrollment_stage_0;
				create temporary table dc_enrollment_stage_0
				( 
				 SELECT   t1.encounter_datetime, t2.endDate as `end_date`,
				 program_id, date_enrolled, date_completed, patient_id

				 FROM     etl.flat_hiv_summary_v15b t1 
				 JOIN     etl.dates t2 
				 LEFT JOIN ( select patient_id,program_id,date_completed as `program_date_completed`, date_enrolled, date_completed from amrs.patient_program) t4 ON ( t4.patient_id = t1.person_id AND t4.program_id IN ( 3, 9 ) AND t4.program_date_completed IS NULL)
                 join etl.dc_monthly_report_dataset_cycle_queue t5 using (person_id)
				 WHERE    t1.encounter_datetime < date_add(t2.endDate, interval 1 day)
				  AND (t1.next_clinical_datetime_hiv is null or t1.next_clinical_datetime_hiv >= date_add(t2.endDate, interval 1 day) )
				  AND t1.is_clinical_encounter=1 
				  AND   t2.endDate BETWEEN '2019-01-01' AND DATE_ADD(now(), INTERVAL 2 YEAR)
				 group BY person_id, t2.endDate);
                
                drop temporary table if exists latest_encounter;
				create temporary table latest_encounter
				( 
				 SELECT   *
				 FROM     etl.flat_hiv_summary_v15b t1 
				 JOIN     etl.dates t2 
				 join etl.dc_monthly_report_dataset_cycle_queue t5 using (person_id)
				 WHERE    t1.encounter_datetime < date_add(t2.endDate, interval 1 day)
				  AND (t1.next_encounter_datetime_hiv is null or t1.next_encounter_datetime_hiv >= date_add(t2.endDate, interval 1 day) )
				  AND   t2.endDate BETWEEN '2019-01-01' AND DATE_ADD(now(), INTERVAL 2 YEAR)
				 group BY person_id, t2.endDate);
				 
                
			SELECT CONCAT('creating dc_monthly_report_dataset_stage_0 ...');
            DROP temporary table if exists dc_monthly_report_dataset_stage_0;

            CREATE temporary TABLE dc_monthly_report_dataset_stage_0 
			( SELECT t1.*,
				concat(date_format(t2.endDate,"%Y%m"),t1.person_id) as elastic_id,
				t2.endDate as `end_date`,
                t3.person_uuid,
                t3.birthdate,
                t3.gender,
				t4.program_id,
                t4.date_completed as `program_date_completed`,
                t6.cohort_name,
				timestampdiff(day,rtc_date, t2.endDate) as days_since_rtc_date,
				get_arv_names(cur_arv_meds) as cur_arv_meds_names,
                get_arv_names(arv_first_regimen) as arv_first_regimen_names,
                  
                  CASE 
                           WHEN tb_tx_start_date IS NOT NULL THEN @months_since_tb_tx_start_date := timestampdiff(month,tb_tx_start_date,t2.endDate)
                           ELSE NULL 
                  END AS months_since_tb_tx_start_date, 
                  
                  CASE 
                           WHEN ipt_start_date IS NOT NULL THEN @months_since_ipt_start_date := timestampdiff(month,ipt_start_date,t2.endDate)
                           ELSE NULL 
                  END AS months_since_ipt_start_date, 
                  
                  CASE 
                           WHEN arv_first_regimen_start_date IS NOT NULL THEN @months_since_arv_first_regimen_start_date := timestampdiff(month,arv_first_regimen_start_date,t2.endDate)
                           ELSE NULL 
                  END AS months_since_arv_first_regimen_start_date,
                  
                  CASE 
                           WHEN arv_start_date IS NOT NULL THEN @months_since_arv_start_date := timestampdiff(month,arv_start_date,t2.endDate)
                           ELSE NULL 
                  END AS months_since_arv_start_date,
                  
                  CASE 
                           WHEN vl_1_date IS NOT NULL THEN @months_since_last_vl := timestampdiff(month,vl_1_date,t2.endDate)
                           ELSE NULL 
                  END AS months_since_last_vl,
                  
                  CASE 
                           WHEN weight IS NOT NULL 
                           AND      height IS NOT NULL THEN @bmi1 := (t1.weight / ( ( t1.height / 100 ) * ( t1.height / 100 ) ))
                           ELSE NULL 
                  END  AS bmi, 
                  
                  CASE 
                           WHEN timestampdiff(year,t3.birthdate,t2.endDate) > 0 THEN @age := round(timestampdiff(year,t3.birthdate,t2.endDate),0)
                           ELSE @age :=round(timestampdiff(month,t3.birthdate,t2.endDate)/12,2) 
                  END AS age, 
                  
                  case
						when (date_format(t2.endDate, "%Y-%m-01") > t1.death_date) or (date_format(t2.endDate, "%Y-%m-01") > t7.t7_death_date) then @status := "dead"
                        when (t1.transfer_out_date < date_format(t2.endDate,"%Y-%m-01")) or (t7.t7_transfer_out_date < date_format(t2.endDate,"%Y-%m-01")) or t7.latest_tranfer_out=1 then @status := "transfer_out"
						when (date_format(t2.endDate, "%Y-%m-01") > transfer_out_date) or (date_format(t2.endDate, "%Y-%m-01") > t7_transfer_out_date) or t7.latest_tranfer_out=1 then @status := "transfer_out"
						when timestampdiff(day,if(rtc_date,rtc_date,date_add(t1.encounter_datetime, interval 28 day)),t2.endDate) <= 28 then @status := "active"
						when timestampdiff(day,if(rtc_date,rtc_date,date_add(t1.encounter_datetime, interval 28 day)),t2.endDate) between 29 and 90 then @status := "defaulter"
						when timestampdiff(day,if(rtc_date,rtc_date,date_add(t1.encounter_datetime, interval 28 day)),t2.endDate) > 90 then @status := "ltfu"
						else @status := "unknown"
					end as status,
				  CASE 
					   WHEN arv_first_regimen_start_date IS NOT NULL AND arv_first_regimen_start_date < t2.endDate THEN 1
					   ELSE 0 
                  END  AS patient_is_on_art, 
                   CASE 
					   WHEN edd IS NOT NULL THEN @date_pregnant := DATE_ADD(edd, INTERVAL -9 MONTH)
					   ELSE NULL 
				  END AS date_pregnant,
				  CASE 
					   WHEN @date_pregnant IS NOT NULL AND DATE(@date_pregnant) > DATE(arv_first_regimen_start_date) THEN 1
					   ELSE 0 
				  END AS started_arv_b4_pregnancy,
				  CASE 
					   WHEN @date_pregnant IS NOT NULL AND DATE(@date_pregnant) < DATE(arv_first_regimen_start_date) THEN 1
					   ELSE 0 
				  END AS started_arv_after_pregnancy,
                   
				 CASE
                    WHEN vl_1 > 999
                                AND vl_1_date > arv_start_date
                                AND @months_since_last_vl >=3
					THEN 1  
					 #is 24 and below years
                    WHEN @age <=24 AND @patient_is_on_art=1 AND (@months_since_arv_first_regimen_start_date >=6 or @months_since_arv_start_date >=6) and vl_1_date is null  then 1
                    WHEN @age <=24 AND @patient_is_on_art=1 AND @months_since_last_vl >=6  then 1
                    
                    #is 25 and above years
                    WHEN @age >=25 AND @patient_is_on_art=1 AND (@months_since_arv_first_regimen_start_date >=6 or @months_since_arv_start_date >=6) and vl_1_date is null  then 1
                    WHEN @age >=25 AND @patient_is_on_art=1 AND @months_since_last_vl >=6 and (@months_since_arv_first_regimen_start_date <=12 or @months_since_arv_start_date <= 12 ) then 1
                    WHEN @age >=25 AND @patient_is_on_art=1 AND @months_since_last_vl >=12 and (@months_since_arv_first_regimen_start_date > 12 or @months_since_arv_start_date > 12 )then 1
                    
                    # drug substitution and no vl ever
                    WHEN arv_first_regimen_start_date is not null and arv_start_date is not null 
                    and arv_first_regimen_start_date < arv_start_date 
                    AND @months_since_arv_start_date >= 3 
                    AND vl_1_date is null then 1
                    
                    # drug substitution and vl was done before change
                    WHEN arv_first_regimen_start_date is not null and arv_start_date is not null 
                    and arv_first_regimen_start_date < arv_start_date 
                    AND @months_since_arv_start_date >= 3 
                    AND TIMESTAMPDIFF(MONTH,vl_1_date,arv_start_date)>=1  then 1
                    
					#is pregnant and started_arv_b4_pregnancy
					WHEN is_pregnant=1 AND @started_arv_b4_pregnancy=1 and vl_1_date is null then 1
					WHEN is_pregnant=1 AND @started_arv_b4_pregnancy=1 and TIMESTAMPDIFF(MONTH,vl_1_date,@date_pregnant)>=1 then 1
					WHEN is_pregnant=1 AND @started_arv_b4_pregnancy=1 and DATE(vl_1_date) >= DATE(@date_pregnant) and @months_since_last_vl >=6 then 1
					
					#is pregnant and started_arv_after_pregnancy
					WHEN is_pregnant=1 AND @patient_is_on_art=1 AND @started_arv_after_pregnancy=1 and @months_since_arv_first_regimen_start_date >=3 and vl_1_date is null then 1
					WHEN is_pregnant=1 AND @patient_is_on_art=1 AND @started_arv_after_pregnancy=1 and @months_since_arv_first_regimen_start_date >=3 and TIMESTAMPDIFF(MONTH,vl_1_date,@date_pregnant)>=1 then 1
					WHEN is_pregnant=1 AND @patient_is_on_art=1 AND @started_arv_after_pregnancy=1 and @months_since_arv_first_regimen_start_date >=3 and @months_since_last_vl >=6  then 1
                    WHEN is_pregnant=1 AND t1.vl_1 <= 200 AND @months_since_last_vl >= 6 then 1
					
					#breast feeding      
					WHEN is_mother_breastfeeding=1 AND @patient_is_on_art=1 and (@months_since_arv_first_regimen_start_date >=6 or @months_since_arv_start_date >=6) and vl_1_date is null then 1
					WHEN is_mother_breastfeeding=1 AND @patient_is_on_art=1 AND @months_since_last_vl >=6 then 1
					
				    else 0 
                  END AS patients_due_for_vl,
                    
                  CASE 
                           WHEN t1.vl_1 < 401 
                           AND      ( 
                                             @months_since_tb_tx_start_date >= 6 
                                    OR       @months_since_tb_tx_start_date IS NULL ) 
                           AND      @age >= 20 AND @status = 'active'
                           AND      @months_since_arv_first_regimen_start_date>= 12 
                           AND      date(t1.prev_clinical_rtc_date_hiv) = date(t1.encounter_datetime)
                           AND      t1.is_clinical_encounter = 1 
                           AND      @bmi1 >= 18.5 
                           AND  t1.is_pregnant is null
                           AND  t1.is_mother_breastfeeding is null
                           THEN @total_eligible_for_dc := 1 
                           ELSE @total_eligible_for_dc := 0 
                  END AS total_eligible_for_dc, 
                  
                  CASE 
                           WHEN t1.vl_1 < 401 
                           AND      ( 
                                             @months_since_tb_tx_start_date >= 6 
                                    OR       @months_since_tb_tx_start_date IS NULL ) 
                           AND      @age >= 20 AND @status = 'active'
                           AND      @months_since_arv_first_regimen_start_date>= 12 
                           AND      date(t1.prev_clinical_rtc_date_hiv) = date(t1.encounter_datetime)
                           AND      t1.is_clinical_encounter = 1 
                           AND      (t4.program_id  not in (3, 4, 9) or t4.program_id  is null)
                           AND      @bmi1 >= 18.5 
                           AND  t1.is_pregnant is null
                           AND  t1.is_mother_breastfeeding is null
                           THEN @eligible_not_on_dc := 1 
                           ELSE @eligible_not_on_dc := 0 
                  END AS eligible_not_on_dc,
                  
                  CASE 
                           WHEN t1.vl_1 < 401 
                           AND      ( 
                                             @months_since_tb_tx_start_date >= 6 
                                    OR       @months_since_tb_tx_start_date IS NULL ) 
                           AND      @age >= 20 AND @status = 'active'
                           AND      @months_since_arv_first_regimen_start_date>= 12 
                           AND      date(t1.prev_clinical_rtc_date_hiv) = date(t1.encounter_datetime)
                           AND      t1.is_clinical_encounter = 1 
                           AND      @bmi1 >= 18.5  
                           AND      (t4.program_id in (3, 9) and t4.program_id != 4)
                           AND  t1.is_pregnant is null
                           AND  t1.is_mother_breastfeeding is null
                           THEN @eligible_and_on_dc := 1
                           ELSE @eligible_and_on_dc := 0 
                  END AS eligible_and_on_dc,
                  
                  case when t4.program_id in (3,9) and @status = 'active' then @active_on_dc := 1
                  else @active_on_dc := 0 end as enrolled_in_dc,
                  IF(@total_eligible_for_dc = 0 and @active_on_dc = 1, 1, 0 ) as enrolled_not_eligible,
                  IF(t4.program_id in (3,9) and  t6.cohort_type_id = 1, 1, 0) as enrolled_in_dc_community

                  
         FROM     etl.flat_hiv_summary_v15b t1 
		 JOIN     etl.dates t2 
		 LEFT JOIN cohort_members_stage_0 t6 on (t1.person_id = t6.c_person_id and t2.endDate = t6.c_end_date)
         LEFT JOIN dc_enrollment_stage_0 t4 on (t1.person_id = t4.patient_id  and t2.endDate = t4.end_date)
         LEFT JOIN (select person_id as `t7_person_id`, encounter_datetime as `latest_enc_date`, transfer_out as `latest_tranfer_out`,death_date as `t7_death_date`, endDate,transfer_out_date as `t7_transfer_out_date`  from latest_encounter) t7 on (t1.person_id = t7.t7_person_id  and t2.endDate = t7.endDate)
		 LEFT JOIN (  SELECT person_id as `t3_person_id`, gender, birthdate, uuid AS `person_uuid`, voided FROM amrs.person) t3 on (t1.person_id = t3.t3_person_id and t3.voided = 0)  
         join etl.dc_monthly_report_dataset_cycle_queue t5 using (person_id)
         WHERE    t1.encounter_datetime < date_add(t2.endDate, interval 1 day)
         and (t1.next_clinical_datetime_hiv is null or t1.next_clinical_datetime_hiv >= date_add(t2.endDate, interval 1 day) )
		 and t1.is_clinical_encounter=1 
         AND  t2.endDate BETWEEN '2019-01-01' AND DATE_ADD(now(), INTERVAL 2 YEAR) 
         order BY person_id, t2.endDate);
         
		SET @dyn_sql=CONCAT('replace into dc_monthly_report_dataset'                                             
				'(select 
				null as date_created,  
				elastic_id,
				t1.end_date,
				encounter_type,
				t1.encounter_id,
				t1.encounter_datetime,
				t1.person_id,
				t1.person_uuid,
				t1.birthdate,
				t1.age,
				t1.gender,
				vl_1,
				vl_1_date,
				vl_2,
				vl_2_date,
				ipt_start_date,
				ipt_completion_date,
				t1.rtc_date,
				days_since_rtc_date,
                status as cur_status,
				cohort_name,
				location_uuid,
				t1.location_id,
				t1.tb_tx_start_date,
				t1.arv_first_regimen_start_date,
				arv_first_regimen,
				arv_first_regimen_names,
				cur_arv_meds_names,
				cur_arv_line,
				program_id,
				program_date_completed,
				t1.weight,
				t1.height,
				total_eligible_for_dc,
				eligible_not_on_dc,
				eligible_and_on_dc,
				enrolled_not_eligible,
				enrolled_in_dc,
				enrolled_in_dc_community,
                patients_due_for_vl
				
				from dc_monthly_report_dataset_stage_0 t1)');
        
						PREPARE s1 from @dyn_sql; 
						EXECUTE s1; 
						DEALLOCATE PREPARE s1;  
						
                        -- delete already built patients
						SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join dc_monthly_report_dataset_cycle_queue t2 using (person_id);'); 
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
                '(select * from ',@write_table,');');
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
		  if(log = true) then
		  insert into flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
		  end if;
		SELECT 
			CONCAT(@table_version,
					' : Time to complete: ',
					TIMESTAMPDIFF(MINUTE, @start, @end),
					' minutes');
   END$$
DELIMITER;