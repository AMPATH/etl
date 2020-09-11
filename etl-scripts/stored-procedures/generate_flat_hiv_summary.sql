DELIMITER $$
CREATE  PROCEDURE `generate_flat_hiv_summary`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
                    set @primary_table := "flat_hiv_summary_v15b";
                    set @query_type = query_type;
                    set @queue_table = "";
                    set @total_rows_written = 0;
                    
                    set @start = now();
                    set @table_version = "flat_hiv_summary_v2.24";

                    set session sort_buffer_size=512000000;

                    set @sep = " ## ";
                    set @lab_encounter_type = 99999;
                    set @death_encounter_type = 31;
                    set @last_date_created = (select max(max_date_created) from etl.flat_obs);

                    
                    
CREATE TABLE IF NOT EXISTS flat_hiv_summary_v15b (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    uuid VARCHAR(100),
    visit_id INT,
    visit_type SMALLINT,
    encounter_id INT,
    encounter_datetime DATETIME,
    encounter_type INT,
    is_transit INT,
    is_clinical_encounter INT,
    location_id INT,
    location_uuid VARCHAR(100),
    visit_num INT,
    mdt_session_number INT,
    enrollment_date DATETIME,
    enrollment_location_id INT,
    ovc_non_enrollment_reason INT,
    ovc_non_enrollment_date DATETIME,
    hiv_start_date DATETIME,
    death_date DATETIME,
    scheduled_visit INT,
    transfer_in TINYINT,
    transfer_in_location_id INT,
    transfer_in_date DATETIME,
    transfer_out TINYINT,
    transfer_out_location_id INT,
    transfer_out_date DATETIME,
    patient_care_status INT,
    out_of_care INT,
    prev_rtc_date DATETIME,
    rtc_date DATETIME,
    med_pickup_rtc_date DATETIME,
    arv_first_regimen VARCHAR(500),
    arv_first_regimen_location_id INT,
    arv_first_regimen_start_date DATETIME,
    arv_first_regimen_start_date_flex DATETIME,
    prev_arv_meds VARCHAR(500),
    cur_arv_meds VARCHAR(500),
    cur_arv_meds_strict VARCHAR(500),
    cur_arv_drugs VARCHAR(500),
    prev_arv_drugs VARCHAR(500),
    arv_start_date DATETIME,
    arv_start_location_id INT,
    prev_arv_start_date DATETIME,
    prev_arv_end_date DATETIME,
    prev_arv_line INT,
    cur_arv_line INT,
    cur_arv_line_strict INT,
    cur_arv_line_reported TINYINT,
    prev_arv_adherence VARCHAR(200),
    cur_arv_adherence VARCHAR(200),
    hiv_status_disclosed INT,
    is_pregnant BOOLEAN,
    edd DATETIME,
    tb_screen BOOLEAN,
    tb_screening_result INT,
    tb_screening_datetime DATETIME,
    on_ipt BOOLEAN,
    ipt_start_date DATETIME,
    ipt_stop_date DATETIME,
    ipt_completion_date DATETIME,
    on_tb_tx BOOLEAN,
    tb_tx_start_date DATETIME,
    tb_tx_end_date DATETIME,
    tb_tx_stop_date DATETIME,
    pcp_prophylaxis_start_date DATETIME,
    condoms_provided_date DATETIME,
    modern_contraceptive_method_start_date DATETIME,
    contraceptive_method INT,
    menstruation_status INT,
    is_mother_breastfeeding BOOLEAN,
    cur_who_stage INT,
    discordant_status INT,
    cd4_resulted DOUBLE,
    cd4_resulted_date DATETIME,
    cd4_1 DOUBLE,
    cd4_1_date DATETIME,
    cd4_2 DOUBLE,
    cd4_2_date DATETIME,
    cd4_percent_1 DOUBLE,
    cd4_percent_1_date DATETIME,
    cd4_percent_2 DOUBLE,
    cd4_percent_2_date DATETIME,
    vl_resulted INT,
    vl_resulted_date DATETIME,
    vl_1 INT,
    vl_1_date DATETIME,
    vl_2 INT,
    vl_2_date DATETIME,
    height INT,
    weight INT,
    expected_vl_date SMALLINT,
    vl_order_date DATETIME,
    cd4_order_date DATETIME,
    hiv_dna_pcr_order_date DATETIME,
    hiv_dna_pcr_resulted INT,
    hiv_dna_pcr_resulted_date DATETIME,
    hiv_dna_pcr_1 INT,
    hiv_dna_pcr_1_date DATETIME,
    hiv_dna_pcr_2 INT,
    hiv_dna_pcr_2_date DATETIME,
    hiv_rapid_test_resulted INT,
    hiv_rapid_test_resulted_date DATETIME,
    prev_encounter_datetime_hiv DATETIME,
    next_encounter_datetime_hiv DATETIME,
    prev_encounter_type_hiv MEDIUMINT,
    next_encounter_type_hiv MEDIUMINT,
    prev_clinical_datetime_hiv DATETIME,
    next_clinical_datetime_hiv DATETIME,
    prev_clinical_location_id MEDIUMINT,
    next_clinical_location_id MEDIUMINT,
    prev_clinical_rtc_date_hiv DATETIME,
    next_clinical_rtc_date_hiv DATETIME,
    outreach_date_bncd DATETIME,
    outreach_death_date_bncd DATETIME,
    outreach_patient_care_status_bncd INT,
    transfer_date_bncd DATETIME,
    transfer_transfer_out_bncd DATETIME,
    phone_outreach INT,
    home_outreach INT,
    outreach_attempts INT,
    outreach_missed_visit_reason INT,
    travelled_outside_last_3_months INT,
    travelled_outside_last_6_months INT,
    travelled_outside_last_12_months INT,
    last_cross_boarder_screening_datetime DATETIME,
    is_cross_border_country INT,
    cross_border_service_offered INT,
    country_of_residence INT,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX person_uuid (uuid),
    INDEX location_enc_date (location_uuid , encounter_datetime),
    INDEX enc_date_location (encounter_datetime , location_uuid),
    INDEX location_id_rtc_date (location_id , rtc_date),
    INDEX location_uuid_rtc_date (location_uuid , rtc_date),
    INDEX loc_id_enc_date_next_clinical (location_id , encounter_datetime , next_clinical_datetime_hiv),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
);
                    
                    
                                        
                    if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_hiv_summary_temp_",queue_number);
                            set @queue_table = concat("flat_hiv_summary_build_queue_",queue_number);                                                                    

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_hiv_summary_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            
                            SET @dyn_sql=CONCAT('delete t1 from flat_hiv_summary_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                    end if;
    
                    
                    if (@query_type="sync") then
                            select 'SYNCING..........................................';
                            set @write_table = "flat_hiv_summary_v15b";
                            set @queue_table = "flat_hiv_summary_sync_queue";
CREATE TABLE IF NOT EXISTS flat_hiv_summary_sync_queue (
    person_id INT PRIMARY KEY
);                            
                            


                            set @last_update = null;
SELECT 
    MAX(date_updated)
INTO @last_update FROM
    etl.flat_log
WHERE
    table_name = @table_version;

                            replace into flat_hiv_summary_sync_queue
                            (select distinct patient_id
                                from amrs.encounter
                                where date_changed > @last_update
                            );

                            replace into flat_hiv_summary_sync_queue
                            (select distinct person_id
                                from etl.flat_obs
                                where max_date_created > @last_update
                            );

                            replace into flat_hiv_summary_sync_queue
                            (select distinct person_id
                                from etl.flat_lab_obs
                                where max_date_created > @last_update
                            );

                            replace into flat_hiv_summary_sync_queue
                            (select distinct person_id
                                from etl.flat_orders
                                where max_date_created > @last_update
                            );
                            
                            replace into flat_hiv_summary_sync_queue
                            (select person_id from 
                                amrs.person 
                                where date_voided > @last_update);


                            replace into flat_hiv_summary_sync_queue
                            (select person_id from 
                                amrs.person 
                                where date_changed > @last_update);
                                

                      end if;
                      

                    
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

SELECT @person_ids_count AS 'num patients to sync';



                    
                    SET @dyn_sql=CONCAT('delete t1 from ',@primary_table, ' t1 join ',@queue_table,' t2 using (person_id);'); 
                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  

                    set @total_time=0;
                    set @cycle_number = 0;
                    

                    while @person_ids_count > 0 do

                        set @loop_start_time = now();
                        
                        
                        drop temporary table if exists flat_hiv_summary_build_queue__0;
                        

                        
                        SET @dyn_sql=CONCAT('create temporary table flat_hiv_summary_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;  


                        drop temporary table if exists flat_hiv_summary_0a;
                        create temporary table flat_hiv_summary_0a
                        (select
                            t1.person_id,
                            t1.visit_id,
                            v.visit_type_id as visit_type,
                            t1.encounter_id,
                            t1.encounter_datetime,
                            t1.encounter_type,
							case
                              when v.visit_type_id IS NULL then NULL
                              when v.visit_type_id = 24 then 1
                              when v.visit_type_id IS NOT NULL AND v.visit_type_id != 24 THEN 0
                              else null
                            end as is_transit,
                            t1.location_id,
                            t1.obs,
                            t1.obs_datetimes,

                            
                            case
                                when t1.encounter_type in (1,2,3,4,10,14,15,17,19,26,32,33,34,47,105,106,112,113,114,117,120,127,128,138,140,153,154,158,162,163) then 1
                                when t1.encounter_type in (186) AND v.visit_type_id not in (24,25,80,104) AND v.visit_type_id is NOT NULL then 1
                                when t1.encounter_type in (158) AND v.visit_type_id not in (104) AND v.visit_type_id is NOT NULL then 1
                                else null
                            end as is_clinical_encounter,

                            case
                                when t1.encounter_type in (116) then 20
                                when t1.encounter_type in (1,2,3,4,10,14,15,17,19,26,32,33,34,47,105,106,112,113,114,115,117,120,127,128,138, 140, 153,154,158,162,163,186,212,214,218) then 10
                                when t1.encounter_type in (129) then 5 
                                else 1
                            end as encounter_type_sort_index,
							case
                               when t1.obs regexp "!!7013=" then 2
                               else 1
                            end as enrollment_sort_index,

                            t2.orders
                            from etl.flat_obs t1
                                join flat_hiv_summary_build_queue__0 t0 using (person_id)
                                left join etl.flat_orders t2 using(encounter_id)
								left join amrs.visit v on (v.visit_id = t1.visit_id)
                            where t1.encounter_type in (1,2,3,4,10,14,15,17,19,22,23,26,32,33,43,47,21,105,106,110,111,112,113,114,116,117,120,127,128,129,138,140,153,154,158, 161,162,163,186,212,214,218)
                                AND NOT obs regexp "!!5303=(822|664|1067)!!"  
                                AND NOT obs regexp "!!9082=9036!!"
                        );

                        insert into flat_hiv_summary_0a
                        (select
                            t1.person_id,
                            null,
                            null,
                            t1.encounter_id,
                            t1.test_datetime,
                            t1.encounter_type,
                            null,
                            null, 
                            t1.obs,
                            null, 
                            
                            0 as is_clinical_encounter,
                            1 as encounter_type_sort_index,
                            1 as enrollment_sort_index,
                            null
                            from etl.flat_lab_obs t1
                                join flat_hiv_summary_build_queue__0 t0 using (person_id)
                        );
                        
                        set @enrollment_date = null;
						set @prev_id = null;
                        set @cur_id = null;
                        
                        drop temporary table if exists flat_hiv_summary_enrollment;
                        create temporary table flat_hiv_summary_enrollment(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
                        (select *,
                        @prev_id := @cur_id as prev_id,
						@cur_id := person_id as cur_id,
                         CASE
                                 WHEN
                                     (@enrollment_date IS NULL
                                         || (@enrollment_date IS NOT NULL
                                         AND @prev_id != @cur_id))
                                         AND obs REGEXP '!!7013='
                                 THEN
                                     @enrollment_date:=REPLACE(REPLACE((SUBSTRING_INDEX(SUBSTRING(obs, LOCATE('!!7013=', obs)),
                                                     @sep,
                                                     1)),
                                             '!!7013=',
                                             ''),
                                         '!!',
                                         '')
                                 WHEN
                                     obs REGEXP '!!7015='
                                         AND (@enrollment_date IS NULL
                                         || (@enrollment_date IS NOT NULL
                                         AND @prev_id != @cur_id))
                                 THEN
                                     @enrollment_date:='1900-01-01'
                                 WHEN
                                     encounter_type NOT IN (21 , @lab_encounter_type)
                                         AND (@enrollment_date IS NULL
                                         || (@enrollment_date IS NOT NULL
                                         AND @prev_id != @cur_id))
                                 THEN
                                     @enrollment_date:=DATE(encounter_datetime)
                                 WHEN @prev_id = @cur_id THEN @enrollment_date
                                 ELSE @enrollment_date:=NULL
                             END AS enrollment_date
                        
                        from flat_hiv_summary_0a
                        order by person_id, enrollment_sort_index desc, date(encounter_datetime), encounter_type_sort_index desc 
                        );

                        drop temporary table if exists flat_hiv_summary_0;
                        create temporary table flat_hiv_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
                        (select * from flat_hiv_summary_enrollment
                        order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );


                        set @prev_id = -1;
                        set @cur_id = -1;
                        set @prev_encounter_date = null;
                        set @cur_encounter_date = null;
                        set @enrollment_date = null;
                        set @ovc_non_enrollment_reason = null;
                        set @ovc_non_enrollment_date = null;
                        set @hiv_start_date = null;
                        set @cur_location = null;
                        set @cur_rtc_date = null;
                        set @prev_rtc_date = null;
                        set @med_pickup_rtc_date = null;
                        set @hiv_start_date = null;
                        set @prev_arv_start_date = null;
                        set @arv_start_date = null;
                        set @prev_arv_end_date = null;
                        set @arv_start_location_id = null;
                        set @arv_first_regimen_start_date = null;
                        set @arv_first_regimen_start_date_flex = null;
                        set @arv_first_regimen = null;
                        set @prev_arv_line = null;
                        set @cur_arv_line = null;
                        set @prev_arv_adherence = null;
                        set @cur_arv_adherence = null;
                        set @hiv_status_disclosed = null;
                        set @is_pregnant = null;
                        set @edd = null;
                        set @prev_arv_meds = null;
                        set @cur_arv_meds = null;
                        set @cur_arv_drugs = null;
                        set @prev_arv_drugs = null;
                        set @ipt_start_date = null;
                        set @ipt_end_date = null;
                        set @ipt_completion_date = null;

                        set @on_tb_tx = null;
                        set @tb_tx_start_date = null;
                        set @tb_tx_end_date = null;
                        set @tb_tx_stop_date = null;
                        set @pcp_prophylaxis_start_date = null;
                        set @tb_screen = null;
                        set @tb_screening_result = null;
                        set @tb_screening_datetime = null;
                        
                        set @death_date = null;
                        
                        set @patient_care_status=null;

                        set @condoms_provided_date = null;
                        set @modern_contraceptive_method_start_date = null;
                        set @contraceptive_method = null;
                        set @menstruation_status = null;
                        set @is_mother_breastfeeding = null;
                        
                        set @cur_who_stage = null;

                        set @vl_1=null;
                        set @vl_2=null;
                        set @vl_1_date=null;
                        set @vl_2_date=null;
                        set @vl_resulted=null;
                        set @vl_resulted_date=null;
                        set @height=null;
                        set @weight=null;

                        set @cd4_resulted=null;
                        set @cd4_resulted_date=null;
                        set @cd4_1=null;
                        set @cd4_1_date=null;
                        set @cd4_2=null;
                        set @cd4_2_date=null;
                        set @cd4_percent_1=null;
                        set @cd4_percent_1_date=null;
                        set @cd4_percent_2=null;
                        set @cd4_percent_2_date=null;
                        set @vl_order_date = null;
                        set @cd4_order_date = null;

                        set @hiv_dna_pcr_order_date = null;
                        set @hiv_dna_pcr_1=null;
                        set @hiv_dna_pcr_2=null;
                        set @hiv_dna_pcr_1_date=null;
                        set @hiv_dna_pcr_2_date=null;

                        set @hiv_rapid_test_resulted=null;
                        set @hiv_rapid_test_resulted_date= null;


                        
                        
                        

                        drop temporary table if exists flat_hiv_summary_1;
                        create temporary table flat_hiv_summary_1 (index encounter_id (encounter_id))
                        (select
                            obs,
                            encounter_type_sort_index,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            t1.person_id,
                            p.uuid,
                            t1.visit_id,
                            t1.visit_type,
                            t1.encounter_id,
                            @prev_encounter_date := date(@cur_encounter_date) as prev_encounter_date,
                            @cur_encounter_date := date(encounter_datetime) as cur_encounter_date,
                            t1.encounter_datetime,                            
                            t1.encounter_type,
                            t1.is_transit,
                            t1.is_clinical_encounter,                                                    
                            t1.enrollment_date,             
                            CASE
                             WHEN
                                 (@enrollment_location_id IS NULL
                                     || (@enrollment_location_id IS NOT NULL
                                     AND @prev_id != @cur_id))
                                     AND obs REGEXP '!!7030=5622'
                             THEN
                                 @enrollment_location_id:=9999
                             WHEN
                                 obs REGEXP '!!7015='
                                     AND (@enrollment_location_id IS NULL
                                     || (@enrollment_location_id IS NOT NULL
                                     AND @prev_id != @cur_id))
                             THEN
                                 @enrollmen_location_id:=9999
                             WHEN
                                 encounter_type NOT IN (21 , @lab_encounter_type)
                                     AND (@enrollment_location_id IS NULL
                                     || (@enrollment_location_id IS NOT NULL
                                     AND @prev_id != @cur_id))
                             THEN
                                 @enrollment_location_id:= location_id
                             WHEN @prev_id = @cur_id THEN @enrollment_location_id
                             ELSE @enrollment_location_id:=NULL
                         END AS enrollment_location_id,

                        case 
                            when obs regexp "!!11219=6834" then @ovc_non_enrollment_reason := 6834
                            when obs regexp "!!11219=1504" then @ovc_non_enrollment_reason := 1504                                                  
                            when @prev_id = @cur_id then @ovc_non_enrollment_reason
                            else @ovc_non_enrollment_reason := null
                        end as ovc_non_enrollment_reason,
                         case
                            when  t1.encounter_type = 214  then @ovc_non_enrollment_date := encounter_datetime
                            when @prev_id = @cur_id then @ovc_non_enrollment_date
                            else null
                        end as ovc_non_enrollment_date,

                            
                            
                            if(obs regexp "!!1839="
                                ,replace(replace((substring_index(substring(obs,locate("!!1839=",obs)),@sep,1)),"!!1839=",""),"!!","")
                                ,null) as scheduled_visit,

                            case
                                when location_id then @cur_location := location_id
                                when @prev_id = @cur_id then @cur_location
                                else null
                            end as location_id,

                            case
                                when @prev_id=@cur_id and t1.encounter_type not in (5,6,7,8,9,21) then @visit_num:= @visit_num + 1
                                when @prev_id != @cur_id then @visit_num := 1
                            end as visit_num,
                            case
                              when encounter_type=110 then
                                case
                                  when obs regexp "!!(10532)" then @mdt_session_number:= 4
								  when obs regexp "!!(10527|10528|10529|10530|10531)" then @mdt_session_number:= 3
								  when obs regexp "!!(10523|10524|10525|10526)" then @mdt_session_number:= 2
								  when obs regexp "!!(10518|10519|10520|10521|10522)" then @mdt_session_number:= 1
								  when @prev_id = @cur_id then @mdt_session_number
								else null
							  end
							  else @mdt_session_number
                            end as mdt_session_number,

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
									when obs regexp "!!9605=" then @med_pickup_rtc_date := replace(replace((substring_index(substring(obs,locate("!!9605=",obs)),@sep,1)),"!!9605=",""),"!!","")
									else @med_pickup_rtc_date := null
                            end as med_pickup_rtc_date,
                                                        
                            
                            
                            case
                                when obs regexp "!!1946=1065!!" then 1
                                when obs regexp "!!1285=(1287|9068)!!" then 1
                                when obs regexp "!!1596=" then 1
                                when obs regexp "!!9082=(159|9036|9083|1287|9068|9079|9504|1285)!!" then 1
                                when t1.encounter_type = @death_encounter_type then 1
                                else null
                            end as out_of_care,
                            
                            

                            
                            
                            
                            
                            
                            

                            case
                                when p.dead or p.death_date then @death_date := p.death_date
                                when obs regexp "!!1570=" then @death_date := replace(replace((substring_index(substring(obs,locate("!!1570=",obs)),@sep,1)),"!!1570=",""),"!!","")
                                when @prev_id != @cur_id or @death_date is null then
                                    case
                                        when obs regexp "!!(1734|1573)=" then @death_date := encounter_datetime
                                        when obs regexp "!!(1733|9082|6206)=159!!" or t1.encounter_type=31 then @death_date := encounter_datetime
                                        else @death_date := null
                                    end
                                else @death_date
                            end as death_date,

                            
                            
                            
                            
                            case
                                when @death_date <= encounter_datetime then @patient_care_status := 159
                                when obs regexp "!!1946=1065!!" then @patient_care_status := 9036
                                when obs regexp "!!1285=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!1285=",obs)),@sep,1)),"!!1285=",""),"!!","")
                                when obs regexp "!!1596=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!1596=",obs)),@sep,1)),"!!1596=",""),"!!","")
                                when obs regexp "!!9082=" then @patient_care_status := replace(replace((substring_index(substring(obs,locate("!!9082=",obs)),@sep,1)),"!!9082=",""),"!!","")
                                
                                when t1.encounter_type = @lab_encounter_type and @cur_id != @prev_id then @patient_care_status := null
                                when t1.encounter_type = @lab_encounter_type and @cur_id = @prev_id then @patient_care_status
                                else @patient_care_status := 6101
                            end as patient_care_status,

                            
                            case
                                when obs regexp "!!9203=" then @hiv_start_date := replace(replace((substring_index(substring(obs,locate("!!9203=",obs)),@sep,1)),"!!9203=",""),"!!","")
                                when obs regexp "!!7015=" then @hiv_start_date := "1900-01-01"
                                when @hiv_start_date is null then @hiv_start_date := date(encounter_datetime)
                                when @prev_id = @cur_id then @hiv_start_date
                                else @hiv_start_date := null
                            end as hiv_start_date,

                            case
                                when obs regexp "!!1255=1256!!" or (obs regexp "!!1255=(1257|1259|981|1258|1849|1850)!!" and @arv_start_date is null ) then @arv_start_location_id := location_id
                                when @prev_id = @cur_id and obs regexp "!!(1250|1088|2154)=" and @arv_start_date is null then @arv_start_location_id := location_id
                                when @prev_id != @cur_id then @arv_start_location_id := null
                                else @arv_start_location_id
                            end as arv_start_location_id,


                            case
                                when @prev_id=@cur_id and @cur_arv_meds is not null then @prev_arv_meds := @cur_arv_meds
                                when @prev_id=@cur_id then @prev_arv_meds
                                else @prev_arv_meds := null
                            end as prev_arv_meds,

                            
                            
                            
                            #2154 : PATIENT REPORTED CURRENT ANTIRETROVIRAL TREATMENT
                            #2157 : PATIENT REPORTED PAST ANTIRETROVIRAL TREATMENT
                            case
                                when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_meds := null
                                when obs regexp "!!1250=" then @cur_arv_meds := normalize_arvs(obs,'1250')
                                    
                                    
                                when obs regexp "!!1088=" then @cur_arv_meds := normalize_arvs(obs,'1088')
                                                                        
                                when obs regexp "!!2154=" then @cur_arv_meds := normalize_arvs(obs,'2154')
								
                                when obs regexp "!!2157=" and not obs regexp "!!2157=1066" then @cur_arv_meds := normalize_arvs(obs,'2157')
                                    
                                when @prev_id = @cur_id then @cur_arv_meds
                                else @cur_arv_meds:= null
                            end as cur_arv_meds,
                            

                            case
								when 1 then null
                                when obs regexp "!!1255=(1107|1260)!!" then null
                                when obs regexp "!!1250=" then @cur_arv_meds := normalize_arvs(obs,'1250')
                                    
                                    
                                when obs regexp "!!1088=" then @cur_arv_meds := normalize_arvs(obs,'1088')
                                    
                                    
                                when obs regexp "!!2154=" then @cur_arv_meds := normalize_arvs(obs,'2154')
								when obs regexp "!!2157=" and not obs regexp "!!2157=1066"  then @cur_arv_meds := normalize_arvs(obs,'2157')
                                    
                                else null
                            end as cur_arv_meds_strict,
                            
                             case
                            
                                when @prev_id=@cur_id and @prev_arv_drugs is not null then @prev_arv_drugs := @cur_arv_drugs
                                when @prev_id=@cur_id then @prev_arv_drugs
                                else @prev_arv_drugs := null
                            
                            end as prev_arv_drugs,
                            
                            case
                            
								when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_drugs := null
                                when obs regexp "!!1250=" then @cur_arv_drugs := normalizeArvsDrugFormulation(obs)
                                    
                                    
                                when obs regexp "!!1088=" then @cur_arv_drugs := normalizeArvsDrugFormulation(obs)
                                                                        
                                when obs regexp "!!2154=" then @cur_arv_drugs := normalizeArvsDrugFormulation(obs)
								
                                when obs regexp "!!2157=" and not obs regexp "!!2157=1066" then @cur_arv_drugs := normalizeArvsDrugFormulation(obs)
                                    
                                when @prev_id = @cur_id then @cur_arv_drugs
                                else @cur_arv_drugs:= null
                            
			               end as cur_arv_drugs,


							case
                                when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                                else @prev_clinical_datetime := null
                            end as prev_clinical_datetime_hiv,
                            
                            case
                                when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
                                when @prev_id = @cur_id then @cur_clinical_datetime
                                else @cur_clinical_datetime := null
                            end as cur_clinical_datetime,
                            
                            
							case
                                when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
                                else @prev_clinical_rtc_date := null
                            end as prev_clinical_rtc_date_hiv,

                            case
                                when is_clinical_encounter then @cur_clinical_rtc_date := @cur_rtc_date
                                when @prev_id = @cur_id then @cur_clinical_rtc_date
                                else @cur_clinical_rtc_date:= null
                            end as cur_clinic_rtc_date,

                            
                            CASE                             
                                WHEN
                                    (@arv_first_regimen_start_date IS NULL || @prev_id != @cur_id)
									AND obs REGEXP '!!1499='
                                THEN
                                    @arv_first_regimen_start_date:=GetValues(obs,1499)                                        

                                WHEN
                                    (@arv_first_regimen_start_date IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!2157=' AND NOT obs regexp '!!2157=1066!!'                                        
                                THEN
                                    @arv_first_regimen_start_date:='1900-01-01'

                                WHEN
                                    (@arv_first_regimen_start_date IS NULL || @prev_id != @cur_id)
									AND (obs REGEXP '!!1255=(1256)!!' || obs REGEXP '!!1250=')
                                THEN
                                    @arv_first_regimen_start_date:=DATE(encounter_datetime)

                                    
								WHEN
                                    (@arv_first_regimen_start_date IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!1088=' AND NOT obs regexp '!!1088=1107!!'
										AND not obs regexp'!!7015='
										AND (@prev_clinical_datetime is null 
														or timestampdiff(day,ifnull(@prev_clinical_rtc_date,date_add(@prev_clinical_datetime, interval 90 day)),encounter_datetime) < 90)
                                THEN
                                    @arv_first_regimen_start_date:= "1900-01-01" #DATE(encounter_datetime)

                                WHEN
                                    (@arv_first_regimen_start_date IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!2154=' AND NOT obs regexp '!!2154=1066!!'
										AND not obs regexp'!!7015='
										AND (@prev_clinical_datetime is null 
														or timestampdiff(day,ifnull(@prev_clinical_rtc_date,date_add(@prev_clinical_datetime, interval 90 day)),encounter_datetime) < 90)
                                THEN
                                    @arv_first_regimen_start_date:= "1900-01-01" # DATE(encounter_datetime)

                                WHEN
                                    (@arv_first_regimen_start_date IS NULL || @prev_id != @cur_id)
                                        AND @cur_arv_meds IS NOT NULL
                                THEN
                                    @arv_first_regimen_start_date:='1900-01-01'
								WHEN @arv_first_regimen_start_date = "1900-01-01" AND obs regexp '!!1499=' then 
                                     @arv_first_regimen_start_date := replace(replace((substring_index(substring(obs,locate("!!1499=",obs)),@sep,1)),"!!1499=",""),"!!","")
                                WHEN @prev_id = @cur_id THEN @arv_first_regimen_start_date
                                WHEN @prev_id != @cur_id THEN @arv_first_regimen_start_date:=NULL
                                ELSE @arv_first_regimen_start_date
                            END AS arv_first_regimen_start_date,
                            


                            
                            /*
							CASE                             
                                WHEN
                                    (@arv_first_regimen_start_date_flex IS NULL || @prev_id != @cur_id)
									AND obs REGEXP '!!1499='
                                THEN
                                    @arv_first_regimen_start_date_flex := GetValues(obs,1499)                               
								WHEN
                                    (@arv_first_regimen_start_date_flex IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!2157=' AND NOT obs regexp '!!2157=1066!!'                                        
                                THEN
                                    @arv_first_regimen_start_date_flex :='1900-01-01'

                                WHEN
                                    (@arv_first_regimen_start_date_flex IS NULL || @prev_id != @cur_id)
									AND (obs REGEXP '!!1255=(1256)!!' || obs REGEXP '!!1250=')
                                THEN
                                    @arv_first_regimen_start_date_flex:=DATE(encounter_datetime)


								WHEN
                                    (@arv_first_regimen_start_date_flex IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!1088=' AND NOT obs regexp '!!1088=1107!!'                                        
                                        AND not obs regexp'!!7015='
										AND (@prev_clinical_datetime is null 
														or timestampdiff(day,ifnull(@prev_clinical_rtc_date,date_add(@prev_clinical_datetime, interval 90 day)),encounter_datetime) < 90)
                                        
                                THEN
                                    @arv_first_regimen_start_date_flex :=date(encounter_datetime)

                                WHEN
                                    (@arv_first_regimen_start_date_flex IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!2154=' AND NOT obs regexp '!!2154=1066!!'
                                        AND not obs regexp'!!7015='
										AND (@prev_clinical_datetime is null 
														or timestampdiff(day,ifnull(@prev_clinical_rtc_date,date_add(@prev_clinical_datetime, interval 90 day)),encounter_datetime) < 90)
                                        
                                THEN
                                    @arv_first_regimen_start_date_flex :=date(encounter_datetime)

                                WHEN
                                    (@arv_first_regimen_start_date_flex IS NULL || @prev_id != @cur_id)
                                        AND @cur_arv_meds IS NOT NULL
                                THEN
                                    @arv_first_regimen_start_date_flex:='1900-01-01'
                                WHEN @prev_id = @cur_id THEN @arv_first_regimen_start_date_flex
                                WHEN @prev_id != @cur_id THEN @arv_first_regimen_start_date_flex:=NULL
                                ELSE @arv_first_regimen_start_date_flex
                            END 
                            */
                            null AS arv_first_regimen_start_date_flex,



/*
                            case
                                when @arv_first_regimen is null and obs regexp "!!2157=" and not obs regexp "!!2157=1066" then @arv_first_regimen := normalize_arvs(obs,'2157')
                                when obs regexp "!!7015=" and @arv_first_regimen is null then @arv_first_regimen := "unknown"
                                when @arv_first_regimen is null and @cur_arv_meds is not null then @arv_first_regimen := @cur_arv_meds
                                when @prev_id = @cur_id then @arv_first_regimen
                                when @prev_id != @cur_id then @arv_first_regimen := @cur_arv_meds
                                else "-1"
                            end as arv_first_regimen,
*/
						
							CASE                             
                                WHEN
                                    (@arv_first_regimen IS NULL || @prev_id != @cur_id)
									AND obs REGEXP '!!1499='
                                THEN
                                    @arv_first_regimen:= "unknown"

                                WHEN
                                    (@arv_first_regimen IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!2157=' AND NOT obs regexp '!!2157=1066!!'                                        
                                THEN
                                    @arv_first_regimen:= "unknown"

                                WHEN
                                    (@arv_first_regimen IS NULL || @prev_id != @cur_id)
									AND (obs REGEXP '!!1255=(1256)!!' || obs REGEXP '!!1250=')
                                THEN
                                    @arv_first_regimen := @cur_arv_meds

                                    
								WHEN
                                    (@arv_first_regimen IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!1088=' AND NOT obs regexp '!!1088=1107!!'
										AND not obs regexp'!!7015='
										AND (@prev_clinical_datetime is null 
														or timestampdiff(day,ifnull(@prev_clinical_rtc_date,date_add(@prev_clinical_datetime, interval 90 day)),encounter_datetime) < 90)

                                THEN
                                    @arv_first_regimen:= "unknown" #@cur_arv_meds

                                WHEN
                                    (@arv_first_regimen IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!2154=' AND NOT obs regexp '!!2154=1066!!'
										AND not obs regexp'!!7015='
										AND (@prev_clinical_datetime is null 
														or timestampdiff(day,ifnull(@prev_clinical_rtc_date,date_add(@prev_clinical_datetime, interval 90 day)),encounter_datetime) < 90)

                                THEN
                                    @arv_first_regimen:= "unknown" #@cur_arv_meds

                                WHEN
                                    (@arv_first_regimen IS NULL || @prev_id != @cur_id)
                                        AND @cur_arv_meds IS NOT NULL
                                THEN
                                    @arv_first_regimen := "unknown"
                                    
								 WHEN @arv_first_regimen = "unknown"  AND obs regexp '!!1633=1065!!' AND obs regexp '!!2157=' then 
                                     @arv_first_regimen := normalize_arvs(obs,'2157')
                                    
                                WHEN @prev_id = @cur_id THEN @arv_first_regimen
                                WHEN @prev_id != @cur_id THEN @arv_first_regimen:=NULL
                                ELSE @arv_first_regimen
                            END AS arv_first_regimen,
                                              
                                              
/*						                            
                            case																	
                                when @arv_first_regimen is null and obs regexp "!!1499=" then  @arv_first_regimen_location_id := 9999
                                when @prev_id != @cur_id and @cur_arv_meds is not null then @arv_first_regimen_location_id := location_id                                
                                when @arv_first_regimen_location_id is null and @cur_arv_meds is not null then @arv_first_regimen_location_id := location_id
                                when @prev_id = @cur_id then @arv_first_regimen_location_id
                                when @prev_id != @cur_id then @arv_first_regimen_location_id := null
                                else "-1"
                            end as arv_first_regimen_location_id,
*/

							CASE                             
                                WHEN
                                    (@arv_first_regimen_location_id IS NULL || @prev_id != @cur_id)
									AND obs REGEXP '!!1499='
                                THEN
                                    @arv_first_regimen_location_id := 9999                                         

                                WHEN
                                    (@arv_first_regimen_location_id IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!2157=' AND NOT obs regexp '!!2157=1066!!'                                        
                                THEN
                                    @arv_first_regimen_location_id:=9999

                                WHEN
                                    (@arv_first_regimen_location_id IS NULL || @prev_id != @cur_id)
									AND (obs REGEXP '!!1255=(1256)!!' || obs REGEXP '!!1250=')
                                THEN
                                    @arv_first_regimen_location_id:=location_id

                                    
								WHEN
                                    (@arv_first_regimen_location_id IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!1088=' AND NOT obs regexp '!!1088=1107!!'
										AND not obs regexp'!!7015='
										AND (@prev_clinical_datetime is null 
														or timestampdiff(day,ifnull(@prev_clinical_rtc_date,date_add(@prev_clinical_datetime, interval 90 day)),encounter_datetime) < 90)
                                THEN
                                    @arv_first_regimen_location_id:= 9999 #location_id

                                WHEN
                                    (@arv_first_regimen_location_id IS NULL || @prev_id != @cur_id)
										AND obs regexp '!!2154=' AND NOT obs regexp '!!2154=1066!!'
										AND not obs regexp'!!7015='
										AND (@prev_clinical_datetime is null 
														or timestampdiff(day,ifnull(@prev_clinical_rtc_date,date_add(@prev_clinical_datetime, interval 90 day)),encounter_datetime) < 90)
                                THEN
                                    @arv_first_regimen_location_id:= 9999 #location_id

                                WHEN
                                    (@arv_first_regimen_location_id IS NULL || @prev_id != @cur_id)
                                        AND @cur_arv_meds IS NOT NULL
                                THEN
                                    @arv_first_regimen_location_id:=9999
                                WHEN @prev_id = @cur_id THEN @arv_first_regimen_location_id
                                WHEN @prev_id != @cur_id THEN @arv_first_regimen_location_id:=NULL
                                ELSE @arv_first_regimen_location_id
                            END AS arv_first_regimen_location_id,



                            case
                                when @prev_id=@cur_id then @prev_arv_line := @cur_arv_line
                                else @prev_arv_line := null
                            end as prev_arv_line,

                            case
                                when obs regexp "!!1255=(1107|1260)!!" then @cur_arv_line := null
                                when obs regexp "!!1250=(6467|6964|792|633|631|9759)!!" then @cur_arv_line := 1
                                when obs regexp "!!1250=(794|635|6160|6159)!!" then @cur_arv_line := 2
                                when obs regexp "!!1250=(6156)!!" then @cur_arv_line := 3
                                when obs regexp "!!1088=(6467|6964|792|633|631|9759)!!" then @cur_arv_line := 1
                                when obs regexp "!!1088=(794|635|6160|6159)!!" then @cur_arv_line := 2
                                when obs regexp "!!1088=(6156)!!" then @cur_arv_line := 3
                                when obs regexp "!!2154=(6467|6964|792|633|631|9759)!!" then @cur_arv_line := 1
                                when obs regexp "!!2154=(794|635|6160|6159)!!" then @cur_arv_line := 2
                                when obs regexp "!!2154=(6156)!!" then @cur_arv_line := 3
                                when @prev_id = @cur_id then @cur_arv_line
                                else @cur_arv_line := null
                            end as cur_arv_line,
                            
                            case
                                when obs regexp "!!1255=(1107|1260)!!" then null
                                when obs regexp "!!1250=(6467|6964|792|633|631|9759)!!" then 1
                                when obs regexp "!!1250=(794|635|6160|6159)!!" then 2
                                when obs regexp "!!1250=(6156)!!" then 3
                                when obs regexp "!!1088=(6467|6964|792|633|631|9759)!!" then 1
                                when obs regexp "!!1088=(794|635|6160|6159)!!" then 2
                                when obs regexp "!!1088=(6156)!!" then 3
                                when obs regexp "!!2154=(6467|6964|792|633|631|9759)!!" then 1
                                when obs regexp "!!2154=(794|635|6160|6159)!!" then 2
                                when obs regexp "!!2154=(6156)!!" then 3
                                else null
                            end as cur_arv_line_strict,
                            

                            
                            
                            
                            
                            
                            
                            
                            case
                                when obs regexp "!!6976=6693!!" then @cur_arv_line_reported := 1
                                when obs regexp "!!6976=6694!!" then @cur_arv_line_reported := 2
                                when obs regexp "!!6976=6695!!" then @cur_arv_line_reported := 3

                                when obs regexp "!!6744=6693!!" then @cur_arv_line_reported := 1
                                when obs regexp "!!6744=6694!!" then @cur_arv_line_reported := 2
                                when obs regexp "!!6744=6695!!" then @cur_arv_line_reported := 3
                                when @prev_id = @cur_id then @cur_arv_line_reported
                                else @cur_arv_line_reported := null
                            end as cur_arv_line_reported,




                            case
                                when @prev_id=@cur_id then @prev_arv_start_date := @arv_start_date
                                else @prev_arv_start_date := null
                            end as prev_arv_start_date,

                            
                            
                            
                            
                            

                            case
                                when obs regexp "!!1255=(1256|1259|1850)" or (obs regexp "!!1255=(1257|1259|981|1258|1849|1850)!!" and @arv_start_date is null ) then @arv_start_date := date(t1.encounter_datetime)
                                when obs regexp "!!1255=(1107|1260)!!" then @arv_start_date := null
                                
                                when @cur_arv_meds != @prev_arv_meds then @arv_start_date := date(t1.encounter_datetime)

                                when @prev_id != @cur_id then @arv_start_date := null
                                else @arv_start_date
                            end as arv_start_date,


                            case
                                when @prev_arv_start_date != @arv_start_date then @prev_arv_end_date  := date(t1.encounter_datetime)
                                else @prev_arv_end_date
                            end as prev_arv_end_date,

                            case
                                when @prev_id=@cur_id then @prev_arv_adherence := @cur_arv_adherence
                                else @prev_arv_adherence := null
                            end as prev_arv_adherence,

                            
                            
                            
                            
                            case
                                when obs regexp "!!8288=6343!!" then @cur_arv_adherence := 'GOOD'
                                when obs regexp "!!8288=6655!!" then @cur_arv_adherence := 'FAIR'
                                when obs regexp "!!8288=6656!!" then @cur_arv_adherence := 'POOR'
                                when @prev_id = @cur_id then @cur_arv_adherence
                                else @cur_arv_adherence := null
                            end as cur_arv_adherence,

                            case
                                when obs regexp "!!6596=(6594|1267|6595)!!" then  @hiv_status_disclosed := 1
                                when obs regexp "!!6596=1118!!" then 0
                                when @prev_id = @cur_id then @hiv_status_disclosed
                                else @hiv_status_disclosed := null
                            end as hiv_status_disclosed,


                            
                            
                            
                            

                            
                            
                            
                            
                            
                            
                            
                            
                            
                            
                            case
                                when obs regexp "!!8351=(48|50|1066|1624|6971|9608)!!" then @is_pregnant := null
                                when @prev_id != @cur_id then
                                    case
                                        when t1.encounter_type in (32,33,44,10) or obs regexp "!!(1279|5596)=" or obs regexp "!!8351=(1065|1484)!!" then @is_pregnant := true
                                        else @is_pregnant := null
                                    end
                                when @is_pregnant is null and (t1.encounter_type in (32,33,44,10) or obs regexp "!!(1279|5596)=") or obs regexp "!!8351=(1065|1484)!!"then @is_pregnant := true
                                when @is_pregnant and (t1.encounter_type in (11,47,34) or timestampdiff(week,@is_pregnant,encounter_datetime) > 40 or timestampdiff(week,@edd,encounter_datetime) > 40 or obs regexp "!!5599=|!!1156=1065!!") then @is_pregnant := null
                                else @is_pregnant
                            end as is_pregnant,


                            
                            
                            
                            
                            

                            case
                                when @prev_id != @cur_id then
                                    case
                                        when @is_pregnant and obs regexp "!!1836=" then @edd :=
                                            date_add(replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!",""),interval 280 day)
                                        when obs regexp "!!1279=" then @edd :=
                                            date_add(encounter_datetime,interval (40-replace(replace((substring_index(substring(obs,locate("!!1279=",obs)),@sep,1)),"!!1279=",""),"!!","")) week)
                                        when obs regexp "!!5596=" then @edd :=
                                            replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
                                        else @edd := null
                                    end
                                when @edd is null then
                                    case
                                        when @is_pregnant and obs regexp "!!1836=" then @edd :=
                                            date_add(replace(replace((substring_index(substring(obs,locate("!!1836=",obs)),@sep,1)),"!!1836=",""),"!!",""),interval 280 day)
                                        when obs regexp "!!1279=" then @edd :=
                                            date_add(encounter_datetime,interval (40-replace(replace((substring_index(substring(obs,locate("!!1279=",obs)),@sep,1)),"!!1279=",""),"!!","")) week)
                                        when obs regexp "!!5596=" then @edd :=
                                            replace(replace((substring_index(substring(obs,locate("!!5596=",obs)),@sep,1)),"!!5596=",""),"!!","")
                                        else @edd
                                    end
                                when @edd and (t1.encounter_type in (11,47,34) or timestampdiff(week,@edd,encounter_datetime) > 4 or obs regexp "!!5599|!!1145=1065!!") then @edd := null
                                else @edd
                            end as edd,

						
                            
                            case
                                when obs regexp "!!6174=" then @tb_screen := true 
                                when obs regexp "!!2022=1065!!" then @tb_screen := true 
                                when obs regexp "!!307=" then @tb_screen := true 
                                when obs regexp "!!12=" then @tb_screen := true 
                                when obs regexp "!!1271=(12|307|8064|2311|2323)!!" then @tb_screen := true 
                                when orders regexp "(12|307|8064|2311|2323)" then @tb_screen := true 
                                when obs regexp "!!1866=(12|307|8064|2311|2323)!!" then @tb_screen := true 
                                when obs regexp "!!5958=1077!!" then @tb_screen := true 
                                when obs regexp "!!2020=1065!!" then @tb_screen := true 
                                when obs regexp "!!2021=1065!!" then @tb_screen := true 
                                when obs regexp "!!2028=" then @tb_screen := true 
                                when obs regexp "!!1268=(1256|1850)!!" then @tb_screen := true
                                when obs regexp "!!5959=(1073|1074)!!" then @tb_screen := true 
                                when obs regexp "!!5971=(1073|1074)!!" then @tb_screen := true 
                                when obs regexp "!!1492=107!!" then @tb_screen := true 
                                when obs regexp "!!1270=" and obs not regexp "!!1268=1257!!" then @tb_screen := true
                            end as tb_screen,
                            
                            case
                                when obs regexp "!!8292=" then @tb_screening_result := 
                                    replace(replace((substring_index(substring(obs,locate("!!8292=",obs)),@sep,1)),"!!8292=",""),"!!","")
                                when @prev_id != @cur_id then @tb_screening_result := null
                                else @tb_screening_result 
                            end as tb_screening_result,

                            case
                                when obs regexp "!!6174=" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!2022=1065!!" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!307=" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!12=" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!1271=(12|307|8064|2311|2323)!!" then @tb_screening_datetime := encounter_datetime 
                                when orders regexp "(12|307|8064|2311|2323)" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!1866=(12|307|8064|2311|2323)!!" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!5958=1077!!" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!2020=1065!!" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!2021=1065!!" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!2028=" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!1268=(1256|1850)!!" then @tb_screening_datetime := encounter_datetime
                                when obs regexp "!!5959=(1073|1074)!!" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!5971=(1073|1074)!!" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!1492=107!!" then @tb_screening_datetime := encounter_datetime 
                                when obs regexp "!!1270=" and obs not regexp "!!1268=1257!!" then @tb_screening_datetime := encounter_datetime
                                when @cur_id = @prev_id then @tb_screening_datetime
                                else @tb_screening_datetime := null
                            end as tb_screening_datetime,


                            case
                                when obs regexp "!!1265=(1107|1260)!!" then @on_ipt := 0
                                when obs regexp "!!1265=!!" then @on_ipt := 1
                                when obs regexp "!!1110=656!!" then @on_ipt := 1
								when @cur_id != @prev_id then @on_ipt := null
                                else @on_ipt
                            end as on_ipt,

                                                                                    
                            
                            case
                                when obs regexp "!!1265=(1256|1850)!!" then @ipt_start_date := encounter_datetime
                                when obs regexp "!!1265=(1257|981|1406|1849)!!" and @ipt_start_date is null then @ipt_start_date := encounter_datetime
                                when obs regexp "!!10591=1065" AND obs regexp "!!6984=" then @ipt_start_date := replace(replace((substring_index(substring(obs,locate("!!6984=",obs)),@sep,1)),"!!6984=",""),"!!","")
								when obs regexp "!!10591=1065" and obs regexp "!!1190=" then @ipt_start_date := replace(replace((substring_index(substring(obs,locate("!!1190=",obs)),@sep,1)),"!!1190=",""),"!!","")
                                when @cur_id != @prev_id then @ipt_start_date := null
                                else @ipt_start_date
                            end as ipt_start_date,
                                                        
                            
                            
                            case
                                when obs regexp "!!1266=" then @ipt_stop_date :=  encounter_datetime
                                when obs regexp "!!10591=1065" and obs regexp "!!8603=" then @ipt_stop_date := replace(replace((substring_index(substring(obs,locate("!!8603=",obs)),@sep,1)),"!!8603=",""),"!!","")
                                when @cur_id = @prev_id then @ipt_stop_date
                                when @cur_id != @prev_id then @ipt_stop_date := null
                                else @ipt_stop_date
                            end as ipt_stop_date,
                            
                            case
                                when obs regexp "!!1266=1267!!" then @ipt_completion_date :=  encounter_datetime
                                when obs regexp "!!10591=1065" and obs regexp "!!8603=" then @ipt_completion_date := replace(replace((substring_index(substring(obs,locate("!!8603=",obs)),@sep,1)),"!!8603=",""),"!!","")
                                when @cur_id = @prev_id then @ipt_completion_date
                                when @cur_id != @prev_id then @ipt_completion_date := null
                                else @ipt_completion_date
                            end as ipt_completion_date,
                            

                            case
                                when obs regexp "!!1268=(1107|1260)!!" then @on_tb_tx := 0
                                when obs regexp "!!1268=" then @on_tb_tx := 1 
                                when obs regexp "!!1111=" and obs not regexp "!!1111=(1267|1107)!!" then @on_tb_tx := 1                                
                                else @on_tb_tx := 0
                            end as on_tb_tx,

                                                                                                                
                            
                            case
                                when obs regexp "!!1113=" then @tb_tx_start_date := date(replace(replace((substring_index(substring(obs,locate("!!1113=",obs)),@sep,1)),"!!1113=",""),"!!",""))
                                when obs regexp "!!1268=1256!!" then @tb_tx_start_date := encounter_datetime
                                when obs regexp "!!1268=(1257|1259|1849|981)!!" and obs regexp "!!7015=" and @tb_tx_start_date is null then @tb_tx_start_date := null
                                when obs regexp "!!1268=(1257|1259|1849|981)!!" and @tb_tx_start_date is null then @tb_tx_start_date := encounter_datetime
                                
                                when obs regexp "!!1111=" and obs not regexp "!!1111=(1267|1107)!!" and @tb_tx_start_date is null then @tb_tx_start_date := encounter_datetime
                                when @cur_id = @prev_id then @tb_tx_start_date
                                else @tb_tx_start_date := null
                            end as tb_tx_start_date,

                            
                            
                            
                            case
                                    when obs regexp "!!2041=" then @tb_tx_end_date := date(replace(replace((substring_index(substring(obs,locate("!!2041=",obs)),@sep,1)),"!!2041=",""),"!!",""))
                                    when obs regexp "!!1268=1267!!" then @tb_tx_end_date := encounter_datetime
                                    when @cur_id = @prev_id then @tb_tx_end_date
                                    else @tb_tx_end_date := null
                            end as tb_tx_end_date,
                            
                            case
                                    when obs regexp "!!1268=(7043|7065|1259|44|7061|102|5622)!!" then @tb_tx_stop_date := encounter_datetime
                                    when @cur_id = @prev_id then @tb_tx_stop_date
                                    else @tb_tx_stop_date := null
                            end as tb_tx_stop_date,


                            
                            
                            case
                                when obs regexp "!!1261=(1107|1260)!!" then @pcp_prophylaxis_start_date := null
                                when obs regexp "!!1261=(1256|1850)!!" then @pcp_prophylaxis_start_date := encounter_datetime
                                when obs regexp "!!1261=1257!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
                                when obs regexp "!!1109=(916|92)!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
                                when obs regexp "!!1193=(916|92)!!" and @pcp_prophylaxis_start_date is null then @pcp_prophylaxis_start_date := encounter_datetime
                                when @prev_id=@cur_id then @pcp_prophylaxis_start_date
                                else @pcp_prophylaxis_start_date := null
                            end as pcp_prophylaxis_start_date,

                            
                            case
                                when @prev_id=@cur_id then
                                    case
                                        when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" and @cd4_1 >= 0 and date(encounter_datetime)<>@cd4_1_date then @cd4_2:= @cd4_1
                                        else @cd4_2
                                    end
                                else @cd4_2:=null
                            end as cd4_2,

                            case
                                when @prev_id=@cur_id then
                                    case
                                        when t1.encounter_type=@lab_encounter_type and obs regexp "!!5497=[0-9]" and @cd4_1 >= 0 then @cd4_2_date:= @cd4_1_date
                                        else @cd4_2_date
                                    end
                                else @cd4_2_date:=null
                            end as cd4_2_date,

                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_date_resulted := date(encounter_datetime)
                                when @prev_id = @cur_id and date(encounter_datetime) = @cd4_date_resulted then @cd4_date_resulted
                            end as cd4_resulted_date,

                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_resulted := cast(replace(replace((substring_index(substring(obs,locate("!!5497=",obs)),@sep,1)),"!!5497=",""),"!!","") + 0 as unsigned)
                                when @prev_id = @cur_id and date(encounter_datetime) = @cd4_date_resulted then @cd4_resulted
                            end as cd4_resulted,



                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_1:= cast(replace(replace((substring_index(substring(obs,locate("!!5497=",obs)),@sep,1)),"!!5497=",""),"!!","") + 0 as unsigned)
                                when @prev_id=@cur_id then @cd4_1
                                else @cd4_1:=null
                            end as cd4_1,


                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!5497=[0-9]" then @cd4_1_date:=date(encounter_datetime)
                                when @prev_id=@cur_id then @cd4_1_date
                                else @cd4_1_date:=null
                            end as cd4_1_date,

                            
                            case
                                when @prev_id=@cur_id then
                                    case
                                        when t1.encounter_type=@lab_encounter_type and obs regexp "!!730=[0-9]" and @cd4_percent_1 >= 0
                                            then @cd4_percent_2:= @cd4_percent_1
                                        else @cd4_percent_2
                                    end
                                else @cd4_percent_2:=null
                            end as cd4_percent_2,

                            case
                                when @prev_id=@cur_id then
                                    case
                                        when obs regexp "!!730=[0-9]" and t1.encounter_type = @lab_encounter_type and @cd4_percent_1 >= 0 then @cd4_percent_2_date:= @cd4_percent_1_date
                                        else @cd4_percent_2_date
                                    end
                                else @cd4_percent_2_date:=null
                            end as cd4_percent_2_date,


                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!730=[0-9]"
                                    then @cd4_percent_1:= cast(replace(replace((substring_index(substring(obs,locate("!!730=",obs)),@sep,1)),"!!730=",""),"!!","") + 1 as unsigned)
                                when @prev_id=@cur_id then @cd4_percent_1
                                else @cd4_percent_1:=null
                            end as cd4_percent_1,

                            case
                                when obs regexp "!!730=[0-9]" and t1.encounter_type = @lab_encounter_type then @cd4_percent_1_date:=date(encounter_datetime)
                                when @prev_id=@cur_id then @cd4_percent_1_date
                                else @cd4_percent_1_date:=null
                            end as cd4_percent_1_date,


                            
                            case
                                    when @prev_id=@cur_id then
                                        case
                                            when obs regexp "!!856=[0-9]" and @vl_1 >= 0
                                                and 
                                                    if(obs_datetimes is null,encounter_datetime, 
                                                        date(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""))) <> date(@vl_1_date) then @vl_2:= @vl_1
                                            else @vl_2
                                        end
                                    else @vl_2:=null
                            end as vl_2,

                            case
                                    when @prev_id=@cur_id then
                                        case
                                            when obs regexp "!!856=[0-9]" and @vl_1 >= 0
                                                and 
                                                    if(obs_datetimes is null,encounter_datetime,
                                                        date(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""))) <>date(@vl_1_date) then @vl_2_date:= @vl_1_date
                                            else @vl_2_date
                                        end
                                    else @vl_2_date:=null
                            end as vl_2_date,
                            case 
                                when obs regexp "!!5090=" then @height := cast(replace(replace((substring_index(substring(obs,locate("!!5090=",obs)),@sep,1)),"!!5090=",""),"!!","") as decimal(4,1))
                                when @prev_id = @cur_id then @height
                                else @height := null
                            end as height,
                            case 
                                when obs regexp "!!5089=" then @weight := cast(replace(replace((substring_index(substring(obs,locate("!!5089=",obs)),@sep,1)),"!!5089=",""),"!!","") as decimal(4,1))
                                when @prev_id = @cur_id then @weight
                                else @weight := null
                            end as weight,

                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_date_resulted := date(encounter_datetime)
                                when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_date_resulted
                            end as vl_resulted_date,

                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_resulted := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") + 0 as unsigned)
                                when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_resulted
                            end as vl_resulted,

                            case
                                    when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") + 0 as unsigned)
                                    when obs regexp "!!856=[0-9]"
                                            and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
                                            and (@vl_1_date is null or (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) > @vl_1_date)
                                        then @vl_1 := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
                                    when @prev_id=@cur_id then @vl_1
                                    else @vl_1:=null
                            end as vl_1,

                            case
                                when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1_date:= encounter_datetime
                                when obs regexp "!!856=[0-9]"
                                        and (@vl_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!",""),@vl_1_date)) > 30)
                                        and (@vl_1_date is null or (replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")) > @vl_1_date)
                                    then @vl_1_date := replace(replace((substring_index(substring(obs_datetimes,locate("!!856=",obs_datetimes)),@sep,1)),"!!856=",""),"!!","")
                                when @prev_id=@cur_id then @vl_1_date
                                else @vl_1_date:=null
                            end as vl_1_date,



                            
                            
                            case
                                when obs regexp "!!1271=856!!" then @vl_order_date := date(encounter_datetime)
                                when orders regexp "856" then @vl_order_date := date(encounter_datetime)
                                when @prev_id=@cur_id and (@vl_1_date is null or @vl_1_date < @vl_order_date) then @vl_order_date
                                else @vl_order_date := null
                            end as vl_order_date,
                            
                            case
                            
                            WHEN (@cur_arv_meds IS NOT NULL AND @vl_1 > 1000) AND (TIMESTAMPDIFF(DAY, @vl_1_date,date(encounter_datetime)) >= 90) THEN 1

                            WHEN (TIMESTAMPDIFF(MONTH,@arv_start_date,date(encounter_datetime)) <= 12) AND (@vl_1_date IS NULL OR TIMESTAMPDIFF(MONTH,@vl_1_date,date(encounter_datetime)) >= 6) AND (TIMESTAMPDIFF(MONTH,@arv_start_date,date(encounter_datetime)) >= 6) THEN 1

                            WHEN (TIMESTAMPDIFF(MONTH,@arv_start_date, date(encounter_datetime)) >= 12) AND (@vl_1_date IS NULL OR TIMESTAMPDIFF(MONTH, @vl_1_date, date(encounter_datetime)) >= 12) THEN 1

                            ELSE 0
                            
                            
                            end as expected_vl_date,

                            
                            case
                                when obs regexp "!!1271=657!!" then @cd4_order_date := date(encounter_datetime)
                                when orders regexp "657" then @cd4_order_date := date(encounter_datetime)
                                when @prev_id=@cur_id then @cd4_order_date
                                else @cd4_order_date := null
                            end as cd4_order_date,

                                
                            case
                              when obs regexp "!!1271=1030!!" then @hiv_dna_pcr_order_date := date(encounter_datetime)
                              when orders regexp "1030" then @hiv_dna_pcr_order_date := date(encounter_datetime)
                              when @prev_id=@cur_id then @hiv_dna_pcr_order_date
                              else @hiv_dna_pcr_order_date := null
                            end as hiv_dna_pcr_order_date,

                            case
                              when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then encounter_datetime
                              when obs regexp "!!1030=[0-9]"
                                  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30)
                                then replace(replace((substring_index(substring(obs_datetimes,locate("1030=",obs_datetimes)),@sep,1)),"1030=",""),"!!","")
                            end as hiv_dna_pcr_resulted_date,

                            case
                              when @prev_id=@cur_id then
                                case
                                  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0 and date(encounter_datetime)<>@hiv_dna_pcr_1_date then @hiv_dna_pcr_2:= @hiv_dna_pcr_1
                                  when obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0
                                    and abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30 then @hiv_dna_pcr_2 := @hiv_dna_pcr_1
                                  else @hiv_dna_pcr_2
                                end
                              else @hiv_dna_pcr_2:=null
                            end as hiv_dna_pcr_2,

                            case
                              when @prev_id=@cur_id then
                                case
                                  when t1.encounter_type=@lab_encounter_type and obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0 and date(encounter_datetime)<>@hiv_dna_pcr_1_date then @hiv_dna_pcr_2_date:= @hiv_dna_pcr_1_date
                                  when obs regexp "!!1030=[0-9]" and @hiv_dna_pcr_1 >= 0
                                    and abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("1030=",obs_datetimes)),@sep,1)),"1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30 then @hiv_dna_pcr_2_date:= @hiv_dna_pcr_1_date
                                  else @hiv_dna_pcr_2_date
                                end
                              else @hiv_dna_pcr_2_date:=null
                            end as hiv_dna_pcr_2_date,

                            case
                              when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                              when obs regexp "!!1030=[0-9]"
                                and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!",""),@hiv_dna_pcr_1_date)) > 30)
                                then cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                            end as hiv_dna_pcr_resulted,

                            case
                              when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then @hiv_dna_pcr_1:= cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                              when obs regexp "!!1030=[0-9]"
                                  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","") ,@hiv_dna_pcr_1_date)) > 30)
                                then @hiv_dna_pcr_1 := cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                              when @prev_id=@cur_id then @hiv_dna_pcr_1
                              else @hiv_dna_pcr_1:=null
                            end as hiv_dna_pcr_1,


                            case
                              when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then @hiv_dna_pcr_1_date:=date(encounter_datetime)
                              when obs regexp "!!1030=[0-9]"
                                  and (@hiv_dna_pcr_1_date is null or abs(datediff(replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","") ,@hiv_dna_pcr_1_date)) > 30)
                                then @hiv_dna_pcr_1_date := replace(replace((substring_index(substring(obs_datetimes,locate("!!1030=",obs_datetimes)),@sep,1)),"!!1030=",""),"!!","")
                              when @prev_id=@cur_id then @hiv_dna_pcr_1_date
                              else @hiv_dna_pcr_1_date:=null
                            end as hiv_dna_pcr_1_date,

                            
                            case
                              when t1.encounter_type = @lab_encounter_type and obs regexp "!!(1040|1042)=[0-9]" then encounter_datetime
                            end as hiv_rapid_test_resulted_date,

                            case
                              when t1.encounter_type = @lab_encounter_type and obs regexp "!!(1040|1042)=[0-9]" then cast(replace(replace((substring_index(substring(obs,locate("!!(1040|1042)=",obs)),@sep,1)),"!!(1040|1042)=",""),"!!","") as unsigned)
                            end as hiv_rapid_test_resulted,
                            
                            case
                                when obs regexp "!!8302=8305!!" then @condoms_provided_date := encounter_datetime
                                when obs regexp "!!374=(190|6717|6718)!!" then @condoms_provided_date := encounter_datetime
                                when obs regexp "!!6579=" then @condoms_provided_date := encounter_datetime
                                when @prev_id = @cur_id then @condoms_provided_date
                                else @condoms_provided_date := null
                            end as condoms_provided_date,
                                
                            
                            
                            
                            

                            case
                                when obs regexp "!!7240=(5275|6220|780|5279|907|6218|6700|6701|5274|9510|9511|9734|9735|6217)!!"
                                    then @modern_contraceptive_method_start_date := date(encounter_datetime)
                                when obs regexp "!!7240=!!"
                                    then @modern_contraceptive_method_start_date := null
                                when obs regexp "!!374=(5275|6220|780|5279|907|6218|6700|6701|5274|9510|9511|9734|9735|6217)!!" and obs regexp "!!1190="
                                    then @modern_contraceptive_method_start_date :=  date(replace(replace((substring_index(substring(obs,locate("!!1190=",obs)),@sep,1)),"!!1190=",""),"!!",""))
                                when obs regexp "!!374=(5275|6220|780|5279|907|6218|6700|6701|5274|9510|9511|9734|9735|6217)!!"
                                    then @modern_contraceptive_method_start_date :=  date(encounter_datetime)
                                when obs regexp "!!374=!!"
                                    then @modern_contraceptive_method_start_date := null
                                when @prev_id = @cur_id then @modern_contraceptive_method_start_date
                                else @modern_contraceptive_method_start_date := null
                            end as modern_contraceptive_method_start_date,
                            
                            
                            case
                                when obs regexp "!!9738=1066!!" then @contraceptive_method := null
                                when obs regexp "!!7240=1107!!" then @contraceptive_method := null
                                when obs regexp "!!7240="
                                    then @contraceptive_method := replace(replace((substring_index(substring(obs,locate("!!7240=",obs)),@sep,1)),"!!7240=",""),"!!","")
                                when obs regexp "!!374="
                                    then @contraceptive_method :=  replace(replace((substring_index(substring(obs,locate("!!374=",obs)),@sep,1)),"!!374=",""),"!!","")
                                when @prev_id = @cur_id then @contraceptive_method
                                else @contraceptive_method := null
                            end as contraceptive_method,

                            case
                                when obs regexp "!!2061=1115!!" then @menstruation_status := 1115	
                                when obs regexp "!!2061=2060!!" then @menstruation_status := 2060	
                                when obs regexp "!!2061=1116!!" then @menstruation_status := 1116	
                                when obs regexp "!!2061=5990!!" then @menstruation_status := 5990	
                                when obs regexp "!!2061=6496!!" then @menstruation_status := 6496	
                                when obs regexp "!!2061=6497!!" then @menstruation_status := 6497	
                                when obs regexp "!!2061=5993!!" then @menstruation_status := 5993	
                                when obs regexp "!!2061=2416!!" then @menstruation_status := 2416	
                                when obs regexp "!!2061=2415!!" then @menstruation_status := 2415	
                                when obs regexp "!!2061=7023!!" then @menstruation_status := 7023	
                                when obs regexp "!!2061=127!!" then @menstruation_status := 127	
                                when obs regexp "!!2061=162!!" then @menstruation_status := 162	
                                when obs regexp "!!2061=1461!!" then @menstruation_status := 1461	
                                when obs regexp "!!2061=5622!!" then @menstruation_status := 5622	
                                when obs regexp "!!2061=5989!!" then @menstruation_status := 5989	
                                when @prev_id = @cur_id then @menstruation_status	
                                else @menstruation_status := null	
                            end as menstruation_status,
                            case
                                when obs regexp "!!5632=1065!!" then @is_mother_breastfeeding:=1
                                else null
                            end as is_mother_breastfeeding,
                            
                            case
                                when obs regexp "!!5356=(1204|1220)!!" then @cur_who_stage := 1
                                when obs regexp "!!5356=(1205|1221)!!" then @cur_who_stage := 2
                                when obs regexp "!!5356=(1206|1222)!!" then @cur_who_stage := 3
                                when obs regexp "!!5356=(1207|1223)!!" then @cur_who_stage := 4
                                when obs regexp "!!8287=(1204|1220)!!" then @cur_who_stage := 1
                                when obs regexp "!!8287=(1205|1221)!!" then @cur_who_stage := 2
                                when obs regexp "!!8287=(1206|1222)!!" then @cur_who_stage := 3
                                when obs regexp "!!8287=(1207|1223)!!" then @cur_who_stage := 4
                                when obs regexp "!!1224=(1204|1220)!!" then @cur_who_stage := 1
                                when obs regexp "!!1224=(1205|1221)!!" then @cur_who_stage := 2
                                when obs regexp "!!1224=(1206|1222)!!" then @cur_who_stage := 3
                                when obs regexp "!!1224=(1207|1223)!!" then @cur_who_stage := 4
								when obs regexp "!!8307=(1204|1220)!!" then @cur_who_stage := 1
                                when obs regexp "!!8307=(1205|1221)!!" then @cur_who_stage := 2
                                when obs regexp "!!8307=(1206|1222)!!" then @cur_who_stage := 3
                                when obs regexp "!!8307=(1207|1223)!!" then @cur_who_stage := 4
                                when @prev_id = @cur_id then @cur_who_stage
                                else @cur_who_stage := null
                            end as cur_who_stage,
                            
                            
                        case 
                            when obs regexp "!!6096=1065" then @discordant_status := 1
                            when obs regexp "!!6096=1066" then @discordant_status := 2
                            when obs regexp "!!6096=1067" then @discordant_status := 3
                            when obs regexp "!!6096=1175" then @discordant_status := 4
                            when obs regexp "!!6096=6826" then @discordant_status := 5
                            when obs regexp "!!6096=6827" then @discordant_status := 6                                                    
                            when @prev_id = @cur_id then @discordant_status
                            else @discordant_status := null
                        end as discordant_status,
                        
                        
                        case
							when encounter_type=21 then
										case
											when obs regexp "!!9063=1065" AND obs regexp "!!9600=1065" then @phone_outreach:= 1
											when obs regexp "!!1569=1555" AND obs regexp "!!9600=1065" then @phone_outreach:= 1
											when obs regexp "!!1558=1555" AND obs regexp "!!9600=1065" then @phone_outreach:= 1
                                            when obs regexp "!!1558=1555" AND obs regexp "!!1559=1065" then @phone_outreach:= 1
											when obs regexp "!!9063=(1065|1560|9064|9065|9066|5622)" AND obs regexp "!!9600=1066" then @phone_outreach:= 2
											when obs regexp "!!1558=1555" AND obs regexp "!!9600=1066" then @phone_outreach:= 2
                                            when obs regexp "!!1558=1555" AND obs regexp "!!1559=1066" then @phone_outreach:= 2
										else @phone_outreach:= null
                                        end
								else @phone_outreach := null
						end as phone_outreach,
                            case
                                when encounter_type=21 then
										case
                                        when obs regexp "!!10085=1065" AND obs regexp "!!1558=(7066|6116|1556|1557)" AND obs regexp "!!1559=1065" then @home_outreach:= 1
										when obs regexp "!!1558=7066" and obs regexp "!!1559=1065" then @home_outreach := 1
                                        when obs regexp "!!1569=1567"  AND obs regexp "!!1559=1065" then @home_outreach := 1
                                        when obs regexp "!!10085=1065" AND "!!1558=(7066|6116|1556|1557)" AND obs regexp "!!1559=1066" then @home_outreach := 2
                                        when obs regexp "!!1569=1567"  AND obs regexp "!!1559=1066" then @home_outreach := 2
										else @home_outreach:= null
										end
								else @home_outreach := null
                            end as home_outreach,
                            
						 case when encounter_type = 21 then obs regexp 
                           case
								when obs regexp "!!1553=" then @outreach_attempts:= CAST(GetValues(obs,'1553') AS SIGNED)
                                when obs regexp "!!9062=" then @outreach_attempts:= CAST(GetValues(obs,'9062') AS SIGNED)
                           end
                           else @outreach_attempts := null
                         end as outreach_attempts,
                            
                         0 as outreach_missed_visit_reason,

                        case 
                            when obs regexp "!!11237=1065" then @travelled_outside_last_3_months := 1
                            when obs regexp "!!11237=1066" then @travelled_outside_last_3_months := 0 
                            when @prev_id = @cur_id then @travelled_outside_last_3_months                                                
                            else @travelled_outside_last_3_months := null
                        end as travelled_outside_last_3_months,

                        case 
                            when obs regexp "!!11238=1065" then @travelled_outside_last_6_months := 1
                            when obs regexp "!!11238=1066" then @travelled_outside_last_6_months := 0 
                            when @prev_id = @cur_id then @travelled_outside_last_6_months                                                
                            else @travelled_outside_last_6_months := null
                        end as travelled_outside_last_6_months,

                        case 
                            when obs regexp "!!11239=1065" then @travelled_outside_last_12_months := 1
                            when obs regexp "!!11239=1066" then @travelled_outside_last_12_months := 0
                            when @prev_id = @cur_id then @travelled_outside_last_12_months                                                 
                            else @travelled_outside_last_12_months := null
                        end as travelled_outside_last_12_months,

                        case
                            when  t1.encounter_type = 218  then @last_cross_boarder_screening_datetime := encounter_datetime
                            when @prev_id = @cur_id then @last_cross_boarder_screening_datetime
                            else null
                        end as last_cross_boarder_screening_datetime,
						
                        case
                            when  @travelled_outside_last_3_months = 1 or @travelled_outside_last_6_months = 1 or @travelled_outside_last_12_months = 1  then @is_cross_border_country := 1
                            else @is_cross_border_country := 0
                        end as is_cross_border_country,

                        case 
                            when obs regexp "!!11243=10739" then @cross_border_service_offered := 10739
                            when obs regexp "!!11243=10649" then @cross_border_service_offered := 10649
                            when obs regexp "!!11243=5483" then @cross_border_service_offered := 5483
                            when obs regexp "!!11243=2050" then @cross_border_service_offered := 2050
                            when obs regexp "!!11243=11244" then @cross_border_service_offered := 11244
                            when obs regexp "!!11243=1964" then @cross_border_service_offered := 1964
                            when obs regexp "!!11243=7913" then @cross_border_service_offered := 7913
                            when obs regexp "!!11243=5622" then @cross_border_service_offered := 5622                                                  
                            when @prev_id = @cur_id then @cross_border_service_offered
                            else @cross_border_service_offered := null
                        end as cross_border_service_offered,

						case 
                            when obs regexp "!!11252=11197" then @country_of_residence := 11197
                            when obs regexp "!!11252=11118" then @country_of_residence := 11118                                                
                            when @prev_id = @cur_id then @country_of_residence
                            else @country_of_residence := null
                        end as country_of_residence
                            
                        from flat_hiv_summary_0 t1
                            join amrs.person p using (person_id)
                        );
                        
						alter table flat_hiv_summary_1 drop prev_id, drop cur_id, drop cur_clinical_datetime, drop cur_clinic_rtc_date;

                        set @prev_id = -1;
                        set @cur_id = -1;
                        set @prev_clinical_location_id = null;
                        set @cur_clinical_location_id = null;


                        drop table if exists flat_hiv_summary_02;
                        create temporary table flat_hiv_summary_02
                        (select *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := person_id as cur_id,


                            case
                                when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                                else @prev_clinical_location_id := null
                            end as next_clinical_location_id,

                            case
                                when is_clinical_encounter then @cur_clinical_location_id := location_id
                                when @prev_id = @cur_id then @cur_clinical_location_id
                                else @cur_clinical_location_id := null
                            end as cur_clinic_location_id

                            from flat_hiv_summary_1
                            order by person_id, encounter_datetime desc
						);

                        alter table flat_hiv_summary_02 drop prev_id, drop cur_id;
                

                        set @prev_id = -1;
                        set @cur_id = -1;
                        set @prev_encounter_datetime = null;
                        set @cur_encounter_datetime = null;

                        set @prev_clinical_datetime = null;
                        set @cur_clinical_datetime = null;

                        set @next_encounter_type = null;
                        set @cur_encounter_type = null;
                        
                        set @prev_visit_type = null;
                        set @cur_visit_type = null;
                        
                        set @prev_visit_id = null;
                        set @cur_visit_id = null;

                        drop table if exists flat_hiv_summary_2;
                        create temporary table flat_hiv_summary_2
                        (select *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := person_id as cur_id,

                            case
                                when @prev_id = @cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                                else @prev_encounter_datetime := null
                            end as next_encounter_datetime_hiv,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

                            case
                                when @prev_id=@cur_id then @next_encounter_type := @cur_encounter_type
                                else @next_encounter_type := null
                            end as next_encounter_type_hiv,

                            @cur_encounter_type := encounter_type as cur_encounter_type,

                            case
                                when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                                else @prev_clinical_datetime := null
                            end as next_clinical_datetime_hiv,

                            case
                                when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
                                when @prev_id = @cur_id then @cur_clinical_datetime
                                else @cur_clinical_datetime := null
                            end as cur_clinic_datetime,

                            case
                                when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
                                else @prev_clinical_rtc_date := null
                            end as next_clinical_rtc_date_hiv,

                            case
                                when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
                                when @prev_id = @cur_id then @cur_clinical_rtc_date
                                else @cur_clinical_rtc_date:= null
                            end as cur_clinical_rtc_date,

                            case
                                when @prev_id != @cur_id then null
                                when is_clinical_encounter then @outreach_date_bncd
                                else null
                            end as outreach_date_bncd,

                            case
                                when encounter_type=21 and @outreach_date_bncd is null then @outreach_date_bncd := encounter_datetime
                                when is_clinical_encounter then @outreach_date_bncd := null
                                when @prev_id != @cur_id then @outreach_date_bncd := null
                                else @outreach_date_bncd
                            end as next_outreach_date_bncd,

                            case
                                when @prev_id != @cur_id then null
                                when is_clinical_encounter then @outreach_death_date_bncd
                                else null
                            end as outreach_death_date_bncd,

                            case
                                when encounter_type=21 and @outreach_death_date_bncd  is null then @outreach_death_date_bncd  := death_date
                                when is_clinical_encounter then @outreach_death_date_bncd  := null
                                when @prev_id != @cur_id then @outreach_death_date_bncd := null
                                else @outreach_death_date_bncd
                            end as next_outreach_death_date_bncd,


                            case
                                when @prev_id != @cur_id then null
                                when is_clinical_encounter then cast(@outreach_patient_care_status_bncd as unsigned)
                                else null
                            end as outreach_patient_care_status_bncd,

                            case
                                when encounter_type=21 and @outreach_patient_care_status_bncd is null then @outreach_patient_care_status_bncd := patient_care_status
                                when is_clinical_encounter then @outreach_patient_care_status_bncd := null
                                when @prev_id != @cur_id then @outreach_patient_care_status_bncd := null
                                else @outreach_patient_care_status_bncd
                            end as next_outreach_patient_care_status_bncd,

                            case
                                when @prev_id != @cur_id then null
                                when is_clinical_encounter then @transfer_date_bncd
                                else null
                            end as transfer_date_bncd,

                            case
                                when encounter_type=116 and @transfer_date_bncd is null then @transfer_date_bncd := encounter_datetime
                                when is_clinical_encounter then @transfer_date_bncd := null
                                when @prev_id != @cur_id then @transfer_date_bncd := null
                                else @transfer_date_bncd
                            end as next_transfer_date_bncd,

                            case
                                when @prev_id != @cur_id then null
                                when is_clinical_encounter then @transfer_transfer_out_bncd
                                else null
                            end as transfer_transfer_out_bncd,

                            case
                                when encounter_type=116 and @transfer_transfer_out_bncd is null then @transfer_transfer_out_bncd := encounter_datetime
                                when is_clinical_encounter then @transfer_transfer_out_bncd := null
                                when @prev_id != @cur_id then @transfer_transfer_out_bncd := null
                                else @transfer_transfer_out_bncd
                            end as next_transfer_transfer_out_bncd

                            from flat_hiv_summary_02
                            order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
                        );

                        alter table flat_hiv_summary_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;

						set @prev_id = -1;
                        set @cur_id = -1;
                        set @prev_clinical_location_id = null;
                        set @cur_clinical_location_id = null;

                        drop temporary table if exists flat_hiv_summary_03;
                        create temporary table flat_hiv_summary_03 (index person_enc (person_id, encounter_datetime desc))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,

                            case
                                when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                                else @prev_clinical_location_id := null
                            end as prev_clinical_location_id,


                            case
                                when is_clinical_encounter then @cur_clinical_location_id := location_id
                                when @prev_id = @cur_id then @cur_clinical_location_id
                                else @cur_clinical_location_id := null
                            end as cur_clinical_location_id


                            from flat_hiv_summary_2 t1
                            order by person_id, encounter_datetime asc
                        );

                        alter table flat_hiv_summary_03 drop prev_id, drop cur_id;

                        set @prev_id = -1;
                        set @cur_id = -1;
                        set @prev_encounter_type = null;
                        set @cur_encounter_type = null;
                        set @prev_encounter_datetime = null;
                        set @cur_encounter_datetime = null;
                        set @prev_clinical_datetime = null;
                        set @cur_clinical_datetime = null;

                        drop temporary table if exists flat_hiv_summary_3;
                        create temporary table flat_hiv_summary_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,

                            case
                                when @prev_id=@cur_id then @prev_encounter_type := @cur_encounter_type
                                else @prev_encounter_type:=null
                            end as prev_encounter_type_hiv,    
                            @cur_encounter_type := encounter_type as cur_encounter_type,

                            case
                                when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                                else @prev_encounter_datetime := null
                            end as prev_encounter_datetime_hiv,

                            @cur_encounter_datetime := encounter_datetime as cur_encounter_datetime,

/* Moved to flat_hiv_summary_1
                            case
                                when @prev_id = @cur_id then @prev_clinical_datetime := @cur_clinical_datetime
                                else @prev_clinical_datetime := null
                            end as prev_clinical_datetime_hiv,

                            case
                                when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
                                when @prev_id = @cur_id then @cur_clinical_datetime
                                else @cur_clinical_datetime := null
                            end as cur_clinical_datetime,
*/
                            
                            case
                                when @prev_id = @cur_id then @prev_visit_id := @cur_visit_id
                                else @prev_visit_id := null
                            end as prev_visit_id,

                            case
                                when @prev_id = @cur_id then @cur_visit_id := visit_id
                                else @cur_visit_id := null
                            end as cur_visit_id,

                            case
                                when @prev_id = @cur_id and @prev_visit_id != @cur_visit_id  then @prev_visit_type := @cur_visit_type
								when @prev_id = @cur_id and @prev_visit_id = @cur_visit_id then @prev_visit_type
                                else @prev_visit_type := null
                            end as prev_visit_type,

                            case
                                when @prev_id = @cur_id then @cur_visit_type := visit_type
                                else @cur_visit_type := null
                            end as cur_visit_type

/* MOVED to flat_hiv_summary_1
                            case
                                when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
                                else @prev_clinical_rtc_date := null
                            end as prev_clinical_rtc_date_hiv,

                            case
                                when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
                                when @prev_id = @cur_id then @cur_clinical_rtc_date
                                else @cur_clinical_rtc_date:= null
                            end as cur_clinic_rtc_date
*/
                            from flat_hiv_summary_03 t1
                            order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );
                                        
                        alter table flat_hiv_summary_3 drop prev_id, drop cur_id;

                        set @prev_id = null;
                        set @cur_id = null;
                        set @transfer_in = null;
                        set @transfer_in_date = null;
                        set @transfer_in_location_id = null;
                        set @transfer_out = null;
                        set @transfer_out_date = null;
                        set @transfer_out_location_id = null;
                
                        drop temporary table if exists flat_hiv_summary_4;

                        create temporary table flat_hiv_summary_4 ( index person_enc (person_id, encounter_datetime))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                                                    
                            
                            
                            case
                                when obs regexp "!!7015=" then @transfer_in := 1
                                when prev_clinical_location_id != location_id and (visit_type is null or visit_type not in (23,24,33)) and (prev_visit_type is null or prev_visit_type not in (23,24,33)) then @transfer_in := 1
                                else @transfer_in := null
                            end as transfer_in,

                            case 
                                when obs regexp "!!7015=" then @transfer_in_date := date(encounter_datetime)
                                when prev_clinical_location_id != location_id and (visit_type is null or visit_type not in (23,24,33)) and (prev_visit_type is null or prev_visit_type not in (23,24,33))  then @transfer_in_date := date(encounter_datetime)
                                when @cur_id = @prev_id then @transfer_in_date
                                else @transfer_in_date := null
                            end transfer_in_date,
                            
                            case 
                                when obs regexp "!!7015=1287" then @transfer_in_location_id := 9999
                                when prev_clinical_location_id != location_id and (visit_type is null or visit_type not in (23,24,33)) and (prev_visit_type is null or prev_visit_type not in (23,24,33)) then @transfer_in_location_id := cur_clinical_location_id
                                when @cur_id = @prev_id then @transfer_in_location_id
                                else @transfer_in_location_id := null
                            end transfer_in_location_id,


                            case
                                    when obs regexp "!!1285=!!" then @transfer_out := 1
                                    when obs regexp "!!1596=1594!!" then @transfer_out := 1
                                    when obs regexp "!!9082=(1287|1594|9068|9504|1285)!!" then @transfer_out := 1
                                    when next_clinical_location_id != location_id and next_encounter_type_hiv != 186 then @transfer_out := 1
                                    else @transfer_out := null
                            end as transfer_out,

                            case 
                                when obs regexp "!!1285=(1287|9068|2050)!!" and next_clinical_datetime_hiv is null then @transfer_out_location_id := 9999
                                when obs regexp "!!1285=1286!!" and next_clinical_datetime_hiv is null then @transfer_out_location_id := 9998
                                when next_clinical_location_id != location_id and next_encounter_type_hiv != 186 then @transfer_out_location_id := prev_clinical_location_id
                                else @transfer_out_location_id := null
                            end transfer_out_location_id,

                            
                            case 
                                when @transfer_out and next_clinical_datetime_hiv is null then @transfer_out_date := date(cur_rtc_date)
                                when next_clinical_location_id != location_id then @transfer_out_date := date(next_clinical_datetime_hiv)
                                when transfer_transfer_out_bncd then @transfer_out_date := date(transfer_transfer_out_bncd)
                                else @transfer_out_date := null
                            end transfer_out_date

                                                        
                        
                            from flat_hiv_summary_3 t1
                            order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_hiv_summary_4;
                    
SELECT @new_encounter_rows;                    
                    set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;
    
                    
                    
                    SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
                        '(select
                        null,
                        person_id,
                        t1.uuid,
                        visit_id,
                        visit_type,
                        encounter_id,
                        encounter_datetime,
                        encounter_type,
                        is_transit,
                        is_clinical_encounter,
                        location_id,
                        t2.uuid as location_uuid,
                        visit_num,
                        mdt_session_number,
                        enrollment_date,                        
                        enrollment_location_id,
                        ovc_non_enrollment_reason,
                        ovc_non_enrollment_date,                                      
                        hiv_start_date,
                        death_date,
                        scheduled_visit,                        
                        transfer_in,
                        transfer_in_location_id,
                        transfer_in_date,                        
                        transfer_out,
                        transfer_out_location_id,
                        transfer_out_date,
                        patient_care_status,
                        out_of_care,
                        prev_rtc_date,
                        cur_rtc_date as rtc_date,
                        med_pickup_rtc_date,
                        arv_first_regimen,
                        arv_first_regimen_location_id,
                        arv_first_regimen_start_date,
                        arv_first_regimen_start_date_flex,
                        prev_arv_meds,
                        cur_arv_meds,
                        cur_arv_meds_strict,
                        cur_arv_drugs,
                        prev_arv_drugs,
                        arv_start_date,                        
                        arv_start_location_id,
                        prev_arv_start_date,
                        prev_arv_end_date,                                                
                        prev_arv_line,
                        cur_arv_line,
                        cur_arv_line_strict,
                        cur_arv_line_reported,
                        prev_arv_adherence,
                        cur_arv_adherence,
                        hiv_status_disclosed,
                        is_pregnant,
                        edd,
                        tb_screen,
                        tb_screening_result,
                        tb_screening_datetime,
                        on_ipt,
                        ipt_start_date,
                        ipt_stop_date,
                        ipt_completion_date,
                        on_tb_tx,
                        tb_tx_start_date,
                        tb_tx_end_date,  
                        tb_tx_stop_date,
                        pcp_prophylaxis_start_date,
                        condoms_provided_date,
                        modern_contraceptive_method_start_date,
                        contraceptive_method,
                        menstruation_status,
                        is_mother_breastfeeding,
                        cur_who_stage,
                        discordant_status,
                        cd4_resulted,
                        cd4_resulted_date,
                        cd4_1,
                        cd4_1_date,
                        cd4_2,
                        cd4_2_date,
                        cd4_percent_1,
                        cd4_percent_1_date,
                        cd4_percent_2,
                        cd4_percent_2_date,
                        vl_resulted,
                        vl_resulted_date,
                        vl_1,
                        vl_1_date,
                        vl_2,
                        vl_2_date,
                        height,
                        weight,
                        expected_vl_date,
                        vl_order_date,
                        cd4_order_date,
                        hiv_dna_pcr_order_date,
                        hiv_dna_pcr_resulted,
                        hiv_dna_pcr_resulted_date,
                        hiv_dna_pcr_1,
                        hiv_dna_pcr_1_date,
                        hiv_dna_pcr_2,
                        hiv_dna_pcr_2_date,
                        hiv_rapid_test_resulted,
                        hiv_rapid_test_resulted_date,
                        prev_encounter_datetime_hiv,
                        next_encounter_datetime_hiv,
                        prev_encounter_type_hiv,
                        next_encounter_type_hiv,
                        prev_clinical_datetime_hiv,
                        next_clinical_datetime_hiv,
                        prev_clinical_location_id,
                        next_clinical_location_id,
                        prev_clinical_rtc_date_hiv,
                        next_clinical_rtc_date_hiv,
                        outreach_date_bncd,
                        outreach_death_date_bncd,
                        outreach_patient_care_status_bncd,
                        transfer_date_bncd,
                        transfer_transfer_out_bncd,
                        phone_outreach,
                        home_outreach,
						outreach_attempts,
                        outreach_missed_visit_reason,
                        travelled_outside_last_3_months,
                        travelled_outside_last_6_months,
                        travelled_outside_last_12_months,
                        last_cross_boarder_screening_datetime,
                        is_cross_border_country,
                        cross_border_service_offered,
                        country_of_residence
                        
                        from flat_hiv_summary_4 t1
                        join amrs.location t2 using (location_id))');

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    

                    

                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_hiv_summary_build_queue__0 t2 using (person_id);'); 

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
                 #if (@query_type="sync") then
                 insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
                 #end if;
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');

        END$$
DELIMITER ;