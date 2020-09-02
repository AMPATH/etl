DELIMITER $$
CREATE PROCEDURE `generate_flat_hei_summary_v1_0`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
                    set @primary_table := "flat_hei_summary";
                    set @total_rows_written = 0;
                    set @query_type = query_type;
                    
                    set @start = now();
                    set @table_version := "flat_hei_summary_v1.0";

                    set session sort_buffer_size=512000000;

                    set @sep = " ## ";
                    set @lab_encounter_type := 99999;
                    set @death_encounter_type := 31;
                    set @last_date_created := (select max(max_date_created) from etl.flat_obs);
                    set @encounter_types := "(21,67,110,115,168,186)";
                    set @clinical_encounter_types := "(115,186)";
                    
SELECT 'Initializing variables successfull ...';


                    
                    
CREATE TABLE IF NOT EXISTS flat_hei_summary (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    uuid VARCHAR(100),
    gender TEXT,
    birth_date DATE,
    death_date DATE,
    patient_care_status INT,
    rtc_date DATETIME,
    prev_rtc_date DATETIME,
    visit_id INT,
    visit_type INT,
    encounter_id INT,
    encounter_datetime DATETIME,
    encounter_type INT,
    date_enrolled DATE,
    is_clinical_encounter INT,
    location_id INT,
    clinic VARCHAR(100),
    enrollment_location_id INT,
    ovc_non_enrolment_reason INT,
    ovc_non_enrolment_date DATETIME,
    ovc_exit_reason INT,
    ovc_exit_date DATETIME,
    transfer_in TINYINT,
    transfer_in_location_id INT,
    transfer_in_date DATETIME,
    transfer_out TINYINT,
    transfer_out_location_id INT,
    transfer_out_date DATETIME,
    initial_hiv_dna_pcr_order_date DATE,
    hiv_dna_pcr_resulted VARCHAR(100),
	hiv_dna_pcr_date DATETIME,
    hiv_dna_pcr_1 INT,
    hiv_dna_pcr_1_date DATETIME,
    hiv_dna_pcr_2 INT,
    hiv_dna_pcr_2_date DATETIME,
    hiv_dna_pcr_3 INT,
    hiv_dna_pcr_3_date DATETIME,
    hiv_dna_pcr_4 INT,
    hiv_dna_pcr_4_date DATETIME,
    antibody_screen_1 INT,
    antibody_screen_1_date DATETIME,
    antibody_screen_2 INT,
    antibody_screen_2_date DATETIME,
    infant_feeding_method INT,
    mother_alive SMALLINT,
    mother_alive_on_child_enrollment SMALLINT,
    mother_person_id INT,
    mother_death_date DATETIME,
    person_bringing_patient INT,
    hei_outcome SMALLINT,
    vl_resulted INT,
    vl_resulted_date DATETIME,
    vl_1 INT,
    vl_1_date DATETIME,
    vl_2 INT,
    vl_2_date DATETIME,
    vl_order_date DATETIME,
    prev_arv_meds VARCHAR(500),
    cur_arv_meds VARCHAR(500),
    newborn_arv_meds VARCHAR(500),
    prev_clinical_location_id MEDIUMINT,
    next_clinical_location_id MEDIUMINT,
    prev_encounter_datetime_hiv DATETIME,
    next_encounter_datetime_hiv DATETIME,
    prev_clinical_datetime_hiv DATETIME,
    next_clinical_datetime_hiv DATETIME,
    PRIMARY KEY encounter_id (encounter_id),
    INDEX person_date (person_id , encounter_datetime),
    INDEX location_id_rtc_date (location_id),
    INDEX encounter_type (encounter_type),
    INDEX date_created (date_created)
);

SELECT 'created table successfully ...';
                    
                    
                                        
                    if(@query_type="build") then
                            select 'BUILDING..........................................';
                            set @write_table = concat("flat_hei_summary_temp_",queue_number);
                            set @queue_table = concat("flat_hei_summary_build_queue_",queue_number);                                                                    

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  

                            
                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_hei_summary_build_queue limit ', queue_size, ');'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            
                            /*
                            SET @dyn_sql=CONCAT('delete t1 from flat_hei_summary_build_queue t1 join ',@queue_table, ' t2 using (person_id);'); 
                            PREPARE s1 from @dyn_sql; 
                            EXECUTE s1; 
                            DEALLOCATE PREPARE s1;  
                            */

                    end if;
    
					SELECT 'Removing test patients ...';
                    
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
                        
                        
                        drop temporary table if exists flat_hei_summary_build_queue__0;
                        

                        
                        SET @dyn_sql=CONCAT('create temporary table flat_hei_summary_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');'); 
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
                        
                        
SELECT 'creating  flat_hei_summary_0a from flat_obs...';


                        drop temporary table if exists flat_hei_summary_0a;
                        create  temporary table flat_hei_summary_0a
                        (select
                            t1.person_id,
                            t1.visit_id,
                            v.visit_type_id as visit_type,
                            t1.encounter_id,
                            t1.encounter_datetime,
                            t1.encounter_type,
                            t1.location_id,
                            l.name as `clinic`,
                            t1.obs,
                            t1.obs_datetimes,

                            
                            case
                                when t1.encounter_type in (3,4,114,115) then 1
                                else null
                            end as is_clinical_encounter,

                            case
                                when t1.encounter_type in (116) then 20
                                when t1.encounter_type in (3,4,9,114,115,214,220) then 10
                                when t1.encounter_type in (129) then 5 
                                else 1
                            end as encounter_type_sort_index,

                            t2.orders
                            from etl.flat_obs t1
                                join flat_hei_summary_build_queue__0 t0 using (person_id)
                                join amrs.location l using (location_id)
                                left join etl.flat_orders t2 using(encounter_id)
                                left join amrs.visit v on (v.visit_id = t1.visit_id)
                            where t1.encounter_type in (21,67,110,114,115,168,186,214,220)
                            AND  (v.visit_type_id IS NULL OR v.visit_type_id IN (25,33,47,53))
                            AND NOT obs regexp "!!5303=703!!"
                        );
                        
                        SELECT 'creating  flat_hei_summary_0a from flat_lab_obs...';

                        insert into flat_hei_summary_0a
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
                            null
                            from etl.flat_lab_obs t1
                                join flat_hei_summary_build_queue__0 t0 using (person_id)
                        );

                        drop temporary table if exists flat_hei_summary_0;
                        create temporary table if not exists flat_hei_summary_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
                        (select * from flat_hei_summary_0a
                        order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );


                        set @prev_id = -1;
                        set @cur_id = -1;
						set @prev_encounter_date = null;
                        set @cur_encounter_date = null;
						set @cur_rtc_date = null;
                        set @prev_rtc_date = null;
                        set @cur_location = null;
                        set @cur_clinic = null;
                        set @enrollment_location_id = null;
                        set @initial_hiv_dna_pcr_order_date = null;
                        set @hiv_dna_pcr_resulted := null;
                        set @initial_pcr_8wks := null;
                        set @initial_pcr_8wks_12months := null;
                        set @date_enrolled := null;
                        set @death_date:= null;
                        set @patient_care_status := null;
                        set @infant_feeding_method := null;
                        set @infant_feeding_6months := null;
                        set @infant_feeding_7_12_months := null;
                        set @infant_feeding_13_18_months := null;
                        set @mother_alive := null;
                        set @mother_alive_on_child_enrollment:=null;
                        set @person_bringing_patient := null;
                        set @hei_outcome := null;
                        
						set @vl_1=null;
                        set @vl_2=null;
                        set @vl_1_date=null;
                        set @vl_2_date=null;
                        set @vl_resulted=null;
                        set @vl_resulted_date=null;
                        
                        set @hiv_dna_pcr_1=null;
                        set @hiv_dna_pcr_2=null;
                        set @hiv_dna_pcr_1_date=null;
                        set @hiv_dna_pcr_2_date=null;
                        set @hiv_dna_pcr_3=null;
                        set @hiv_dna_pcr_3_date=null;
                        set @hiv_dna_pcr_4=null;
                        set @hiv_dna_pcr_4_date=null;
                        
                        set @antibody_screen_1= null;
                        set @antibody_screen_1_date = null;
                        
                        set @prev_arv_meds = null;
                        set @cur_arv_meds = null;
                        set @newborn_arv_meds = null;
                   



                        drop temporary table if exists flat_hei_summary_1;
                        create temporary table flat_hei_summary_1 (index encounter_id (encounter_id))
                        (select
                            obs,
                            encounter_type_sort_index,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            t1.person_id,
                            t1.visit_type,
                            p.uuid,
                            p.gender,
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
                            p.birthDate as  birth_date,
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
                                when @prev_id=@cur_id then @prev_rtc_date := @cur_rtc_date
                                else @prev_rtc_date := null
                            end as prev_rtc_date,
                            case
                                when obs regexp "!!5096=" then @cur_rtc_date := replace(replace((substring_index(substring(obs,locate("!!5096=",obs)),@sep,1)),"!!5096=",""),"!!","")
                                when @prev_id = @cur_id then if(@cur_rtc_date > encounter_datetime,@cur_rtc_date,null)
                                else @cur_rtc_date := null
                            end as cur_rtc_date,
                            t1.visit_id,
                            t1.encounter_id,
                            @prev_encounter_date := date(@cur_encounter_date) as prev_encounter_date,
                            @cur_encounter_date := date(encounter_datetime) as cur_encounter_date,
                            t1.encounter_datetime,                            
                            t1.encounter_type,
                            case
                               when obs regexp "!!1839=" AND obs regexp "!!1839=7850!!" then @date_enrolled:= GetValues(obs,'7013')
                               when obs regexp "!!1839=" AND NOT obs regexp "!!1839=7850!!" AND @date_enrolled is NULL then @date_enrolled := date(encounter_datetime)
                               when NOT obs regexp "!!1839=" AND @date_enrolled IS NULL then @date_enrolled := date(encounter_datetime)
                               else @date_enrolled
                            end as date_enrolled,
                            t1.is_clinical_encounter,                                                    
                            case
                                when location_id then @cur_location := location_id
                                when @prev_id = @cur_id then @cur_location
                                else null
                            end as location_id,
                            t1.clinic,
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
                            when obs regexp "!!11219=6834" then @ovc_non_enrolment_reason := 6834
                            when obs regexp "!!11219=1504" then @ovc_non_enrolment_reason := 1504
                            when @prev_id = @cur_id then @ovc_non_enrolment_reason
                            else @ovc_non_enrolment_reason := null
                        end as ovc_non_enrolment_reason,

                         case
                            when  t1.encounter_type = 214  then @ovc_non_enrolment_date := encounter_datetime
                            when @prev_id = @cur_id then @ovc_non_enrolment_date
                            else null
                        end as ovc_non_enrolment_date,

                        case 
                            when obs regexp "!!1596=8204" then @ovc_exit_reason := 8204
                            when obs regexp "!!1596=11292" then @ovc_exit_reason := 11292  
                            when obs regexp "!!1596=10119" then @ovc_exit_reason := 10119
                            when obs regexp "!!1596=8640" then @ovc_exit_reason := 8640                                                 
                            when @prev_id = @cur_id then @ovc_exit_reason
                            else @ovc_exit_reason := null
                        end as ovc_exit_reason,

                        case
                            when  t1.encounter_type = 220  then @ovc_exit_date := encounter_datetime
                            when @prev_id = @cur_id then @ovc_exit_date
                            else null
                        end as ovc_exit_date,

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
                            case
							when @initial_hiv_dna_pcr_order_date IS NULL then
                               case
								  when obs regexp "!!1271=1030!!" then @initial_hiv_dna_pcr_order_date := date(encounter_datetime)
								  when obs regexp "!!1030=" then @initial_hiv_dna_pcr_order_date := date(encounter_datetime)
								  when orders regexp "1030" then @initial_hiv_dna_pcr_order_date := date(encounter_datetime)
								  when @prev_id=@cur_id then @initial_hiv_dna_pcr_order_date
								  else @initial_hiv_dna_pcr_order_date := null
							   end
							else @initial_hiv_dna_pcr_order_date
                            end as initial_hiv_dna_pcr_order_date,
                            case
                              when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                              else @hiv_dna_pcr_resulted := null
                            end as hiv_dna_pcr_resulted,
                            
                            case
								  when obs regexp "!!1271=1030!!" then @hiv_dna_pcr_date := date(encounter_datetime)
								  when obs regexp "!!1030=" then @hiv_dna_pcr_date := date(encounter_datetime)
								  when orders regexp "1030" then @hiv_dna_pcr_date := date(encounter_datetime)
								  when @prev_id=@cur_id then @hiv_dna_pcr_date
								  else @hiv_dna_pcr_date := null
                            end as hiv_dna_pcr_date,
                            
                            case
                               when @prev_id=@cur_id then 
                                case
								  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_4  IS NULL AND @hiv_dna_pcr_3 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_3_date) > 30 then @hiv_dna_pcr_4:=cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
								  else @hiv_dna_pcr_4
								end
							  when @prev_id!=@cur_id then
                                case
                                   when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_3 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_3_date) > 30 then @hiv_dna_pcr_4:=cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                                   else @hiv_dna_pcr_4:= null
                                end
                            end as hiv_dna_pcr_4,
                            case
                               when @prev_id=@cur_id then 
                                case
								  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_4_date  IS NULL AND @hiv_dna_pcr_3 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_3_date) > 30 then @hiv_dna_pcr_4_date:= date(encounter_datetime)
								  else @hiv_dna_pcr_4_date
								end
							  when @prev_id!=@cur_id then
                                case
                                   when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_3 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_3_date) > 30 then @hiv_dna_pcr_4_date:= date(encounter_datetime)
                                   else @hiv_dna_pcr_4_date:= null
                                end
                            end as hiv_dna_pcr_4_date,
                             case
                               when @prev_id=@cur_id then 
                                case
								  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_3  IS NULL AND @hiv_dna_pcr_2 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_2_date) > 30 then @hiv_dna_pcr_3:=cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
								  else @hiv_dna_pcr_3
								end
							  when @prev_id!=@cur_id then
                                case
                                   when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_2 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_2_date) > 30 then @hiv_dna_pcr_3:=cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                                   else @hiv_dna_pcr_3:= null
                                end
                            end as hiv_dna_pcr_3,
                            case
                               when @prev_id=@cur_id then 
                                case
								  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_3_date  IS NULL AND @hiv_dna_pcr_2 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_2_date) > 30 then @hiv_dna_pcr_3_date:= date(encounter_datetime)
								  else @hiv_dna_pcr_3_date
								end
							  when @prev_id!=@cur_id then
                                case
                                   when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_2 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_2_date) > 30 then @hiv_dna_pcr_3_date:= date(encounter_datetime)
                                   else @hiv_dna_pcr_3_date:= null
                                end
                            end as hiv_dna_pcr_3_date,
                            
                              case
                               when @prev_id=@cur_id then 
                                case
								  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_2  IS NULL AND @hiv_dna_pcr_1 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_1_date) > 30 then @hiv_dna_pcr_2:=cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
								  else @hiv_dna_pcr_2
								end
							  when @prev_id!=@cur_id then
                                case
                                   when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_1 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_1_date) > 30 then @hiv_dna_pcr_2:=cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                                   else @hiv_dna_pcr_2:= null
                                end
                            end as hiv_dna_pcr_2,
                            
                         
                            
                            case
                               when @prev_id=@cur_id then 
                                case
								  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_2_date  IS NULL AND @hiv_dna_pcr_1 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_1_date) > 30 then @hiv_dna_pcr_2_date:= date(encounter_datetime)
								  else @hiv_dna_pcr_2_date
								end
							  when @prev_id!=@cur_id then
                                case
                                   when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_1 > 0 AND datediff(encounter_datetime,@hiv_dna_pcr_1_date) > 30 then @hiv_dna_pcr_2_date:= date(encounter_datetime)
                                   else @hiv_dna_pcr_2_date:= null
                                end
                            end as hiv_dna_pcr_2_date,
                            
                            case
                               when @prev_id=@cur_id then 
                                case
								  when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_1  IS NULL  then @hiv_dna_pcr_1:= cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
								  else @hiv_dna_pcr_1
								end
							  when @prev_id!=@cur_id then
                                case
                                   when t1.encounter_type = @lab_encounter_type and obs regexp "!!1030=[0-9]" then @hiv_dna_pcr_1:= cast(replace(replace((substring_index(substring(obs,locate("!!1030=",obs)),@sep,1)),"!!1030=",""),"!!","") as unsigned)
                                   else @hiv_dna_pcr_1:= null
                                end
                            end as hiv_dna_pcr_1,


                            case
                               when @prev_id=@cur_id then 
                                case
								  when obs regexp "!!1030=[0-9]" AND @hiv_dna_pcr_1_date  IS NULL  then @hiv_dna_pcr_1_date:= date(encounter_datetime)
								  else @hiv_dna_pcr_1_date
								end
							  when @prev_id!=@cur_id then
                                case
                                   when obs regexp "!!1030=[0-9]" then @hiv_dna_pcr_1_date:= date(encounter_datetime)
                                   else @hiv_dna_pcr_1_date:= null
                                end
                            end as hiv_dna_pcr_1_date,
                            
                             case
                              when obs regexp "!!6342=[0-9]" AND @antibody_screen_1 > 0 AND @antibody_screen_2 IS NULL then @antibody_screen_2 := cast(replace(replace((substring_index(substring(obs,locate("!!6342=",obs)),@sep,1)),"!!6342=",""),"!!","") as unsigned)
                              when @prev_id=@cur_id then @antibody_screen_2
                              else @antibody_screen_2:=null
                            end as antibody_screen_2,
						    case
                              when obs regexp "!!6342=[0-9]" AND @antibody_screen_1 > 0 AND @antibody_screen_2_date IS NULL then @antibody_screen_2_date := date(encounter_datetime)
                              when @prev_id=@cur_id then @antibody_screen_2_date
                              else @antibody_screen_2_date:=null
                            end as antibody_screen_2_date,
                            
                            case
                              when obs regexp "!!6342=" AND @antibody_screen_1 IS NULL then @antibody_screen_1 := cast(replace(replace((substring_index(substring(obs,locate("!!6342=",obs)),@sep,1)),"!!6342=",""),"!!","") as unsigned)
                              when @prev_id=@cur_id then @antibody_screen_1
                              else @antibody_screen_1:=null
                            end as antibody_screen_1,
						    case
                              when obs regexp "!!6342=" AND @antibody_screen_1_date IS NULL then @antibody_screen_1_date := date(encounter_datetime)
                              when @prev_id=@cur_id then @antibody_screen_1_date
                              else @antibody_screen_1_date:=null
                            end as antibody_screen_1_date,
						
                            case 
                              when  obs regexp "!!1151=1173" then @infant_feeding_method:=1
                              when  obs regexp "!!1151=1152" then @infant_feeding_method:= 2
                              when  obs regexp "!!1151=5254"  then @infant_feeding_method:= 3
                              when  obs regexp "!!1151=1150" then @infant_feeding_method:= 4
                              when  obs regexp "!!1151=6046" then @infant_feeding_method:=5
                              when  obs regexp "!!1151=5526" then @infant_feeding_method:= 6
                              when  obs regexp "!!1151=968"  then @infant_feeding_method:= 7
                              when  obs regexp "!!1151=1401" then @infant_feeding_method:= 8
                              when  obs regexp "!!1151=1402" then @infant_feeding_method:= 9
                              when  obs regexp "!!1151=1403" then @infant_feeding_method:= 10
                              when  obs regexp "!!1151=1404" then @infant_feeding_method:= 11
                              when  obs regexp "!!1151=1405" then @infant_feeding_method:= 12
                              when  obs regexp "!!1151=6820" then @infant_feeding_method:= 13
                              when  obs regexp "!!1151=5622" then @infant_feeding_method:= 14
                              when  obs regexp "!!1151=6985" then @infant_feeding_method:= 15
                              when  obs regexp "!!1151=7370" then @infant_feeding_method:= 16
                              when  obs regexp "!!1151=6945" then @infant_feeding_method:= 17
                              when  obs regexp "!!1151=8240" then @infant_feeding_method:= 18
							  else  @infant_feeding_method
                            
                            end as infant_feeding_method,
                            
                            case 

                              when  obs regexp "!!9601=1065" then @mother_alive:= 1
                              when  obs regexp "!!969=970" then @mother_alive:= 1
                              when  obs regexp "!!2000=1066" then @mother_alive:= 1
                              when  obs regexp "!!9601=1066" then @mother_alive:= 2
                              when  obs regexp "!!2000=1065" then @mother_alive:= 2
                              when  @prev_id=@cur_id then @mother_alive
							  else  @mother_alive:= null
                            
                            end as mother_alive,
                            case
								when @prev_id=@cur_id AND @mother_alive_on_child_enrollment IS NULL then
                                     case
									   when  obs regexp "!!9601=1065" then @mother_alive_on_child_enrollment:= 1
									   when  obs regexp "!!969=970" then @mother_alive_on_child_enrollment:= 1
									   when  obs regexp "!!2000=1066" then @mother_alive_on_child_enrollment:= 1
									   when  obs regexp "!!9601=1066" then @mother_alive_on_child_enrollment:= 2
									   when  obs regexp "!!2000=1065" then @mother_alive_on_child_enrollment:= 2
									   else @mother_alive_on_child_enrollment
									end
								when @prev_id!=@cur_id  then
                                   case
                                      when  obs regexp "!!9601=1065" then @mother_alive_on_child_enrollment:= 1
									   when  obs regexp "!!969=970" then @mother_alive_on_child_enrollment:= 1
									   when  obs regexp "!!2000=1066" then @mother_alive_on_child_enrollment:= 1
									   when  obs regexp "!!9601=1066" then @mother_alive_on_child_enrollment:= 2
									   when  obs regexp "!!2000=1065" then @mother_alive_on_child_enrollment:= 2
									   else @mother_alive_on_child_enrollment:=null
                                   end
								else @mother_alive_on_child_enrollment
                            end as mother_alive_on_child_enrollment,
                            
                             mother.person_id as mother_person_id,
                             mother.death_date as mother_death_date,
                            
                             case 
                              when  obs regexp "!!969=" then @person_bringing_patient:= GetValues(obs,'969')
							  else  @person_bringing_patient
                            
                            end as person_bringing_patient,
                            
                            case
								when  obs regexp "!!8586=1594" then @hei_outcome:= 1
								when  obs regexp "!!8586=5240" then @hei_outcome:= 2
								when  obs regexp "!!8586=1593" then @hei_outcome:= 3
								when  obs regexp "!!8586=5622" then @hei_outcome:= 4
								when  obs regexp "!!8586=8585" then @hei_outcome:= 5
								when  obs regexp "!!8586=2240" then @hei_outcome:= 6
                                when  @prev_id=@cur_id then @hei_outcome
							  else  @hei_outcome:= null
                            
                            end as hei_outcome,
                            
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
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_date_resulted := date(encounter_datetime)
                                when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_date_resulted
                            end as vl_resulted_date,

                            case
                                when t1.encounter_type = @lab_encounter_type and obs regexp "!!856=[0-9]" then @vl_resulted := cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
                                when @prev_id = @cur_id and date(encounter_datetime) = @vl_date_resulted then @vl_resulted
                            end as vl_resulted,

                            case
                                    when obs regexp "!!856=[0-9]" and t1.encounter_type = @lab_encounter_type then @vl_1:=cast(replace(replace((substring_index(substring(obs,locate("!!856=",obs)),@sep,1)),"!!856=",""),"!!","") as unsigned)
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
                            
                            #1187 : New born ARV use
                            case
                                when obs regexp "!!1187=(1107|1267)!!" then @newborn_arv_meds := null
                                when obs regexp "!!1187=" then @newborn_arv_meds := normalize_pmtct_arvs(obs,'1187')
                                when @prev_id = @cur_id then @newborn_arv_meds
                                else @newborn_arv_meds:= null
                            end as newborn_arv_meds

                        from flat_hei_summary_0 t1
                            join amrs.person p using (person_id)
                            LEFT JOIN amrs.relationship `r` on (r.person_b = p.person_id AND r.relationship = 2)
                            LEFT JOIN amrs.person `mother` on (mother.person_id = r.person_a AND mother.gender = 'F')
                        );
                        
                

                        set @prev_id = null;
                        set @cur_id = null;
                        set @prev_encounter_datetime = null;
                        set @cur_encounter_datetime = null;

                        set @prev_clinical_location_id = null;
                        set @cur_clinical_location_id = null;


                        alter table flat_hei_summary_1 drop prev_id, drop cur_id, drop cur_clinical_datetime, drop cur_clinic_rtc_date;

                        drop temporary table if exists flat_hei_summary_2;
                        create temporary table flat_hei_summary_2
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
                                when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                                else @prev_clinical_location_id := null
                            end as next_clinical_location_id,

                            case
                                when is_clinical_encounter then @cur_clinical_datetime := encounter_datetime
                                when @prev_id = @cur_id then @cur_clinical_datetime
                                else @cur_clinical_datetime := null
                            end as cur_clinic_datetime,

                            case
                                when is_clinical_encounter then @cur_clinical_location_id := location_id
                                when @prev_id = @cur_id then @cur_clinical_location_id
                                else @cur_clinical_location_id := null
                            end as cur_clinic_location_id,

                            case
                                when @prev_id = @cur_id then @prev_clinical_rtc_date := @cur_clinical_rtc_date
                                else @prev_clinical_rtc_date := null
                            end as next_clinical_rtc_date_hiv,

                            case
                                when is_clinical_encounter then @cur_clinical_rtc_date := cur_rtc_date
                                when @prev_id = @cur_id then @cur_clinical_rtc_date
                                else @cur_clinical_rtc_date:= null
                            end as cur_clinical_rtc_date

                            from flat_hei_summary_1
                            order by person_id, date(encounter_datetime) desc, encounter_type_sort_index desc
                        );

                        alter table flat_hei_summary_2 drop prev_id, drop cur_id, drop cur_encounter_type, drop cur_encounter_datetime, drop cur_clinical_rtc_date;


                        set @prev_id = null;
                        set @cur_id = null;
                        set @prev_encounter_type = null;
                        set @cur_encounter_type = null;
                        set @next_encounter_type = null;
                        set @prev_clinical_location_id = null;
                        set @cur_clinical_location_id = null;

                        drop  temporary table if exists flat_hei_summary_3;
                        create  temporary table flat_hei_summary_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            
                             case
                                when @prev_id=@cur_id then @prev_encounter_datetime := @cur_encounter_datetime
                                else @prev_encounter_datetime := null
                            end as prev_encounter_datetime_hiv,
                            
                            case
                                when @prev_id = @cur_id then @prev_clinical_location_id := @cur_clinical_location_id
                                else @prev_clinical_location_id := null
                            end as prev_clinical_location_id,


                            case
                                when is_clinical_encounter then @cur_clinical_location_id := location_id
                                when @prev_id = @cur_id then @cur_clinical_location_id
                                else @cur_clinical_location_id := null
                            end as cur_clinical_location_id

                            from flat_hei_summary_2 t1
                            order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );
                                        
                        alter table flat_hei_summary_3 drop prev_id, drop cur_id;

                        set @prev_id = null;
                        set @cur_id = null;
                        set @transfer_in = null;
                        set @transfer_in_date = null;
                        set @transfer_in_location_id = null;
                        set @transfer_out = null;
                        set @transfer_out_date = null;
                        set @transfer_out_location_id = null;
                        
                        #Handle transfers
                
                        drop temporary table if exists flat_hei_summary_4;

                        create temporary table flat_hei_summary_4 ( index person_enc (person_id, encounter_datetime))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id,
                            case
                                when obs regexp "!!7015=" then @transfer_in := 1
                                when prev_clinical_location_id != location_id then @transfer_in := 1
                                else @transfer_in := null
                            end as transfer_in,

                            case 
                                when obs regexp "!!7015=" then @transfer_in_date := date(encounter_datetime)
                                when prev_clinical_location_id != location_id then @transfer_in_date := date(encounter_datetime)
                                when @cur_id = @prev_id then @transfer_in_date
                                else @transfer_in_date := null
                            end transfer_in_date,
                            
                            case 
                                when obs regexp "!!7015=1287" then @transfer_in_location_id := 9999
                                when prev_clinical_location_id != location_id then @transfer_in_location_id := prev_clinical_location_id
                                when @cur_id = @prev_id then @transfer_in_location_id
                                else @transfer_in_location_id := null
                            end transfer_in_location_id,
                            
                            case
                                    when obs regexp "!!1285=" then @transfer_out := 1
                                    when obs regexp "!!1596=1594!!" then @transfer_out := 1
                                    when obs regexp "!!9082=(1287|1594|9068|9504|1285)!!" then @transfer_out := 1
                                    when next_clinical_location_id != location_id then @transfer_out := 1
                                    else @transfer_out := null
                            end as transfer_out,

                            case 
                                when obs regexp "!!1285=(1287|9068|2050)!!" and next_clinical_datetime_hiv is null then @transfer_out_location_id := 9999
                                when obs regexp "!!1285=1286!!" and next_clinical_datetime_hiv is null then @transfer_out_location_id := 9998
                                when next_clinical_location_id != location_id then @transfer_out_location_id := next_clinical_location_id
                                else @transfer_out_location_id := null
                            end transfer_out_location_id,

                            
                            case 
                                when @transfer_out and next_clinical_datetime_hiv is null then @transfer_out_date := date(IF(cur_rtc_date IS NOT NULL,cur_rtc_date,encounter_datetime))
                                when next_clinical_location_id != location_id then @transfer_out_date := date(next_clinical_datetime_hiv)
                                else @transfer_out_date := null
                            end transfer_out_date
                                                    
                            
                          
                            from flat_hei_summary_3 t1
                            order by person_id, date(encounter_datetime), encounter_type_sort_index
                        );


SELECT 
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_hei_summary_4;
                    
SELECT @new_encounter_rows;                    
                    set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;
    
                    
                    
                    SET @dyn_sql=CONCAT('replace into ',@write_table,                                              
                        '(select
                        null,
                        person_id,
                        t1.uuid,
                        gender,
                        birth_date,
                        death_date,
                        patient_care_status,
						cur_rtc_date as rtc_date,
                        prev_rtc_date,
                        visit_id,
                        visit_type,
                        encounter_id,
                        encounter_datetime,
                        encounter_type,
                        date_enrolled,
                        is_clinical_encounter,
                        location_id,
                        clinic,
                        enrollment_location_id,
                        ovc_non_enrolment_reason,
                        ovc_non_enrolment_date,
                        ovc_exit_reason,
                        ovc_exit_date,
                        transfer_in,
                        transfer_in_location_id,
                        transfer_in_date,
                        transfer_out,
                        transfer_out_location_id,
                        transfer_out_date,
						initial_hiv_dna_pcr_order_date,
                        hiv_dna_pcr_resulted,
                        hiv_dna_pcr_date,
                        hiv_dna_pcr_1,
                        hiv_dna_pcr_1_date,
                        hiv_dna_pcr_2,
                        hiv_dna_pcr_2_date,
                        hiv_dna_pcr_3,
                        hiv_dna_pcr_3_date,
                        hiv_dna_pcr_4,
                        hiv_dna_pcr_4_date,
                        antibody_screen_1,
                        antibody_screen_1_date,
                        antibody_screen_2,
                        antibody_screen_2_date,
                        infant_feeding_method,
                        mother_alive,
                        mother_alive_on_child_enrollment,
                        mother_person_id,
                        mother_death_date,
                        person_bringing_patient,
                        hei_outcome,
						vl_resulted,
                        vl_resulted_date,
                        vl_1,
                        vl_1_date,
                        vl_2,
                        vl_2_date,
                        vl_order_date,
                        prev_arv_meds,
                        cur_arv_meds,
                        newborn_arv_meds,
                        prev_clinical_location_id,
						next_clinical_location_id,
                        prev_encounter_datetime_hiv,
                        next_encounter_datetime_hiv,
                        prev_clinical_datetime_hiv,
	                    next_clinical_datetime_hiv
                        from flat_hei_summary_4 t1)');

                    PREPARE s1 from @dyn_sql; 
                    EXECUTE s1; 
                    DEALLOCATE PREPARE s1;  
                    

                    

                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_hei_summary_build_queue__0 t2 using (person_id);'); 

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
                        
						SET @dyn_sql=CONCAT('describe ',@write_table,';');
                        PREPARE s1 from @dyn_sql; 
                        EXECUTE s1; 
                        DEALLOCATE PREPARE s1;
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
SELECT 
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');



END$$
DELIMITER ;