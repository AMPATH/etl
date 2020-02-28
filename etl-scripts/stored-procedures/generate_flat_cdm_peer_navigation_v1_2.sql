CREATE DEFINER=`etl_user`@`%` PROCEDURE `generate_flat_cdm_peer_navigation_v1_2`(IN query_type varchar(50), IN queue_number int, IN queue_size int, IN cycle_size int)
BEGIN
                    set @primary_table := "flat_cdm_peer_navigation";
                    set @total_rows_written = 0;
                    set @query_type = query_type;
                    set @clinical_encounter_types = "(-1)";


                    set @start = now();
                    set @table_version := "flat_cdm_peer_navigation_v1.1";

                    set session sort_buffer_size=512000000;

                    set @sep = " ## ";
                    set @boundary = "!!";
                    set @lab_encounter_type := 99999;
                    set @death_encounter_type := 31;
                    set @last_date_created := (select max(max_date_created) from etl.flat_obs);
                    set @encounter_types = "(193,192)";
SELECT 'variables initialized ...';

SET @dyn_sql=CONCAT('drop table ',@primary_table,';');
PREPARE s1 from @dyn_sql;
EXECUTE s1;
DEALLOCATE PREPARE s1;



CREATE TABLE IF NOT EXISTS flat_cdm_peer_navigation (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    person_id INT,
    encounter_datetime TIMESTAMP,
    encounter_id INT,
    encounter_type INT,
    location_id INT,
    referal_urgency SMALLINT,
    referred_in_or_out SMALLINT,
    initial_or_follow_up SMALLINT,
	are_you_the_referring_or_receiving_peer SMALLINT,
    patient_able_to_be_contacted SMALLINT,
    lifestyle_modification_plan SMALLINT,
    current_medication_regimen SMALLINT,
    understand_bp_meds_and_purpose SMALLINT,
    understand_meds_frequency SMALLINT,
    last_hypersenitive_regimen_change SMALLINT,
    missed_bp_meds_7days SMALLINT,
    reason_meds_missed INT,
    reason_for_referal INT,
    barries_to_complete_referral INT,
    concerns_preventing_referral INT,
    patient_referal_status INT,
    is_referal_completed INT,
    referal_preparation_discussed INT,
    next_appointment_facility INT,
    next_peer_outreach INT
);

SELECT 'table created  ...';

                    if(@query_type="build") then
                            select 'BUILDING..........................................';

                            set @write_table = concat("flat_cdm_peer_navigation_temp_",queue_number);
                            set @queue_table = concat("flat_cdm_peer_navigation_build_queue_",queue_number);

                            SET @dyn_sql=CONCAT('Create table if not exists ',@write_table,' like ',@primary_table);
                            PREPARE s1 from @dyn_sql;
                            EXECUTE s1;
                            DEALLOCATE PREPARE s1;


                            SET @dyn_sql=CONCAT('Create table if not exists ',@queue_table,' (select * from flat_cdm_peer_navigation_build_queue limit ', queue_size, ');');
                            PREPARE s1 from @dyn_sql;
                            EXECUTE s1;
                            DEALLOCATE PREPARE s1;


                            /*SET @dyn_sql=CONCAT('delete t1 from flat_cdm_peer_navigation_build_queue t1 join ',@queue_table, ' t2 using (person_id);');
                            PREPARE s1 from @dyn_sql;
                            EXECUTE s1;
                            DEALLOCATE PREPARE s1; */


                    end if;

		if (@query_type="sync") then
							select 'SYNCING..........................................';
							set @write_table = "flat_cdm_peer_navigation";
							set @queue_table = "flat_cdm_peer_navigation_test_sync_queue";
CREATE TABLE IF NOT EXISTS flat_cdm_peer_navigation_test_sync_queue (
    person_id INT
);

set @last_update = null;

SELECT
    MAX(date_updated)
INTO @last_update FROM
    etl.flat_log
WHERE
    table_name = @table_version;


							replace into flat_cdm_peer_navigation_test_sync_queue
							(select distinct patient_id
								from amrs.encounter
								where date_changed > @last_update
							);

							replace into flat_cdm_peer_navigation_test_sync_queue
							(select distinct person_id
								from etl.flat_obs
								where max_date_created > @last_update
							);

							replace into flat_cdm_peer_navigation_test_sync_queue
							(select distinct person_id
								from etl.flat_lab_obs
								where max_date_created > @last_update
							);

							replace into flat_cdm_peer_navigation_test_sync_queue
							(select distinct person_id
								from etl.flat_orders
								where max_date_created > @last_update
							);

                            replace into flat_cdm_peer_navigation_test_sync_queue
                            (select person_id from
								amrs.person
								where date_voided > @last_update);


                            replace into flat_cdm_peer_navigation_test_sync_queue
                            (select person_id from
								amrs.person
								where date_changed > @last_update);

					  end if;



					SELECT 'Working on test patients ...';



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


                        drop temporary table if exists flat_cdm_peer_navigation_build_queue__0;



                        SET @dyn_sql=CONCAT('create temporary table flat_cdm_peer_navigation_build_queue__0 (person_id int primary key) (select * from ',@queue_table,' limit ',cycle_size,');');
                        PREPARE s1 from @dyn_sql;
                        EXECUTE s1;
                        DEALLOCATE PREPARE s1;




                        drop temporary table if exists flat_cdm_peer_navigation_0a;
                        create temporary table flat_cdm_peer_navigation_0a
                        (select
                            t1.person_id,
                            t1.obs,
                            t1.visit_id,
                            t1.encounter_id,
                            t1.encounter_datetime,
                            t1.encounter_type,
                            t1.location_id
                            from etl.flat_obs t1
                                join flat_cdm_peer_navigation_build_queue__0 t0 using (person_id)
                            where t1.encounter_type in (193,192)
                        );

                        insert into flat_cdm_peer_navigation_0a
                        (select
                            t1.person_id,
                            t1.obs,
                            null,
                            t1.encounter_id,
                            t1.test_datetime,
                            t1.encounter_type,
                            t1.location_id
                            from etl.flat_lab_obs t1
                                join flat_cdm_peer_navigation_build_queue__0 t0 using (person_id)
                        );

                        drop temporary table if exists flat_cdm_peer_navigation_0;
                        create temporary table flat_cdm_peer_navigation_0(index encounter_id (encounter_id), index person_enc (person_id,encounter_datetime))
                        (select * from flat_cdm_peer_navigation_0a
                        order by person_id, date(encounter_datetime)
                        );


                        set @prev_id = null;
                        set @cur_id = null;
                        set @referal_urgency := null;
                        set @initial_or_follow_up := null;
                        set @lifestyle_modification_plan := null;
                        set @current_medication_regimen := null;
                        set @are_you_the_referring_or_receiving_peer := null;
						set	@patient_able_to_be_contacted := null;
                        set @understand_bp_meds_and_purpose :=null;
                        set @understand_meds_frequency := null ;
                        set @last_hypersenitive_regimen_change :=null ;
                        set @missed_bp_meds_7days :=null;
                        set @reason_meds_missed :=null;
                        set @reason_for_referal :=null;
                        set @barries_to_complete_referral :=null;
                        set @barries_prevent_referreal := null;
                        set @patient_referal_status  := null ;
                        set @referal_preparation_discussed :=null ;
                        set @is_referal_completed := null ;
                        set @concerns_preventing_referral :=null ;
                        set @next_appointment_facility :=null ;
                        set @referred_in_or_out :=null;
                        set @next_peer_outreach :=null;

                        drop temporary table if exists flat_cdm_peer_navigation_1;
                        create temporary table flat_cdm_peer_navigation_1 (index encounter_id (encounter_id))
                        (select
							obs,
							@prev_id := @cur_id as prev_id,
							@cur_id := t1.person_id as cur_id,
							t1.person_id,
                            t1.encounter_datetime,
							t1.encounter_id,
							t1.encounter_type,
                            t1.location_id,
                            case
								when obs regexp "!!10494=8989!!" then @referal_urgency := 1
								when obs regexp "!!10494=7316!!" then @referal_urgency := 2
                                when obs regexp "!!10494=7314!!" then @referal_urgency := 3
								else @referal_urgency := null
							end as referal_urgency,
                            case
								when obs regexp "!!10114=10497!!" then @referred_in_or_out:= 1
								when obs regexp "!!10114=10112!!" then @referred_in_or_out := 2
								else @referred_in_or_out := null
							end as referred_in_or_out,
                            case
								when obs regexp "!!1839=7850!!" then @initial_or_follow_up:= 1
								when obs regexp "!!1839=2345!!" then @initial_or_follow_up := 2
								else @initial_or_follow_up := null
							end as initial_or_follow_up,
                            case
								when obs regexp "!!10382=10628!!" then @are_you_the_referring_or_receiving_peer:= 1
								when obs regexp "!!10382=10629!!" then @are_you_the_referring_or_receiving_peer := 2
								else @are_you_the_referring_or_receiving_peer := null
							end as are_you_the_referring_or_receiving_peer,
                            case
								when obs regexp "!!10382=10628!!" then @patient_able_to_be_contacted:= 1
								when obs regexp "!!10382=10629!!" then @patient_able_to_be_contacted := 2
								else @patient_able_to_be_contacted := null
							end as patient_able_to_be_contacted,
                            case
								when obs regexp "!!9147=1065!!" then @lifestyle_modification_plan := 1
								when obs regexp "!!9147=1066!!" then @lifestyle_modification_plan := 2
								else @lifestyle_modification_plan := null
							end as lifestyle_modification_plan,
                             case
								when obs regexp "!!10241=1242!!" then @current_medication_regimen := 1
								when obs regexp "!!10241=1243!!" then @current_medication_regimen := 2
                                when obs regexp "!!10241=2265!!" then @current_medication_regimen := 3
								when obs regexp "!!10241=250!!" then  @current_medication_regimen := 4
                                when obs regexp "!!10241=2267!!" then @current_medication_regimen := 5
								when obs regexp "!!10241=2276!!" then @current_medication_regimen := 6
                                when obs regexp "!!10241=7303!!" then @current_medication_regimen := 7
                                when obs regexp "!!10241=8834!!" then @current_medication_regimen := 8
                                when obs regexp "!!10241=8836!!" then @current_medication_regimen := 9
								else @current_medication_regimen := null
							end as current_medication_regimen,
                            case
								when obs regexp "!!1654=1065!!" then @understand_bp_meds_and_purpose := 1
								when obs regexp "!!1654=1066!!" then @understand_bp_meds_and_purpose := 2
								else @understand_bp_meds_and_purpose := null
							end as understand_bp_meds_and_purpose,
							case
								when obs regexp "!!10451=1065!!" then @understand_meds_frequency := 1
								when obs regexp "!!10451=1066!!" then @understand_meds_frequency := 2
								else @understand_meds_frequency := null
							end as understand_meds_frequency,
                            case
								when obs regexp "!!10495=1107!!" then @last_hypersenitive_regimen_change := 1
								when obs regexp "!!10495=1930!!" then @last_hypersenitive_regimen_change := 2
                                when obs regexp "!!10495=981!!" then  @last_hypersenitive_regimen_change := 1
								when obs regexp "!!10495=1260!!" then @last_hypersenitive_regimen_change := 2
								else @last_hypersenitive_regimen_change := null
							end as last_hypersenitive_regimen_change,
							case
								when obs regexp "!!10452=1065!!" then @missed_bp_meds_7days := 1
								when obs regexp "!!10452=1066!!" then @missed_bp_meds_7days := 2
								else @missed_bp_meds_7days := null
							end as missed_bp_meds_7days,
							case
								when obs regexp "!!1668=9148!!" then @reason_meds_missed := 1
								when obs regexp "!!1668=1648!!" then @reason_meds_missed := 2
                                when obs regexp "!!1668=1664!!" then @reason_meds_missed := 3
								when obs regexp "!!1668=6295!!" then @reason_meds_missed := 4
                                when obs regexp "!!1668=6100!!" then @reason_meds_missed := 5
								when obs regexp "!!1668=1546!!" then @reason_meds_missed := 6
                                when obs regexp "!!1668=1647!!" then @reason_meds_missed := 7
								when obs regexp "!!1668=9126!!" then @reason_meds_missed := 8
                                when obs regexp "!!1668=10453!!" then @reason_meds_missed := 9
                                when obs regexp "!!1668=1548!!" then @reason_meds_missed := 10
								when obs regexp "!!1668=5622!!" then @reason_meds_missed := 11
								else @reason_meds_missed := null
							end as reason_meds_missed,
                            case
								when obs regexp "!!2327=6411!!" then @reason_for_referal := 1
								when obs regexp "!!2327=6583!!" then @reason_for_referal := 2
                                when obs regexp "!!2327=10496!!" then @reason_for_referal := 3
								when obs regexp "!!2327=1664!!" then @reason_for_referal := 4
                                when obs regexp "!!2327=6451!!" then @reason_for_referal := 5
								when obs regexp "!!2327=1486!!" then @reason_for_referal := 6
                                when obs regexp "!!2327=7342!!" then @reason_for_referal := 7
								when obs regexp "!!2327=1578!!" then @reason_for_referal := 8
                                when obs regexp "!!2327=5622!!" then @reason_for_referal := 9
								when obs regexp "!!2327=1548!!" then @reason_for_referal := 10
                                when obs regexp "!!2327=2329!!" then @reason_for_referal := 11
                                when obs regexp "!!2327=2330!!" then @reason_for_referal := 12
                                when obs regexp "!!2327=5484!!" then @reason_for_referal := 13
								when obs regexp "!!2327=6510!!" then @reason_for_referal := 14
                                when obs regexp "!!2327=7465!!" then @reason_for_referal := 15
								when obs regexp "!!2327=6511!!" then @reason_for_referal := 16
                                when obs regexp "!!2327=6502!!" then @reason_for_referal := 17
                                when obs regexp "!!2327=7190!!" then @reason_for_referal := 18
                                when obs regexp "!!2327=8262!!" then @reason_for_referal := 19
								else @reason_for_referal := null
							end as reason_for_referal,
							case
								when obs regexp "!!10459=1107!!" then @barries_to_complete_referral := 1
								when obs regexp "!!10459=10458!!" then @barries_to_complete_referral := 2
                                when obs regexp "!!10459=10457!!" then @barries_to_complete_referral := 3
								when obs regexp "!!10459=1578!!" then @barries_to_complete_referral := 4
                                when obs regexp "!!10459=10455!!" then @barries_to_complete_referral := 5
								when obs regexp "!!10459=820!!" then @barries_to_complete_referral := 6
                                when obs regexp "!!10459=1576!!" then @barries_to_complete_referral := 7
								when obs regexp "!!10459=1577!!" then @barries_to_complete_referral := 8
                                when obs regexp "!!10459=1666!!" then @barries_to_complete_referral := 9
                                when obs regexp "!!10459=10456!!" then @barries_to_complete_referral := 10
								when obs regexp "!!10459=1915!!" then @barries_to_complete_referral := 11
								else @barries_to_complete_referral := null
							end as barries_to_complete_referral,
                            case
								when obs regexp "!!10460=1065!!" then @barries_prevent_referreal := 1
								when obs regexp "!!10460=1066!!" then @barries_prevent_referreal := 2
								else @barries_prevent_referreal := null
							end as barries_prevent_referreal,
                            case
								when obs regexp "!!9082=10461!!" then @patient_referal_status := 1
								when obs regexp "!!9082=10462!!" then @patient_referal_status := 2
                                when obs regexp "!!9082=10463!!" then @patient_referal_status := 3
								when obs regexp "!!9082=9083!!" then @patient_referal_status := 4
                                when obs regexp "!!9082=1593!!" then @patient_referal_status := 5
								when obs regexp "!!9082=9079!!" then @patient_referal_status := 6
                                when obs regexp "!!9082=1915!!" then @patient_referal_status := 7
								else @patient_referal_status := null
							end as patient_referal_status,
							case
								when obs regexp "!!10467=10464!!" then @referal_preparation_discussed := 1
								when obs regexp "!!10467=6468!!" then @referal_preparation_discussed := 2
                                when obs regexp "!!10467=10465!!" then @referal_preparation_discussed := 3
								when obs regexp "!!10467=10466!!" then @referal_preparation_discussed := 4
								else @referal_preparation_discussed := null
							end as referal_preparation_discussed,
							case
								when obs regexp "!!10462=1065!!" then @is_referal_completed := 1
								when obs regexp "!!10462=1066!!" then @is_referal_completed := 2
								else @is_referal_completed := null
							end as is_referal_completed,
                            case
								when obs regexp "!!10486=1065!!" then @concerns_preventing_referral := 1
								when obs regexp "!!10486=1066!!" then @concerns_preventing_referral := 2
								else @concerns_preventing_referral := null
							end as concerns_preventing_referral,
							case
								when obs regexp "!!10486=1065!!" then @next_appointment_facility := 1
								when obs regexp "!!10486=1066!!" then @next_appointment_facility := 2
								else @next_appointment_facility := null
							end as next_appointment_facility,
                            case
								when obs regexp "!!9166= null!!" then @next_peer_outreach := 1
								else @next_peer_outreach := null
							end as next_peer_outreach

                        from flat_cdm_peer_navigation_0 t1
                            join amrs.person p using (person_id)
                        );



                        set @prev_id = null;
                        set @cur_id = null;

                        set @prev_clinical_location_id = null;
                        set @cur_clinical_location_id = null;


                        alter table flat_cdm_peer_navigation_1 drop prev_id, drop cur_id;

                        drop table if exists flat_cdm_peer_navigation_2;
                        create temporary table flat_cdm_peer_navigation_2
                        (select *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := person_id as cur_id

                            from flat_cdm_peer_navigation_1
                            order by person_id, date(encounter_datetime) desc
                        );

                        alter table flat_cdm_peer_navigation_2 drop prev_id, drop cur_id;


                        set @prev_id = null;
                        set @cur_id = null;
                        set @prev_clinical_location_id = null;
                        set @cur_clinical_location_id = null;

                        drop temporary table if exists flat_cdm_peer_navigation_3;
                        create temporary table flat_cdm_peer_navigation_3 (prev_encounter_datetime datetime, prev_encounter_type int, index person_enc (person_id, encounter_datetime desc))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id

                            from flat_cdm_peer_navigation_2 t1
                            order by person_id, date(encounter_datetime)
                        );

                        alter table flat_cdm_peer_navigation_3 drop prev_id, drop cur_id;

                        set @prev_id = null;
                        set @cur_id = null;

                        #Handle transfers

                        drop temporary table if exists flat_cdm_peer_navigation_4;

                        create temporary table flat_cdm_peer_navigation_4 ( index person_enc (person_id, encounter_datetime))
                        (select
                            *,
                            @prev_id := @cur_id as prev_id,
                            @cur_id := t1.person_id as cur_id



                            from flat_cdm_peer_navigation_3 t1
                            order by person_id, date(encounter_datetime)
                        );


SELECT
    COUNT(*)
INTO @new_encounter_rows FROM
    flat_cdm_peer_navigation_4;

SELECT @new_encounter_rows;
                    set @total_rows_written = @total_rows_written + @new_encounter_rows;
SELECT @total_rows_written;



                    SET @dyn_sql=CONCAT('replace into ',@write_table,
                        '(select
                        date_created,
                        person_id,
                        encounter_datetime,
                        encounter_id,
                        encounter_type,
                        location_id,
                        referal_urgency,
                        initial_or_follow_up,
						referred_in_or_out,
                        are_you_the_referring_or_receiving_peer,
                        patient_able_to_be_contacted,
                        lifestyle_modification_plan,
                        current_medication_regimen,
                        understand_bp_meds_and_purpose,
                        understand_meds_frequency,
                        last_hypersenitive_regimen_change,
						missed_bp_meds_7days,
                        reason_meds_missed,
                        reason_for_referal,
                        barries_to_complete_referral,
                        concerns_preventing_referral,
                        patient_referal_status,
                        is_referal_completed,
                        referal_preparation_discussed,
                        next_appointment_facility,
                        next_peer_outreach



                        from flat_cdm_peer_navigation_4 t1
                        join amrs.location t2 using (location_id))');

                    PREPARE s1 from @dyn_sql;
                    EXECUTE s1;
                    DEALLOCATE PREPARE s1;




                    SET @dyn_sql=CONCAT('delete t1 from ',@queue_table,' t1 join flat_cdm_peer_navigation_build_queue__0 t2 using (person_id);');

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
insert into etl.flat_log values (@start,@last_date_created,@table_version,timestampdiff(second,@start,@end));
SELECT
    CONCAT(@table_version,
            ' : Time to complete: ',
            TIMESTAMPDIFF(MINUTE, @start, @end),
            ' minutes');



END
