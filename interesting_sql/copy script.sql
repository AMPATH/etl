select p.gender, p.birthdate, e.encounter_type, e.location_id,t1.*, t2.prev_encounter_datetime,t2.next_encounter_datetime, t2.prev_encounter_type,t2.next_encounter_type
 from amrs.encounter e
 join flat_moh_indicators t1 using (encounter_id)
 join amrs.person p using (person_id)
 left outer join derived_encounter t2 using (encounter_id,person_id)
 where location_id in (1,2,3)
 into outfile '/var/lib/mysql/moh_data.txt'
 FIELDS TERMINATED BY ','
 ENCLOSED BY '"'
 LINES TERMINATED BY '\n';