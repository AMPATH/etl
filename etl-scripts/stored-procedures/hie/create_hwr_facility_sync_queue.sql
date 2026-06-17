DELIMITER $$
CREATE  PROCEDURE `create_hwr_facility_sync_queue`(IN location_uuid varchar(50))
BEGIN

        set @location_uuid:=location_uuid;
		CREATE TABLE IF NOT EXISTS hie.hwr_sync (
    id INT AUTO_INCREMENT,
    provider_uuid VARCHAR(100) NOT NULL,
    national_id VARCHAR(100) NOT NULL,
    location_uuid VARCHAR(100) NULL,
    created_at TIMESTAMP DEFAULT CURRENT_TIMESTAMP,
    INDEX provider_uuid (provider_uuid),
    INDEX national_id (national_id),
    INDEX location_uuid (location_uuid),
    PRIMARY KEY id (id)
);
        
       
SELECT 
    CONCAT('Adding providers to hwr_sync queue...location :',
            @location_uuid);

DELETE FROM hie.hwr_sync 
WHERE
    location_uuid in (@location_uuid);

SELECT 
    CONCAT('Deleting all records hwr_sync queue...location :',
            @location_uuid);

replace into hie.hwr_sync(
SELECT
  NULL as id, 
  pr.uuid AS provider_uuid, 
  pa.value_reference AS national_id, 
  epl.location_uuid AS location_uuid,
  NULL as date_created
FROM
    amrs.provider pr
        JOIN
    amrs.provider_attribute pa ON (pa.provider_id = pr.provider_id
        AND pa.attribute_type_id = 5)
        JOIN (
         SELECT 
    ep.provider_id,
    e.encounter_datetime,
    e.location_id,
    pr.person_id,
    l.uuid AS location_uuid
FROM
    amrs.encounter e
        JOIN
    amrs.location l ON (l.location_id = e.location_id)
        INNER JOIN
    amrs.encounter_provider ep ON e.encounter_id = ep.encounter_id
        INNER JOIN
    amrs.provider pr ON ep.provider_id = pr.provider_id
WHERE
    e.voided = 0
    AND l.uuid = @location_uuid
        AND e.encounter_datetime BETWEEN DATE_SUB(NOW(), INTERVAL 12 MONTH) AND NOW()
GROUP BY ep.provider_id
        ) `epl` on (epl.provider_id = pr.provider_id)
	   
WHERE
    pr.retired = 0
GROUP BY pr.provider_id);
SELECT CONCAT('Done Adding providers to hwr_sync queue....');
END$$
DELIMITER ;
