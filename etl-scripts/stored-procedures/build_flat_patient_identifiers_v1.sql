DELIMITER $$
CREATE PROCEDURE `build_flat_patient_identifiers_v1`()
BEGIN
-- Truncate the table
 TRUNCATE TABLE etl.flat_patient_identifiers_v1;
 
 CREATE TABLE IF NOT EXISTS etl.flat_patient_identifiers_v1 (
    date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
    patient_id INT,
    ccc VARCHAR(255),
    ovcid VARCHAR(255),
    nupi VARCHAR(300),
    cr_id VARCHAR(255),
    sha_id VARCHAR(255),
    joint_identifiers VARCHAR(500),
    INDEX patient_date_created (patient_id , date_created),
    INDEX patientid (patient_id)
);
replace into etl.flat_patient_identifiers_v1(
SELECT
    NULL,
    patient_id,
    GROUP_CONCAT(CASE WHEN identifier_type = 28 THEN identifier ELSE NULL END) AS ccc,
    GROUP_CONCAT(CASE WHEN identifier_type = 43 THEN identifier ELSE NULL END) AS ovcid,
    GROUP_CONCAT(CASE WHEN identifier_type = 45 THEN identifier ELSE NULL END) AS nupi,
    GROUP_CONCAT(CASE WHEN identifier_type = 55 THEN identifier ELSE NULL END) AS cr_id,
    GROUP_CONCAT(CASE WHEN identifier_type = 52 THEN identifier ELSE NULL END) AS sha_id,
    GROUP_CONCAT(CASE WHEN identifier_type IN (28, 43, 45) THEN identifier ELSE NULL END) AS joint_identifiers
  FROM amrs.patient_identifier
  WHERE identifier_type IN (28, 43, 45,52,55) AND (voided IS NULL OR voided = 0)
  GROUP BY patient_id);
END$$
DELIMITER ;
