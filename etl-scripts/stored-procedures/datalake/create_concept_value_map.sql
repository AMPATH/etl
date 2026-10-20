-- ---------------------------------------------------------------------------
-- Concept dictionary cache
--
-- OpenMRS stores a coded obs answer in obs.value_coded as the concept_id of
-- the answer.  The packed lab strings built by generate_flat_lab_obs keep that
-- raw id, so reports that print "TRACE" or "NEGATIVE" need a dictionary
-- lookup.  This table is that lookup.  It is deliberately flat and small:
-- one row per concept, the preferred English name, and whether the concept
-- is used as an answer set (amrs.concept_answer).
--
-- Install:
--   mysql etl < create_concept_value_map.sql
--
-- Refresh after adding/changing concepts:
--   CALL etl.refresh_concept_value_map();
-- ---------------------------------------------------------------------------

CREATE TABLE IF NOT EXISTS etl.concept_value_map (
    concept_id   INT          NOT NULL,
    name         VARCHAR(255) DEFAULT NULL,
    is_coded     TINYINT(1)   NOT NULL DEFAULT 0,
    date_created TIMESTAMP    NOT NULL DEFAULT CURRENT_TIMESTAMP
                              ON UPDATE CURRENT_TIMESTAMP,
    PRIMARY KEY (concept_id),
    KEY name_idx (name)
) ENGINE = InnoDB DEFAULT CHARSET = utf8;

DROP PROCEDURE IF EXISTS etl.refresh_concept_value_map;

DELIMITER $$
CREATE PROCEDURE etl.refresh_concept_value_map()
BEGIN
    -- Preferred English name first, then any fully-specified English name,
    -- then any English name, then any name at all.  This mirrors the
    -- concept_name joins already used by the other datalake procedures.
    REPLACE INTO etl.concept_value_map (concept_id, name, is_coded)
    SELECT c.concept_id,
           COALESCE(
               MAX(CASE WHEN cn.locale = 'en' AND cn.locale_preferred = 1 AND cn.voided = 0
                        THEN cn.name END),
               MAX(CASE WHEN cn.locale = 'en' AND cn.concept_name_type = 'FULLY_SPECIFIED'
                              AND cn.voided = 0 THEN cn.name END),
               MAX(CASE WHEN cn.locale = 'en' AND cn.voided = 0 THEN cn.name END),
               MAX(CASE WHEN cn.voided = 0 THEN cn.name END)
           ) AS name,
           CASE
               WHEN MAX(CASE WHEN cd.name = 'Coded' THEN 1 ELSE 0 END) = 1
                   THEN 1
               WHEN EXISTS (
                   SELECT 1
                   FROM amrs.concept_answer ca
                   WHERE ca.concept_id = c.concept_id
               ) THEN 1
               ELSE 0
           END AS is_coded
    FROM amrs.concept c
    LEFT JOIN amrs.concept_name cn
           ON cn.concept_id = c.concept_id
    LEFT JOIN amrs.concept_datatype cd
           ON cd.concept_datatype_id = c.datatype_id
    GROUP BY c.concept_id;
END$$
DELIMITER ;
