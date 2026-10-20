-- ---------------------------------------------------------------------------
-- Coded-value helpers for packed obs strings
--
-- getValues.sql returns the raw token stored after "!!concept_id=".  For
-- coded concepts that token is another concept_id, so a report prints
-- "10900" instead of "TRACE".  GetCodedValue() resolves that token through
-- etl.concept_value_map.  GetConceptName() is the bare id -> name lookup for
-- values that are read directly from amrs.obs rather than from a packed
-- string.
--
-- Requires create_concept_value_map.sql, and the map to have been refreshed:
--   CALL etl.refresh_concept_value_map();
--
-- Install (DELIMITER-wrapped because this file defines routines):
--   { echo 'DELIMITER $$'; cat getCodedValue.sql; echo '$$';
--     echo 'DELIMITER ;'; } | mysql etl
-- ---------------------------------------------------------------------------

DROP FUNCTION IF EXISTS etl.GetConceptName;

DELIMITER $$
CREATE FUNCTION etl.GetConceptName(p_concept_id INT)
RETURNS VARCHAR(255)
CHARSET utf8
READS SQL DATA
BEGIN
    DECLARE v_name VARCHAR(255) DEFAULT NULL;

    IF p_concept_id IS NULL THEN
        RETURN NULL;
    END IF;

    SELECT name
      INTO v_name
      FROM etl.concept_value_map
     WHERE concept_id = p_concept_id;

    -- Keep the id visible when the dictionary does not have the concept, so
    -- missing dictionary entries are obvious in the report rather than blank.
    RETURN IFNULL(v_name, CAST(p_concept_id AS CHAR));
END$$
DELIMITER ;

DROP FUNCTION IF EXISTS etl.GetCodedValue;

DELIMITER $$
CREATE FUNCTION etl.GetCodedValue(p_obs TEXT, p_concept_id INT)
RETURNS VARCHAR(1000)
CHARSET utf8
READS SQL DATA
BEGIN
    DECLARE v_key      VARCHAR(64);
    DECLARE v_pos      INT;
    DECLARE v_raw      VARCHAR(1000);
    DECLARE v_is_coded TINYINT DEFAULT 0;

    IF p_obs IS NULL OR p_concept_id IS NULL THEN
        RETURN NULL;
    END IF;

    SET v_key = CONCAT('!!', p_concept_id, '=');
    SET v_pos = LOCATE(v_key, p_obs);

    IF v_pos = 0 THEN
        RETURN NULL;
    END IF;

    -- Packed format is !!concept=value!! ## !!concept=value!!, so the next
    -- "!!" terminates the value.  Reading from the TEXT argument directly
    -- avoids the VARCHAR(10000) truncation in GetValues().
    SET v_raw = SUBSTRING(p_obs, v_pos + LENGTH(v_key));
    SET v_raw = SUBSTRING_INDEX(v_raw, '!!', 1);

    SELECT is_coded
      INTO v_is_coded
      FROM etl.concept_value_map
     WHERE concept_id = p_concept_id;

    IF v_is_coded = 1 AND v_raw REGEXP '^[0-9]+$' THEN
        RETURN IFNULL(
            (SELECT name
               FROM etl.concept_value_map
              WHERE concept_id = CAST(v_raw AS UNSIGNED)),
            v_raw
        );
    END IF;

    RETURN v_raw;
END$$
DELIMITER ;
