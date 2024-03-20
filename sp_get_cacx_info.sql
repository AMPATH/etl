CREATE DEFINER=`hkorir`@`%` PROCEDURE `sp_get_cacx_info`(IN person_uuid VARCHAR(100))
BEGIN

SET @tah := NULL;
SET @tah_datetime := NULL;
SET @id := (SELECT person_id FROM etl.flat_labs_and_imaging WHERE uuid = person_uuid LIMIT 1);

DROP TABLE IF EXISTS latest_ca_cx_info;
SELECT * FROM  (SELECT 
    o.person_id,
    o.concept_id,
    o.value_coded,
    o.obs_datetime,
    fli.via_or_via_vili,
    fli.pap_smear,
    fli.hpv,
    DATE_FORMAT(fli.test_datetime, '%d-%m-%Y') AS 'test_date',
    CASE
        WHEN via_or_via_vili IS NOT NULL THEN 'VIA or VIA/VILI'
        WHEN pap_smear IS NOT NULL THEN 'PAP SMEAR'
        WHEN hpv IS NOT NULL THEN 'HPV'
        ELSE NULL
    END AS 'test',
    CASE
        WHEN fli.via_or_via_vili = 7469 THEN 'ACETOWHITE LESION'
        WHEN fli.via_or_via_vili = 1115 THEN 'NORMAL'
        WHEN fli.via_or_via_vili = 6497 THEN 'DYSFUNCTIONAL UTERINE BLEEDING'
        WHEN fli.via_or_via_vili = 703 THEN 'POSITIVE'
        WHEN fli.via_or_via_vili = 7470 THEN 'PUNCTUATED CAPILLARIES'
        WHEN fli.via_or_via_vili = 664 THEN 'NEGATIVE'
        WHEN fli.via_or_via_vili = 7472 THEN 'ATYPICAL BLOOD VESSELS'
        WHEN fli.via_or_via_vili = 7293 THEN 'ULCER'
        WHEN fli.via_or_via_vili = 9593 THEN 'FRIABLE TISSUE'
        WHEN fli.via_or_via_vili = 6971 THEN 'POSSIBLE'
        ELSE NULL
    END AS 'via_test_result',
    fhs.ca_cx_screen,
    fhs.ca_cx_screening_datetime,
    fhs.ca_cx_screening_result,
    fhs.ca_cx_screening_result_datetime,
    CASE
        WHEN  o.value_coded = 5276 THEN @tah := 1
	END as female_sterilization,
    CASE
        WHEN  o.value_coded = 12109 THEN 1
	END as cervix_not_accessible,
    CASE
        WHEN  o.value_coded = 1504 THEN 1
	END as Patient_refusal,
    CASE
        WHEN  o.value_coded = 5989 THEN 1
	END as menstruating,
    @tah := IF(o.value_coded = 5276 , 1, @tah) as tah_done,
    @tah_datetime := IF(o.value_coded = 5276 , @tah_datetime := o.obs_datetime, @tah_datetime) as tah_confirmation_datetime
FROM
    amrs.obs o
        LEFT JOIN
    etl.flat_hiv_summary_v15b fhs ON o.person_id = fhs.person_id
        AND o.encounter_id = fhs.encounter_id
        LEFT JOIN
    etl.flat_labs_and_imaging fli ON o.person_id = fli.person_id
        AND (fli.via_or_via_vili IS NOT NULL
        OR fli.pap_smear IS NOT NULL
        OR fli.hpv IS NOT NULL)
WHERE
    o.concept_id IN (12110 , 10400)
        AND o.person_id = @id
        AND o.voided = 0) latest_ca_cx_info ORDER BY obs_datetime DESC, test_date desc, tah_done desc limit 1; 
END