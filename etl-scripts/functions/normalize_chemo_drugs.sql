DELIMITER $$
CREATE FUNCTION `normalize_chemo_drugs`(obs varchar(10000), question_concept_ids varchar(250)) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN
    DECLARE e varchar(500);

    SELECT CONCAT(
      # MELPHALAN (7213) + PREDNISONE (491)
      CASE
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(7213)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=(491)!!') AND NOT obs regexp CONCAT('!!', question_concept_ids, '=(8486)!!') THEN "MP ## "
          ELSE ""
      END,
      # MELPHALAN (7213) + PREDNISONE (491) + BORTEZOMIB (8486)
      CASE
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(7213)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=491!!') AND obs regexp CONCAT('!!', question_concept_ids, '=8486!!') THEN "MPV ## "
          ELSE ""
      END,
      # BORTEZOMIB (8486) + LENALIDOMIDE (8480) + DECADRON (7207)
      CASE
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(8486)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=8480!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7207!!') THEN "VRD ## "
          ELSE ""
      END,
      # BORTEZOMIB (8486) + THALIDOMIDE (8479) + DECADRON (7207)
      CASE
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(8486)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=8479!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7207!!') THEN "VTD ## "
          ELSE ""
      END,
      # BORTEZOMIB (8486) + DECADRON (7207)
      CASE
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(8486)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7207!!') THEN "VD ## "
          ELSE ""
      END,
      # BORTEZOMIB (8486) + MELPHALAN (7213) + DECADRON (7207)
      CASE
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(8486)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7213!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7207!!') THEN "VMD ## "
          ELSE ""
      END,
      # LENALIDOMIDE (8480) + DECADRON (7207)
      CASE 
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(8480)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7207!!') THEN "RD ## "
          ELSE ""
      END,
      # THALIDOMIDE (8479) + DECADRON (7207)
      CASE 
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(8479)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7207!!') THEN "TD ## "
          ELSE ""
      END,
      # LENALIDOMIDE (8480) + CYCLOPHOSPHAMIDE (7203) + DECADRON (7207)
      CASE 
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(8479)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7203!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7207!!') THEN "RCD ## "
          ELSE ""
      END,
       # LENALIDOMIDE (8480) + THALIDOMIDE (8479) + DECADRON (7207)
      CASE 
          WHEN obs regexp CONCAT('!!', question_concept_ids, '=(8480)!!') AND obs regexp CONCAT('!!', question_concept_ids, '=8479!!') AND obs regexp CONCAT('!!', question_concept_ids, '=7207!!') THEN "RTD ## "
          ELSE ""
      END
    )
    INTO e;
    RETURN IF (e = "", null, substring(e, 1, length(e) - 4));
END$$
DELIMITER ;
