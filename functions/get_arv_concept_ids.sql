DELIMITER $$
CREATE FUNCTION `getArvConceptIds`(obs varchar(10000), concept_id int) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN
	DECLARE concepts varchar(1000);
    
    set concepts = GetValues(obs,concept_id);

    return replaceArvConceptIds(concepts);
END$$
DELIMITER ;
