DELIMITER $$
CREATE  FUNCTION `normalizeArvsDrugFormulation`(obs varchar(10000)) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN

    declare e varchar(500);

        

			SELECT 

    CONCAT(CASE

                WHEN

                    obs REGEXP '!!1088_drug='

                THEN

                    CONCAT('1088=',

                            GETCUSTOMVALUES(obs, '1088_drug'),

                            ' ## ')

                ELSE ''

            END)

INTO e;

            

            return if(e="",null,substring(e,1,length(e)-4));

END$$
DELIMITER ;
