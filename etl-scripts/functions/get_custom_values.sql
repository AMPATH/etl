DELIMITER $$
CREATE  FUNCTION `GetCustomValues`(obs varchar(10000), concept_id text) RETURNS varchar(1000) CHARSET latin1
    DETERMINISTIC
BEGIN

	DECLARE e varchar(100);	

	DECLARE sep varchar(10);

    DECLARE k varchar(50);

    

    set sep = " ## ";

    set k = concat("!!",concept_id,"=");



	select 

		replace(replace((substring_index(substring(obs,locate(k, obs)),sep,ROUND ((LENGTH(obs) - LENGTH( REPLACE ( obs, k, "") ) ) / LENGTH(k) ))),k,""),"!!","")

		into e;



  RETURN e;

END$$
DELIMITER ;
