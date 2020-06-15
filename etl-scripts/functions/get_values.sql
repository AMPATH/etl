DELIMITER $$
CREATE  FUNCTION `GetValues`(obs varchar(10000), concept_id int) RETURNS varchar(1000) CHARSET latin1
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
	if(e = "") then
		RETURN null;
	else
		RETURN e;
	end if;

END$$
DELIMITER ;
