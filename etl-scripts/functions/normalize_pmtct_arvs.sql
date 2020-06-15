DELIMITER $$
CREATE  FUNCTION `normalize_pmtct_arvs`(obs varchar(10000),question_concept_ids varchar(250)) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN
        
			DECLARE e varchar(500);
        
			select concat(            
				#ZIDOVUDINE FOR SIX WEEKS(9615)
				case
					when obs regexp concat('!!',question_concept_ids,'=(9615)!!') then "9615 ## "
					else ""    
				end, 
                
                
				#INFANT GIVEN NEVIRAPINE FOR SIX WEEKS (8523)
				case
					when obs regexp concat('!!',question_concept_ids,'=(8523)!!') then "8523 ## "
					else ""    
				end, 
                
                #NEVIRAPINE FOR TWELVE WEEKS (9614)
				case
					when obs regexp concat('!!',question_concept_ids,'=(9614)!!') then "9614 ## "
					else ""    
				end,
                
                #INFANT GIVEN NEVIRAPINE FOR FOURTEEN WEEKS (9506)
				case
					when obs regexp concat('!!',question_concept_ids,'=(9506)!!') then "9506 ## "
					else ""    
				end


			) into e;
            #return e;
            
            return if(e="",null,substring(e,1,length(e)-4));
            
        END$$
DELIMITER ;
