DELIMITER $$
CREATE FUNCTION `transform_kenya_emr_arvs`(obs varchar(10000),regimen_concept_id varchar(250)) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN
/*

*/
                set @regimen := null;
                select
                (
				case
					when obs regexp concat('!!',regimen_concept_id,'=10985!!') then @regimen:= '797 ## 628 ## 9759'
					when obs regexp concat('!!',regimen_concept_id,'=10797!!') then @regimen:= '802 ## 628 ## 631'
					when obs regexp concat('!!',regimen_concept_id,'=6964!!') then @regimen:= '802 ## 628 ## 633'
					when obs regexp concat('!!',regimen_concept_id,'=6467!!') then @regimen:= '797 ## 628 ## 631'
					when obs regexp concat('!!',regimen_concept_id,'=9053!!') then @regimen:= '797 ## 628 ## 633'
					when obs regexp concat('!!',regimen_concept_id,'=792!!') then @regimen:= '625 ## 628 ## 631'
					when obs regexp concat('!!',regimen_concept_id,'=9052!!') then @regimen:= '625 ## 628 ## 633'
					when obs regexp concat('!!',regimen_concept_id,'=10268!!') then @regimen:= '802 ## 628 ## 9759'
					when obs regexp concat('!!',regimen_concept_id,'=10986!!') then @regimen:= '814 ## 628 ## 9759'
					when obs regexp concat('!!',regimen_concept_id,'=10798!!') then @regimen:= '797 ## 628 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=10799!!') then @regimen:= '797 ## 628 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=9055!!') then @regimen:= '802 ## 628 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=10800!!') then @regimen:= '802 ## 628 ## 11023 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=10846!!') then @regimen:= '625 ## 628 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=10851!!') then @regimen:= '797 ## 802 ## 628 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=10850!!') then @regimen:= '6158 ## 6156 ## 6157 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=9051!!') then @regimen:= '814 ## 628 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=11024!!') then @regimen:= '814 ## 628 ## 11023 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=9051!!') then @regimen:= '814 ## 628 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=9050!!') then @regimen:= '814 ## 628 ## 631'
					when obs regexp concat('!!',regimen_concept_id,'=10801!!') then @regimen:= '814 ## 628 ## 633'
					when obs regexp concat('!!',regimen_concept_id,'=817!!') then @regimen:= '797 ## 628 ## 814'
					when obs regexp concat('!!',regimen_concept_id,'=10848!!') then @regimen:= '625 ## 628 ## 814'
					when obs regexp concat('!!',regimen_concept_id,'=10802!!') then @regimen:= '802 ## 814 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=10803!!') then @regimen:= '814 ## 796 ## 9026 ## 795'
					when obs regexp concat('!!',regimen_concept_id,'=10847!!') then @regimen:= '814 ## 802 ## 628 ## 9026 ## 795'
					else ""    
				end 
                
                ) into @regimen;
                
                return @regimen;
            
        END$$
DELIMITER ;