DELIMITER $$
CREATE  FUNCTION `get_arv_names`(concepts varchar(1000)) RETURNS varchar(1000) CHARSET latin1
    DETERMINISTIC
BEGIN
        
			DECLARE e varchar(500);
        
			select concat(            
				#d4T-40 (STAVUDINE) (625)
				case
					when concepts regexp '([[:<:]]|^)(6965|792|625)([[:>:]]|$)' then "d4T ## "
					else ""    
				end, 
                
                
				#LAMIVUDINE (628)
				case
					when concepts regexp '([[:<:]]|^)(6467|1400|630|6679|6965|9050|817|9052|792|628)([[:>:]]|$)'  then "3TC ## "
					else ""    
				end, 

                #NEVIRAPINE (631)
				case
					when concepts regexp '([[:<:]]|^)(631|6467|9050|6095|792)([[:>:]]|$)' then "NVP ## "
					else ""
				end, 

				#EFAVIRENZ (633)
				case
					when concepts regexp '([[:<:]]|^)(633|6964|9053|9052)([[:>:]]|$)' then "EFV ## "
					else "" 
				end, 
                
				# NELFINAVIR (635)
				case
					when concepts regexp '([[:<:]]|^)635([[:>:]]|$)' then "NFV ## "
					else ""
				end,
                
                #INDINAVIR (749)
				case
					when concepts regexp '([[:<:]]|^)749([[:>:]]|$)' then "IDV ## "
					else ""  
				end, 
                
                #EMTRICITABINE (791)
				case
					when concepts regexp '([[:<:]]|^)(791|6180|9435)([[:>:]]|$)' then "FTC ## "
					else ""   
				end,
				
				#RITONAVIR (795)
				case
					when concepts regexp '([[:<:]]|^)(795|9051|9905|6160|794|9055)([[:>:]]|$)' then "RTV ## "
					else ""    
				end, 
                
                #DDI 200 (DIDANOSINE) (796)
				case
					when concepts regexp '([[:<:]]|^)796([[:>:]]|$)' then "DDI ## "
					else ""    
				end, 
                
                #ZIDOVUDINE (797)
				case
					when concepts regexp '([[:<:]]|^)(6467|630|797|817|9053|9615|8588|8587)([[:>:]]|$)' then "AZT ## "
					else ""    
				end,
                
                #TENOFOVIR (802)
				case
					when concepts regexp '([[:<:]]|^)(6964|1400|6180|8435|802|9055)([[:>:]]|$)' then "TDF ## "
					else ""
				end, 
                
                #ABACAVIR (814)
				case
					when concepts regexp '([[:<:]]|^)(9051|6679|814|9050|817)([[:>:]]|$)' then "ABC ## "
					else ""
				end, 
                
				#RALTEGRAVIR (6156)
				case
					when concepts regexp '([[:<:]]|^)6156([[:>:]]|$)' then "RAL ## "
					else ""   
				end, 
                
				# DARUNAVIR (6157)
				case
					when concepts regexp '([[:<:]]|^)6157([[:>:]]|$)' then "DRV ## "
					else ""
				end, 
                
                #ETRAVIRINE (6158)
				case
					when concepts regexp '([[:<:]]|^)6158([[:>:]]|$)' then "ETR ## "
					else ""    
				end,
					
				#ATAZANAVIR (6159)
				case
					when concepts regexp '([[:<:]]|^)(6159|6160)([[:>:]]|$)' then "ATV ## "
					else ""   
				end, 
                
                
                #LOPINAVIR (9026)
				case
					when concepts regexp '([[:<:]]|^)(9051|9026|794|9055)([[:>:]]|$)' then "LOP ## "
					else ""    
				end,
                
                # DOLUTEGRAVIR (9759)
				case
					when concepts regexp '([[:<:]]|^)9759([[:>:]]|$)' then "DTG ## "
					else ""    
				end, 

                				
                # RILPIVIRINE (10090)
				case
					when concepts regexp '([[:<:]]|^)(10090|9435)([[:>:]]|$)' then "RPV ## "
					else ""   
				end,
                
                # OTHER/UKNOWN
				case
					when concepts regexp '([[:<:]]|^)5424([[:>:]]|$)' then "UNKNOWN DRUG ## "
					else ""   
				end
                

			) into e;
            #return e;
            
            return if(e="",null,substring(e,1,length(e)-4));
            
        END$$
DELIMITER ;
