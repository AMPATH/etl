DELIMITER $$
CREATE  FUNCTION `normalize_arvs`(obs varchar(10000),question_concept_ids varchar(250)) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN
        
			DECLARE e varchar(500);
        
			select concat(            
				#d4T-40 (STAVUDINE) (625)
				case
					when obs regexp concat('!!',question_concept_ids,'=(6965|792|625)!!') then "625 ## "
#					when obs regexp '!!(1088|2154)=(6965|792|625)!!') then "625 ## "
					else ""    
				end, 
                
                
				#LAMIVUDINE (628)
				case
					when obs regexp concat('!!',question_concept_ids,'=(6467|1400|630|6679|6965|9050|817|9052|792|628|6964|10268|9051)!!') then "628 ## "
#					when obs regexp '!!(1088|2154)=(6467|1400|630|6679|6965|9050|817|9052|792|628)!!') then "628 ## "
					else ""    
				end, 

                #NEVIRAPINE (631)
				case
					when obs regexp concat('!!',question_concept_ids,'=(631|6467|9050|6095|792)!!') then "631 ## "
#					when obs regexp '!!(1088|2154)=(631|6467|9050|6095|792)!!') then "631 ## "
					else ""
				end, 

				#EFAVIRENZ (633)
				case
					when obs regexp concat('!!',question_concept_ids,'=(633|6964|9053|9052)!!') then "633 ## "
#					when obs regexp '!!(1088|2154)=(633|6964|9053|9052)!!') then "633 ## "
					else "" 
				end, 
                
				# NELFINAVIR (635)
				case
					when obs regexp concat('!!',question_concept_ids,'=635!!') then "635 ## "
#					when obs regexp '!!(1088|2154)=635!!') then "635 ## "
					else ""
				end,
                
                #INDINAVIR (749)
				case
					when obs regexp concat('!!',question_concept_ids,'=749!!') then "749 ## "
#					when obs regexp '!!(1088|2154)=749!!') then "749 ## "
					else ""  
				end, 
                
                #EMTRICITABINE (791)
				case
					when obs regexp concat('!!',question_concept_ids,'=(791|6180|9435)!!') then "791 ## "
#					when obs regexp '!!(1088|2154)=(791|6180|9435)!!') then "791 ## "
					else ""   
				end,
				
				#RITONAVIR (795)
				case
					when obs regexp concat('!!',question_concept_ids,'=(795|9051|9905|6160|794|9055)!!') then "795 ## "
#					when obs regexp '!!(1088|2154)=(795|9051|9905|6160|794|9055)!!') then "795 ## "
					else ""    
				end, 
                
                #DDI 200 (DIDANOSINE) (796)
				case
					when obs regexp concat('!!',question_concept_ids,'=796!!') then "796 ## "
#					when obs regexp '!!(1088|2154)=796!!') then "796 ## "
					else ""    
				end, 
                
                #ZIDOVUDINE (797)
				case
					when obs regexp concat('!!',question_concept_ids,'=(6467|630|797|817|9053|9615|8588|8587)!!') then "797 ## "
#					when obs regexp '!!(1088|2154)=(6467|630|797|817|9053|9615|8588|8587)!!') then "797 ## "
					else ""    
				end,
                
                #TENOFOVIR (802)
				case
					when obs regexp concat('!!',question_concept_ids,'=(6964|1400|6180|8435|802|9055|10268)!!') then "802 ## "
#					when obs regexp '!!(1088|2154)=(6964|1400|6180|8435|802|9055)!!') then "802 ## "
					else ""
				end, 
                
                #ABACAVIR (814)
				case
					when obs regexp concat('!!',question_concept_ids,'=(9051|6679|814|9050|817)!!') then "814 ## "
#					when obs regexp '!!(1088|2154)=(9051|6679|814|9050|817)!!') then "814 ## "
					else ""
				end, 
                
				#RALTEGRAVIR (6156)
				case
					when obs regexp concat('!!',question_concept_ids,'=6156!!') then "6156 ## "
#					when obs regexp '!!(1088|2154)=6156!!') then "6156 ## "
					else ""   
				end, 
                
				# DARUNAVIR (6157)
				case
					when obs regexp concat('!!',question_concept_ids,'=6157!!') then "6157 ## "
#					when obs regexp '!!(1088|2154)=6157!!') then "6157 ## "
					else ""
				end, 
                
                #ETRAVIRINE (6158)
				case
					when obs regexp concat('!!',question_concept_ids,'=6158!!') then "6158 ## "
#					when obs regexp '!!(1088|2154)=6158!!') then "6158 ## "
					else ""    
				end,
					
				#ATAZANAVIR (6159)
				case
					when obs regexp concat('!!',question_concept_ids,'=(6159|6160)!!') then "6159 ## "
#					when obs regexp '!!(1088|2154)=(6159|6160)!!') then "6159 ## "
					else ""   
				end, 
                
                
                #LOPINAVIR (9026)
				case
					when obs regexp concat('!!',question_concept_ids,'=(9051|9026|794|9055)!!') then "9026 ## "
#					when obs regexp '!!(1088|2154)=(9051|9026|794|9055)!!') then "9026 ## "
					else ""    
				end,
                
                # DOLUTEGRAVIR (9759)
				case
					when obs regexp concat('!!',question_concept_ids,'=(9759|10268)!!') then "9759 ## "
#					when obs regexp '!!(1088|2154)=9759!!') then "9759 ## "
					else ""    
				end, 

                				
                # RILPIVIRINE (10090)
				case
					when obs regexp concat('!!',question_concept_ids,'=(10090|9435)!!') then "10090 ## "
#					when obs regexp '!!(1088|2154)=(10090|9435)!!') then "10090 ## "
					else ""   
				end,
                
				case
					when obs regexp concat('!!',question_concept_ids,'=(5424|5811)!!') then "5424 ## "
					else ""   
				end,

			    case
					when obs regexp concat('!!',question_concept_ids,'=12053!!') then "12053 ## "
					else ""
				end

			) into e;
            #return e;
            
            return if(e="",null,substring(e,1,length(e)-4));
            
        END;
