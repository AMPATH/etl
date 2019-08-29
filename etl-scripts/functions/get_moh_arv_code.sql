DELIMITER $$
CREATE  FUNCTION `get_moh_arv_code`(concepts varchar(1000), age smallint, line tinyint) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN
        
			DECLARE e varchar(500);
        
			select
				case 
					when age >= 12 then
						case
                        
							#ADULT 1ST LINE REGIMENS
                        
							#AZT + 3TC + NVP (Zidovudine + Lamivudine + Nevirapine) AF1A
							when concepts = '628 ## 631 ## 797' then "AF1A"
							
							#AZT + 3TC + EFV (Zidovudine + Lamivudine + Efavirenz)
							when concepts = '628 ## 633 ## 797' then "AF1B"
							
							#TDF + 3TC + NVP (Tenofovir + Lamivudine + Nevirapine)
							when concepts = '628 ## 631 ## 802' then "AF2A"
							
							#TDF + 3TC + EFV (Tenofovir + Lamivudine + Efavirenz)
							when concepts = '628 ## 633 ## 802' then "AF2B"
							
							#d4T + 3TC + NVP (Stavudine + Lamivudine + Nevirapine)
							when concepts = '625 ## 628 ## 631' then "AF3A"
							
							#d4T + 3TC + EFV (Stavudine + Lamivudine + Efavirenz)
							when concepts = '625 ## 628 ## 633' then "AF3B"
							
							#ABC + 3TC + NVP (Abacavir + Lamivudine + Nevirapine)
							when concepts = '628 ## 631 ## 814' then "AF4A"
							
							#ABC + 3TC + EFV (Abacavir + Lamivudine + Efavirenz
							when concepts = '628 ## 633 ## 814' then "AF4B"
                            
							
                            #ADULT 2ND LINE REGIMENS
                            
                            #AZT + 3TC + LPV/r (Zidovudine + Lamivudine + Lopinavir/ Ritonavir)	AS1A
							when concepts = '628 ## 795 ## 814 ## 9026' then "AS1A"
                            
                            #AZT + 3TC + ATV/r (Zidovudine + Lamivudine + Atazanavir/Ritonavir)	AS1B
							when concepts = '628 ## 795 ## 797 ## 6159' then "AS1B"

							#TDF + 3TC + LPV/r (Tenofovir + Lamivudine + Lopinavir/Ritonavir)	AS2A
							when concepts = '628 ## 795 ## 802 ## 9026' then "AS2A"

							#TDF + 3TC + ATV/r (Tenofovir + Lamivudine + Atazanavir/Ritonavir)	AS2C
							when concepts = '628 ## 795 ## 802 ## 6159' then "AS2C"

							#ABC + 3TC + LPV/r (Abacavir + Lamivudine + Lopinavir/Ritonavir)	AS5A
							when concepts = '628 ## 795 ## 814 ## 9026' then "AS5A"

							#ABC + 3TC + ATV/r (Abacavir + Lamivudine + Atazanavir/Ritonavir)	AS5B
							when concepts = '628 ## 795 ## 814 ## 6159' then "AS5B"
												
                                                
							#THIRD LINE REGIMENS
                            #RAL + 3TC + DRV + RTV (Raltegravir + Lamivudine + Darunavir + Ritonavir) AT1A
							when concepts = '628 ## 795 ## 6156 ## 6157' then "AT1A"

                            #RAL + 3TC + DRV + RTV + AZT (Raltegravir + Lamivudine + Darunavir + Ritonavir + Zidovudine) AT1B
							when concepts = '628 ## 795 ## 797 ## 6156 ## 6157' then "AT1B"

							#RAL + 3TC + DRV + RTV+TDF (Raltegravir + Lamivudine + Darunavir + Ritonavir + Tenofovir) AT1C
							when concepts = '628 ## 795 ## 802 ## 6156 ## 6157' then "AT1C"

							#ETV + 3TC + DRV + RTV (Etravirine + Lamivudine + Darunavir + Ritonavir) AT2A
							when concepts = '628 ## 795 ## 6157 ## 6158' then "AT2A"
                                                
                                                
							else "adult unknown"
						end
                        
					when age < 12 then
						case
							#AZT + 3TC + NVP (Zidovudine + Lamivudine + Nevirapine)	CF1A
                            when concepts = '628 ## 631 ## 797' then 'CF1A'

							#AZT + 3TC + NVP (Zidovudine + Lamivudine + EFV)	CF1B
                            when concepts = '628 ## 633 ## 797' then 'CF1B'

							#AZT + 3TC + LPV/r (Zidovudine + Lamivudine + Lopinavir/Ritonavir )	CF1C
                            when concepts = '628 ## 795 ## 797 ## 9026' then 'CF1C'

							#AZT + 3TC + ATV/r (Zidovudine + Lamivudine + Atazanavir/Ritonavir )	CF1D
                            when concepts = '628 ## 795 ## 797 ## 6159' then 'CF1D'

							#ABC + 3TC + NVP (Abacavir + Lamivudine + Nevirapine)	CF2A
                            when concepts = '628 ## 631 ## 814' then 'CF2A'

							#ABC + 3TC + EFV (Abacavir + Lamivudine + Efavirenz)	CF2B
                            when concepts = '628 ## 633 ## 814' then 'CF2B'

							#ABC + 3TC + LPV/r (Abacavir + Lamivudine + Lopinavir/Ritonavir )	CF2D
                            when concepts = '628 ## 795 ## 814 ## 9026' then 'CF2D'

							#ABC + 3TC + ATV/r (Abacavir + Lamivudine + Atazanavir/Ritonavir )	CF2E
                            when concepts = '628 ## 795 ## 814 ## 6159' then 'CF2E'

							#d4T + 3TC + NVP for children weighing >= 25kg (Stavudine + Lamivudine + Nevirapine)	CF3A
                            when concepts = '625 ## 628 ## 631' then 'CF3A'

							#d4T + 3TC + EFV for children weighing >= 25kg (Stavudine + Lamivudine + Efavirenz)	CF3B
                            when concepts = '625 ## 628 ## 633' then 'CF3B'

							#TDF + 3TC + NVP (Tenofovir + Lamivudine + Nevirapine)	CF4A
                            when concepts = '628 ## 631 ## 802' then 'CF4A'

							#TDF + 3TC + EFV (Tenofovir + Lamivudine + Efavirenz)	CF4B
                            when concepts = '628 ## 633 ## 802' then 'CF4B'

							#TDF + 3TC + LPV/r (Tenofovir + Lamivudine + Lopinavir/Ritonavir)	CF4C
                            when concepts = '628 ## 795 ## 802 ## 9026' then 'CF4C'

							#TDF + 3TC + ATV/r (Tenofovir + Lamivudine + Atazanavir/Ritonavir)	CF4D
                            when concepts = '628 ## 795 ## 802 ## 6159' then 'CF4D'

							#AZT + 3TC + LPV/r (Zidovudine + Lamivudine + Lopinavir/ Ritonavir )	CS1A
							when concepts = '628 ## 795 ## 814 ## 9026' then 'CS1A'
                            
							#AZT + 3TC +ATV/r (Zidovudine + Lamivudine + Atazanavir/Ritonavir )	CS1B
							when concepts = '628 ## 795 ## 797 ## 6159' then 'CS1B'
							
                            #ABC + 3TC + LPV/r (Abacavir + Lamivudine + Lopinavir/Ritonavir)	CS2A
							when concepts = '628 ## 795 ## 814 ## 9026' then 'CS2A'
							
                            #ABC + 3TC + ATV/r (Abacavir + Lamivudine + Atazanavir/Ritonavir)	CS2C
							when concepts = '628 ## 795 ## 814 ## 6159' then 'CS2C'


							#PEDS 3RD LINE REGIMENS
                            
                            #RAL + 3TC + DRV + RTV (Raltegravir + Lamivudine + Darunavir + Ritonavir)	CT1A
                            when concepts = '628 ## 795 ## 6156 ## 6157' then 'CT1A'
                            
							#RAL + 3TC + DRV + RTV + AZT (Raltegravir + Lamivudine + Darunavir + Ritonavir + Zidovudine)	CT1B
							when concepts = '628 ## 795 ## 797 ## 6156 ## 6157' then 'CT1B'
                            
                            #RAL + 3TC + DRV + RTV + ABC (Raltegravir + Lamivudine + Darunavir + Ritonavir + Abacavir)	CT1C
							when concepts = '628 ## 795 ## 814 ## 6156 ## 6157' then 'CT1C'
                            
                            #ETV + 3TC + DRV + RTV (Etravirine + Lamivudine + Darunavir + Ritonavir)	CT2A
							when concepts = '628 ## 795 ## 6157 ## 6158' then 'CT2A'
                            
							else "peds unknown"
                        end
				
				end                                
			into e;
            return e;
            
            
        END$$
DELIMITER ;
