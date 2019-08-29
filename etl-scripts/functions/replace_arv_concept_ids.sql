DELIMITER $$
CREATE  FUNCTION `replaceArvConceptIds`(obs varchar(10000)) RETURNS varchar(100) CHARSET latin1
    DETERMINISTIC
BEGIN
	DECLARE e varchar(100);	
    DECLARE i int;
	DECLARE cur_arv varchar(10);
    
	drop temporary table  if exists foo;
	create temporary table foo (concept_id int);

    set i = 1;
    set cur_arv = SPLIT_STR(obs, " ## ", i);    

    while (cur_arv != "" and i < 5) do

        case
			#ZIDOVUDINE AND LAMIVUDINE (630) #azt=797 3tc=628
			when cur_arv="630" then insert into foo values (797), (628);
            
            #Triomune-40 (STAVUDINE LAMIVUDINE AND NEVIRAPINE) (792) #d4t=625 3tc=628 nvp=631
            when cur_arv= "792" then insert into foo values (625), (628), (631);

			#LOPINAVIR AND RITONAVIR (794) #lop=9026 rit=795
            when cur_arv="794" then insert into foo values (9026), (795);
	
			#LAMIVUDINE AND TENOFOVIR (1400) #3tc=628 tdf=802
            when cur_arv="1400" then insert into foo values (628),(802);

			#ATAZANAVIR AND RITONAVIR (6160) #atz = 6159 rit=795
            when cur_arv="6160" then insert into foo values (6159),(795) ;

            			#EMTRICITABINE AND TENOFOVIR (6180) #emt=791 tdf=802
            when cur_arv="6180" then insert into foo values (791),(802);

			#NEVIRAPINE LAMIVUDINE AND ZIDOVUDINE (6467) #nvp=631 3tc=628 azt=797
            when cur_arv="6467" then insert into foo values (631),(628),(797);

			#TENOFOVIR AND LAMIVUDINE AND EFAVIRENZ (6964) # tdf= 802 3tc=628 efv = 633
            when cur_arv='6964' then insert into foo values (802),(628),(633);

			#LAMIVUDINE AND STAVUDINE (6965) #3tc=628 d4t=625
			when cur_arv='6965' then insert into foo values (628),(625);

			#ABACAVIR AND LAMIVUDINE (6679) #abc=814 3tc=628
            when cur_arv='6679' then insert into foo values (814),(628);

			#EMTRICITABINE AND RIPLIVIRINE AND TENOFOVIR (EVIPLERA) (9435) # emt=791 rip=?? tdf=802
            when cur_arv='9435' then insert into foo values (791),(802);

            else insert into foo values (cur_arv);
		end case;        
#		insert into foo values (1,cur_arv);
		set i = i+1;
		set cur_arv = SPLIT_STR(obs, " ## ", i);
    end while;

	select group_concat(distinct concept_id order by concept_id separator ' ## ') into e from foo;
    
    #select count(*) into i from foo group by id;
  RETURN e;

END$$
DELIMITER ;
