DELIMITER $$
CREATE  PROCEDURE `generate_location_facilities`()
BEGIN
drop table if exists `hie`.facility_locations;
create table if not exists `hie`.facility_locations(
   date_created TIMESTAMP DEFAULT CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP,
   location_id int not null,
   location_name varchar(100) not null,
   facility_code varchar(20) null,
   fr_code varchar(20) null,
   location_uuid varchar(100) not null,
   regulator varchar(100) null,
   facility_level varchar(100) null,
   facility_category varchar(100) null,
   facility_owner varchar(100) null,
   facility_type varchar(100) null,
   county varchar(100) null,
   sub_county varchar(100) null,
   ward varchar(100) null,
   found boolean null,
   approved boolean null,
   operational_status varchar(30) null,
   current_license_expiry_date date null,
   index facility_code(facility_code),
   index location (location_uuid)
);

replace into `hie`.facility_locations (SELECT 
    NULL AS date_created,
    l.location_id,
    l.name AS 'location_name',
    la.value_reference AS 'facility_code',
    fr.value_reference AS 'fr_code',
    l.uuid AS 'location_uuid',
    f.regulator AS 'regulator',
    f.facility_level AS 'facility_level',
    f.facility_category AS 'facility_category',
    f.facility_owner AS 'facility_owner',
    f.facility_type AS 'facility_type',
    f.county AS 'county',
    f.sub_county AS 'sub_county',
    f.ward AS 'ward',
    NULL AS 'found',
    f.approved AS 'approved',
    f.operational_status AS 'operational_status',
    f.current_license_expiry_date AS 'current_license_expiry_date'
FROM
    amrs.location l
        LEFT JOIN
    amrs.location_attribute `la` ON (la.location_id = l.location_id
        AND la.attribute_type_id = 2)
	 LEFT JOIN
    amrs.location_attribute `fr` ON (fr.location_id = l.location_id
        AND fr.attribute_type_id = 8)
        LEFT JOIN
    hie.facilities f ON (f.facility_code = la.value_reference)
WHERE
    l.retired = 0 AND la.voided = 0
GROUP BY l.location_id);
END$$
DELIMITER ;
