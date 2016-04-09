#This table will provide an easy way to filter cohort end dates for clinical summaru visualization
drop table if exists etl.dates;
create table etl.dates (endDate datetime);
insert into dates (endDate)
SELECT LAST_DAY(e.encounter_datetime) as endDate
from etl.flat_hiv_summary e
group by year(e.encounter_datetime), month(e.encounter_datetime)
order by year(e.encounter_datetime), month(e.encounter_datetime);
