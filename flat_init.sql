#drop table if exists flat_log;
create table if not exists flat_log (date_updated datetime, table_name varchar(100), index date_updated (date_updated));

#This will be used by derived tables to rebuild derived data for patients with new/changed data
#It will be populated by each of the flat_table scripts. 
drop table if exists flat_new_person_data;
create table flat_new_person_data(person_id int, primary key person_id (person_id));