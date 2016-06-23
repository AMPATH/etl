#drop table if exists flat_log;
create table if not exists flat_log (date_created timestamp, date_updated datetime, table_name varchar(100), seconds_to_complete smallint, index date_updated (date_updated));

select @now := now();
