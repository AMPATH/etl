#drop table if exists flat_log;
create table if not exists flat_log (date_updated datetime, table_name varchar(100), index date_updated (date_updated));
