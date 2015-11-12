drop table if exists testhbaseb;
CREATE TABLE testhbaseb (key int, val binary)
STORED BY 'org.apache.hadoop.hive.hbase.HBaseStorageHandler'
WITH SERDEPROPERTIES (
"hbase.columns.mapping" = ":key,cf:val#b"
);
insert into table testhbaseb select 1, 'hello' from src where key = 100;
insert into table testhbaseb select 2, 'hi' from src where key = 100;
select * from testhbaseb;
drop table testhbaseb;


