

create table parquet_tab1(key string, value string) stored as PARQUET;
create table parquet_tab2(key string, value string) stored as PARQUET;
insert overwrite table parquet_tab1 select * from src;
insert overwrite table parquet_tab2 select * from src;

select * from parquet_tab1 limit 10;
select * from parquet_tab2 limit 10;

explain
select parquet_tab1.key, parquet_tab2.value
FROM parquet_tab1 JOIN parquet_tab2 ON parquet_tab1.key = parquet_tab2.key;

select parquet_tab1.key, parquet_tab2.value
FROM parquet_tab1 JOIN parquet_tab2 ON parquet_tab1.key = parquet_tab2.key;



