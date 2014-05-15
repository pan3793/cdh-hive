drop table staging;
drop table parquet_ctas;
drop table parquet_ctas_advanced;
drop table parquet_ctas_alias;
drop table parquet_ctas_mixed;

create table staging (key int, value string) stored as textfile;
insert into table staging select * from src order by key limit 10;

create table parquet_ctas
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
STORED AS
 INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
 OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
as select * from staging
;

describe parquet_ctas;
select * from parquet_ctas;

create table parquet_ctas_advanced
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
 STORED AS
 INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
 OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
as select key+1,concat(value,"value") from staging;

describe parquet_ctas_advanced;
select * from parquet_ctas_advanced;

create table parquet_ctas_alias
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
 STORED AS
 INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
 OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
as select key+1 as mykey,concat(value,"value") as myvalue from staging;

describe parquet_ctas_alias;
select * from parquet_ctas_alias;

create table parquet_ctas_mixed
ROW FORMAT SERDE 'parquet.hive.serde.ParquetHiveSerDe'
 STORED AS
 INPUTFORMAT 'parquet.hive.DeprecatedParquetInputFormat'
 OUTPUTFORMAT 'parquet.hive.DeprecatedParquetOutputFormat'
as select key,key+1,concat(value,"value") as myvalue from staging;

describe parquet_ctas_mixed;
select * from parquet_ctas_mixed;
