drop table if exists t1;
drop table if exists t2_avro;
create table t1 (c1 bigint, c2 string);

CREATE TABLE t2_avro
PARTITIONED BY (p1 string)
ROW FORMAT SERDE 
  'org.apache.hadoop.hive.serde2.avro.AvroSerDe' 
STORED AS INPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerInputFormat' 
OUTPUTFORMAT 
  'org.apache.hadoop.hive.ql.io.avro.AvroContainerOutputFormat'
TBLPROPERTIES(
'avro.schema.literal'='{
"namespace": "testing.hive.avro.serde",
"name": "test",
"type": "record",
"fields": [
{ "name":"c1", "type":"int" },
{ "name":"c2", "type":"string" }
]}');

load data local inpath '../../data/files/avrotest.dat' into table t1;
load data local inpath '../../data/files/avrotest.dat' into table t1;
load data local inpath '../../data/files/avrotest.dat' into table t1;
load data local inpath '../../data/files/avrotest.dat' into table t1;
load data local inpath '../../data/files/avrotest.dat' into table t1;

SET hive.optimize.sort.dynamic.partition=true;
SET hive.exec.dynamic.partition.mode=nonstrict;

insert overwrite table t2_avro partition(p1) select *,c1 as p1 from t1 distribute by p1;
select count (*) from t2_avro;

