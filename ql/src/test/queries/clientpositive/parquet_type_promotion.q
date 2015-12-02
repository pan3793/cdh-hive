DROP TABLE parquet_type_promotion_staging;
DROP TABLE parquet_type_promotion;

SET hive.metastore.disallow.incompatible.col.type.changes=false;

CREATE TABLE parquet_type_promotion_staging (
  cint int,
  cint2 int,
  cint3 int,
  clong bigint,
  clong2 bigint,
  cfloat float,
  cdouble double,
  m1 map<string, int>,
  m2 map<string, bigint>,
  l1 array<int>,
  l2 array<bigint>,
  st1 struct<c1:int, c2:int>,
  fm1 map<string, float>,
  fl1 array<float>,
  fst1 struct<c1:float, c2:float>
) ROW FORMAT DELIMITED
FIELDS TERMINATED BY '|'
COLLECTION ITEMS TERMINATED BY ','
MAP KEYS TERMINATED BY ':';

LOAD DATA LOCAL INPATH '../../data/files/parquet_type_promotion.txt' OVERWRITE INTO TABLE parquet_type_promotion_staging;

SELECT * FROM parquet_type_promotion_staging;

CREATE TABLE parquet_type_promotion (
  cint int,
  cint2 int,
  cint3 int,
  clong bigint,
  clong2 bigint,
  cfloat float,
  cdouble double,
  m1 map<string, int>,
  m2 map<string, bigint>,
  l1 array<int>,
  l2 array<bigint>,
  st1 struct<c1:int, c2:int>,
  fm1 map<string, float>,
  fl1 array<float>,
  fst1 struct<c1:float, c2:float>
) STORED AS PARQUET;

INSERT OVERWRITE TABLE parquet_type_promotion
   SELECT * FROM parquet_type_promotion_staging;


-- This test covers HIVE-12475

DROP TABLE arrays_of_struct_to_map;
CREATE TABLE arrays_of_struct_to_map (locations1 array<struct<c1:int,c2:int>>, locations2 array<struct<f1:int,
f2:int,f3:int>>) STORED AS PARQUET;
INSERT INTO TABLE arrays_of_struct_to_map select array(named_struct("c1",1,"c2",2)), array(named_struct("f1",
77,"f2",88,"f3",99)) FROM parquet_type_promotion LIMIT 1;
SELECT * FROM arrays_of_struct_to_map;
ALTER TABLE arrays_of_struct_to_map REPLACE COLUMNS (locations1 array<struct<c1:int,c2:int,c3:int>>, locations2
array<struct<f1:int,f2:int,f3:int>>);
SELECT * FROM arrays_of_struct_to_map;
