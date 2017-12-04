SET hive.exec.schema.evolution=true;

-- Currently, double to smallint conversion is not supported because it isn't in the lossless
-- TypeIntoUtils.implicitConvertible conversions.
create table src_orc (key double, val string) stored as orc;
set metaconf:hive.metastore.disallow.incompatible.col.type.changes=true;
alter table src_orc change key key smallint;
