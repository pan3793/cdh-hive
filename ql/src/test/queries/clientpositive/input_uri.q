SET  hive.test.mode=true;
SET hive.test.mode.prefix=;
SET hive.entity.capture.input.URI=false;

DROP DATABASE IF EXISTS db1 CASCADE;
CREATE DATABASE db1;

USE db1;
CREATE TABLE tab1( dep_id INT);

LOAD DATA LOCAL INPATH '../data/files/test.dat' INTO TABLE tab1;

dfs ${system:test.dfs.mkdir} ../build/ql/test/data/exports/uri1/temp;
dfs -rmr ../build/ql/test/data/exports/uri1;
EXPORT TABLE tab1 TO 'ql/test/data/exports/uri1';

DROP TABLE tab1;
IMPORT TABLE tab2 FROM 'ql/test/data/exports/uri1';

CREATE TABLE tab3 (key INT, value STRING);
ALTER TABLE tab3 SET LOCATION "file:/test/test/";

CREATE TABLE ptab (key INT, value STRING) PARTITIONED BY (ds STRING);
ALTER TABLE ptab ADD PARTITION (ds='2010');
ALTER TABLE ptab PARTITION(ds='2010') SET LOCATION "file:/test/test/ds=2010";
