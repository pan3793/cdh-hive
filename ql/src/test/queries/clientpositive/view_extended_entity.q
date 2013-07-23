DROP DATABASE IF EXISTS db1;
CREATE DATABASE db1;
USE db1;

set hive.entity.capture.input.URI=true;

CREATE TABLE table1 (key STRING, value STRING)
STORED AS TEXTFILE;

CREATE TABLE table2 (key STRING, value STRING)
STORED AS TEXTFILE;

-- relative reference, no alias
CREATE VIEW v1 AS SELECT * FROM table1;

-- relative reference, aliased
CREATE VIEW v2 AS SELECT t1.* FROM table1 t1;

-- relative reference, multiple tables
CREATE VIEW v3 AS SELECT t1.*, t2.key k FROM table1 t1 JOIN table2 t2 ON t1.key = t2.key;

-- absolute reference, no alias
CREATE VIEW v4 AS SELECT * FROM db1.table1;

-- absolute reference, aliased
CREATE VIEW v5 AS SELECT t1.* FROM db1.table1 t1;

-- absolute reference, multiple tables
CREATE VIEW v6 AS SELECT t1.*, t2.key k FROM db1.table1 t1 JOIN db1.table2 t2 ON t1.key = t2.key;

-- relative reference, explicit column
CREATE VIEW v7 AS SELECT key from table1;

-- absolute reference, explicit column
CREATE VIEW v8 AS SELECT key from db1.table1;

