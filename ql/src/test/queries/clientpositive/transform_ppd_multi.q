set hive.optimize.ppd=true;

CREATE TABLE multi1(id int, name string);
CREATE TABLE multi2(id int, name string);
--multi-insert case: transform has two filter children (HIVE-6395)
EXPLAIN EXTENDED
FROM (
  FROM srcpart src
  SELECT TRANSFORM(src.ds, src.key, src.value)
         USING 'cat' AS (ds, tkey, tvalue)
) tmap
INSERT OVERWRITE TABLE multi1 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100
INSERT OVERWRITE TABLE multi2 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey > 100;

FROM (
  FROM srcpart src
  SELECT TRANSFORM(src.ds, src.key, src.value)
         USING 'cat' AS (ds, tkey, tvalue)
) tmap
INSERT OVERWRITE TABLE multi1 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey < 100
INSERT OVERWRITE TABLE multi2 SELECT tmap.tkey, tmap.tvalue WHERE tmap.tkey > 100;

SELECT * FROM multi1;
SELECT * FROM multi2;

DROP TABLE multi1;
DROP TABLE multi2;
