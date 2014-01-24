set hive.map.aggr=true;
set hive.groupby.skewindata=true;
set mapred.reduce.tasks=31;

CREATE TABLE dest1(c1 DOUBLE, c2 DOUBLE, c3 DOUBLE, c4 DOUBLE, c5 DOUBLE, c6 DOUBLE, c7 DOUBLE, c8 DOUBLE, c9 DOUBLE) STORED AS TEXTFILE;

EXPLAIN
FROM src
INSERT OVERWRITE TABLE dest1 SELECT
  sum(substr(src.value,5)),
  cast(avg(substr(src.value,5)) as decimal(10,5)),
  cast(avg(DISTINCT substr(src.value,5)) as decimal(10,5)),
  cast(max(substr(src.value,5)) as decimal(10,5)),
  cast(min(substr(src.value,5)) as decimal(10,5)),
  cast(std(substr(src.value,5)) as decimal(10,5)),
  cast(stddev_samp(substr(src.value,5)) as decimal(10,5)),
  cast(variance(substr(src.value,5)) as decimal(10,5)),
  cast(var_samp(substr(src.value,5)) as decimal(10,5));

FROM src
INSERT OVERWRITE TABLE dest1 SELECT
  sum(substr(src.value,5)),
  cast(avg(substr(src.value,5)) as decimal(10,5)),
  cast(avg(DISTINCT substr(src.value,5)) as decimal(10,5)),
  cast(max(substr(src.value,5)) as decimal(10,5)),
  cast(min(substr(src.value,5)) as decimal(10,5)),
  cast(std(substr(src.value,5)) as decimal(10,5)),
  cast(stddev_samp(substr(src.value,5)) as decimal(10,5)),
  cast(variance(substr(src.value,5)) as decimal(10,5)),
  cast(var_samp(substr(src.value,5)) as decimal(10,5));

SELECT dest1.* FROM dest1;


