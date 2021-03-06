SET hive.input.format=org.apache.hadoop.hive.ql.io.HiveInputFormat;
SET hive.optimize.ppd=true;
SET mapred.min.split.size=1000;
SET mapred.max.split.size=5000;

create table newtypestbl(c char(10), v varchar(10), d decimal(5,3)) stored as parquet;

insert overwrite table newtypestbl select * from (select cast("apple" as char(10)), cast("bee" as varchar(10)), 0.22 from src src1 union all select cast("hello" as char(10)), cast("world" as varchar(10)), 11.22 from src src2 limit 10) uniontbl;

set hive.optimize.index.filter=false;

-- varchar data types (EQUAL, NOT_EQUAL, LESS_THAN, LESS_THAN_EQUALS, IN, BETWEEN tests)
select * from newtypestbl where v="bee";

set hive.optimize.index.filter=true;
select * from newtypestbl where v="bee";

set hive.optimize.index.filter=false;
select * from newtypestbl where v!="bee";

set hive.optimize.index.filter=true;
select * from newtypestbl where v!="bee";

set hive.optimize.index.filter=false;
select * from newtypestbl where v<"world";

set hive.optimize.index.filter=true;
select * from newtypestbl where v<"world";

set hive.optimize.index.filter=false;
select * from newtypestbl where v<="world" sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl where v<="world" sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl where v="bee   ";

set hive.optimize.index.filter=true;
select * from newtypestbl where v="bee   ";

set hive.optimize.index.filter=false;
select * from newtypestbl where v in ("bee", "orange");

set hive.optimize.index.filter=true;
select * from newtypestbl where v in ("bee", "orange");

set hive.optimize.index.filter=false;
select * from newtypestbl where v in ("bee", "world") sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl where v in ("bee", "world") sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl where v in ("orange");

set hive.optimize.index.filter=true;
select * from newtypestbl where v in ("orange");

set hive.optimize.index.filter=false;
select * from newtypestbl where v between "bee" and "orange";

set hive.optimize.index.filter=true;
select * from newtypestbl where v between "bee" and "orange";

set hive.optimize.index.filter=false;
select * from newtypestbl where v between "bee" and "zombie" sort by c;

set hive.optimize.index.filter=true;
select * from newtypestbl where v between "bee" and "zombie" sort by c;

set hive.optimize.index.filter=false;
select * from newtypestbl where v between "orange" and "pine";

set hive.optimize.index.filter=true;
select * from newtypestbl where v between "orange" and "pine";
